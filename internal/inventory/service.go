// Package inventory es el Inventory Service. Reserva stock cuando se crea un
// pedido (T2) y lo LIBERA (compensación) si el pago falla. Cada cambio de
// stock queda registrado en la tabla inventory_movements (kardex).
package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/store"
)

// Schema de la Inventory DB (vive en su propio contenedor y volumen).
const Schema = `
CREATE TABLE IF NOT EXISTS products (
	product_id TEXT PRIMARY KEY,
	stock      INT NOT NULL CHECK (stock >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
	order_id   TEXT PRIMARY KEY,
	product_id TEXT NOT NULL,
	quantity   INT  NOT NULL,
	status     TEXT NOT NULL, -- RESERVED | RELEASED
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Kardex: una fila por cada transacción que toca el inventario.
CREATE TABLE IF NOT EXISTS inventory_movements (
	id            BIGSERIAL PRIMARY KEY,
	movement_type TEXT NOT NULL,   -- CARGA_INICIAL | AJUSTE | RESERVA | LIBERACION | RECHAZO
	product_id    TEXT NOT NULL,
	order_id      TEXT,            -- pedido (saga) que lo provocó, si aplica
	customer_id   TEXT,
	quantity      INT  NOT NULL,   -- cambio en el stock: negativo sale, positivo entra, 0 sin cambio
	stock_before  INT  NOT NULL,
	stock_after   INT  NOT NULL,
	event_id      TEXT,            -- evento de NATS que lo provocó
	event_type    TEXT,
	detail        TEXT NOT NULL DEFAULT '',
	created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
	CHECK (stock_after = stock_before + quantity)
);
CREATE INDEX IF NOT EXISTS inventory_movements_product ON inventory_movements (product_id, id);
CREATE INDEX IF NOT EXISTS inventory_movements_order   ON inventory_movements (order_id);`

// Tipos de movimiento del kardex.
const (
	CargaInicial = "CARGA_INICIAL" // stock con el que arranca un producto
	Ajuste       = "AJUSTE"        // cambio manual por la API de administración
	Reserva      = "RESERVA"       // T2: sale stock para un pedido
	Liberacion   = "LIBERACION"    // C2: compensación, el stock vuelve
	Rechazo      = "RECHAZO"       // pedido sin stock suficiente (no cambia el stock)
)

var seedStock = map[string]int{"mystery-box": 100, "runner-pro": 50, "studio-buds": 3}

type Product struct {
	ProductID string `json:"product_id"`
	Stock     int    `json:"stock"`
}

// Movement es una fila del kardex.
type Movement struct {
	ID           int64     `json:"id"`
	MovementType string    `json:"movement_type"`
	ProductID    string    `json:"product_id"`
	OrderID      string    `json:"order_id,omitempty"`
	CustomerID   string    `json:"customer_id,omitempty"`
	Quantity     int       `json:"quantity"`
	StockBefore  int       `json:"stock_before"`
	StockAfter   int       `json:"stock_after"`
	EventID      string    `json:"event_id,omitempty"`
	EventType    string    `json:"event_type,omitempty"`
	Detail       string    `json:"detail"`
	CreatedAt    time.Time `json:"created_at"`
}

type Service struct {
	db  *pgxpool.Pool
	bus broker.Broker
}

func New(db *pgxpool.Pool, bus broker.Broker) *Service { return &Service{db: db, bus: bus} }

func (s *Service) Start(ctx context.Context) error {
	for p, n := range seedStock {
		if err := s.seed(ctx, p, n); err != nil {
			return err
		}
	}
	// Productos creados antes de existir el kardex: su stock actual pasa a ser
	// su saldo inicial, para que stock = suma de movimientos siempre cuadre.
	if _, err := s.db.Exec(ctx, `
		INSERT INTO inventory_movements (movement_type, product_id, quantity, stock_before, stock_after, detail)
		SELECT 'CARGA_INICIAL', p.product_id, p.stock, 0, p.stock, 'Saldo inicial al habilitar el kardex'
		FROM products p
		WHERE NOT EXISTS (SELECT 1 FROM inventory_movements m WHERE m.product_id = p.product_id)`); err != nil {
		return err
	}
	if err := s.bus.Subscribe(ctx, events.TopicOrder, "inventory-on-order", s.onOrderEvent); err != nil {
		return err
	}
	return s.bus.Subscribe(ctx, events.TopicPayment, "inventory-on-payment", s.onPaymentEvent)
}

// seed carga el stock inicial solo si el producto no existe (el volumen puede
// tener datos de antes) y lo registra en el kardex.
func (s *Service) seed(ctx context.Context, product string, stock int) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`INSERT INTO products (product_id, stock) VALUES ($1, $2) ON CONFLICT DO NOTHING`, product, stock)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if err := recordMovement(ctx, tx, Movement{
			MovementType: CargaInicial, ProductID: product, Quantity: stock,
			StockBefore: 0, StockAfter: stock, Detail: "Stock inicial del producto",
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// recordMovement escribe una fila en el kardex dentro de la transacción del
// cambio de stock: si el cambio se guarda, su movimiento también.
func recordMovement(ctx context.Context, tx pgx.Tx, m Movement) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO inventory_movements
			(movement_type, product_id, order_id, customer_id, quantity, stock_before, stock_after, event_id, event_type, detail)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), $10)`,
		m.MovementType, m.ProductID, m.OrderID, m.CustomerID, m.Quantity, m.StockBefore, m.StockAfter,
		m.EventID, m.EventType, m.Detail)
	return err
}

// onOrderEvent: pasos ② y ③. Transacción local T2 = reservar stock.
func (s *Service) onOrderEvent(ctx context.Context, e events.Event) error {
	if e.Type != events.OrderCreated {
		return nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	fresh, err := store.MarkProcessed(ctx, tx, e.ID)
	if err != nil {
		return err
	}
	if !fresh {
		logx.Log("inventory", logx.Gray, "evento %s repetido para %s: ignorado (idempotencia)", e.Type, e.OrderID)
		return tx.Commit(ctx)
	}

	// Descuento atómico: solo se aplica si alcanza el stock. Dos pedidos
	// simultáneos nunca pueden tomar la misma unidad.
	var left int
	err = tx.QueryRow(ctx,
		`UPDATE products SET stock = stock - $2 WHERE product_id = $1 AND stock >= $2 RETURNING stock`,
		e.ProductID, e.Quantity).Scan(&left)

	if errors.Is(err, pgx.ErrNoRows) {
		var available int
		_ = tx.QueryRow(ctx, `SELECT COALESCE((SELECT stock FROM products WHERE product_id = $1), 0)`, e.ProductID).Scan(&available)
		out := events.New(events.InventoryRejected, e)
		out.Reason = fmt.Sprintf("sin stock de %s (hay %d, se piden %d)", e.ProductID, available, e.Quantity)
		if err := recordMovement(ctx, tx, Movement{
			MovementType: Rechazo, ProductID: e.ProductID, OrderID: e.OrderID, CustomerID: e.CustomerID,
			Quantity: 0, StockBefore: available, StockAfter: available,
			EventID: e.ID, EventType: string(e.Type),
			Detail: fmt.Sprintf("Pedido rechazado: se pidieron %d u. y había %d", e.Quantity, available),
		}); err != nil {
			return err
		}
		if err := store.AddToOutbox(ctx, tx, events.TopicInventory, out); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		logx.Log("inventory", logx.Red, "② %s ✗ %s", e.OrderID, out.Reason)
		return nil
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO reservations (order_id, product_id, quantity, status) VALUES ($1, $2, $3, 'RESERVED')`,
		e.OrderID, e.ProductID, e.Quantity); err != nil {
		return err
	}
	if err := recordMovement(ctx, tx, Movement{
		MovementType: Reserva, ProductID: e.ProductID, OrderID: e.OrderID, CustomerID: e.CustomerID,
		Quantity: -e.Quantity, StockBefore: left + e.Quantity, StockAfter: left,
		EventID: e.ID, EventType: string(e.Type),
		Detail: fmt.Sprintf("Reserva de %d u. para el pedido %s", e.Quantity, e.OrderID),
	}); err != nil {
		return err
	}
	if err := store.AddToOutbox(ctx, tx, events.TopicInventory, events.New(events.InventoryReserved, e)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logx.Log("inventory", logx.Green, "②③ %s: reservadas %d u. de %s (quedan %d)", e.OrderID, e.Quantity, e.ProductID, left)
	return nil
}

// onPaymentEvent: si el pago falló ejecuta la TRANSACCIÓN COMPENSATORIA de T2
// (devolver el stock) y avisa con StockReleased.
func (s *Service) onPaymentEvent(ctx context.Context, e events.Event) error {
	if e.Type != events.PaymentFailed {
		return nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	fresh, err := store.MarkProcessed(ctx, tx, e.ID)
	if err != nil {
		return err
	}
	if !fresh {
		logx.Log("inventory", logx.Gray, "evento %s repetido para %s: ignorado (idempotencia)", e.Type, e.OrderID)
		return tx.Commit(ctx)
	}

	var product string
	var qty int
	err = tx.QueryRow(ctx, `
		UPDATE reservations SET status = 'RELEASED', updated_at = now()
		WHERE order_id = $1 AND status = 'RESERVED'
		RETURNING product_id, quantity`, e.OrderID).Scan(&product, &qty)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx) // nada que compensar
	}
	if err != nil {
		return err
	}

	var left int
	if err := tx.QueryRow(ctx,
		`UPDATE products SET stock = stock + $2 WHERE product_id = $1 RETURNING stock`, product, qty).Scan(&left); err != nil {
		return err
	}
	if err := recordMovement(ctx, tx, Movement{
		MovementType: Liberacion, ProductID: product, OrderID: e.OrderID, CustomerID: e.CustomerID,
		Quantity: qty, StockBefore: left - qty, StockAfter: left,
		EventID: e.ID, EventType: string(e.Type),
		Detail: fmt.Sprintf("Compensación: se devuelven %d u. porque falló el pago (%s)", qty, e.Reason),
	}); err != nil {
		return err
	}
	if err := store.AddToOutbox(ctx, tx, events.TopicInventory, events.New(events.StockReleased, e)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logx.Log("inventory", logx.Yellow, "↩ COMPENSANDO %s: liberadas %d u. de %s (quedan %d)", e.OrderID, qty, product, left)
	return nil
}

// List devuelve el stock de todos los productos.
func (s *Service) List(ctx context.Context) ([]Product, error) {
	rows, err := s.db.Query(ctx, `SELECT product_id, stock FROM products ORDER BY product_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Product])
}

// Movements devuelve el kardex, filtrado por producto y/o pedido (vacío = todos).
func (s *Service) Movements(ctx context.Context, productID, orderID string, limit int) ([]Movement, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, movement_type, product_id, COALESCE(order_id, ''), COALESCE(customer_id, ''),
		       quantity, stock_before, stock_after, COALESCE(event_id, ''), COALESCE(event_type, ''),
		       detail, created_at
		FROM inventory_movements
		WHERE ($1 = '' OR product_id = $1) AND ($2 = '' OR order_id = $2)
		ORDER BY id DESC
		LIMIT $3`, productID, orderID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Movement])
}

// SetStock crea o ajusta el stock de un producto (API de administración) y
// registra el ajuste en el kardex.
func (s *Service) SetStock(ctx context.Context, productID string, stock int) error {
	if productID == "" || stock < 0 {
		return fmt.Errorf("producto o stock inválido")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	before := 0
	err = tx.QueryRow(ctx, `SELECT stock FROM products WHERE product_id = $1 FOR UPDATE`, productID).Scan(&before)
	isNew := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !isNew {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO products (product_id, stock) VALUES ($1, $2)
		ON CONFLICT (product_id) DO UPDATE SET stock = EXCLUDED.stock`, productID, stock); err != nil {
		return err
	}
	if !isNew && before == stock {
		return tx.Commit(ctx) // sin cambio, no hay movimiento
	}
	m := Movement{MovementType: Ajuste, ProductID: productID, Quantity: stock - before,
		StockBefore: before, StockAfter: stock, Detail: "Ajuste administrativo de stock"}
	if isNew {
		m.MovementType, m.Detail = CargaInicial, "Alta del producto"
	}
	if err := recordMovement(ctx, tx, m); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
