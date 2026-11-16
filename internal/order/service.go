// Package order es el Order Service. Inicia la saga (T1) y decide el estado
// final del pedido (CONFIRMED o CANCELLED) según los eventos que escucha.
package order

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

type Status string

const (
	Pending   Status = "PENDING"   // saga en curso
	Confirmed Status = "CONFIRMED" // todos los pasos OK
	Cancelled Status = "CANCELLED" // un paso falló y se compensó
)

// Schema de la Order DB (vive en su propio contenedor y volumen).
const Schema = `
CREATE SEQUENCE IF NOT EXISTS order_seq;
CREATE TABLE IF NOT EXISTS catalog (
	product_id TEXT PRIMARY KEY,
	price      NUMERIC(12,2) NOT NULL CHECK (price >= 0)
);
CREATE TABLE IF NOT EXISTS orders (
	id          TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL,
	product_id  TEXT NOT NULL,
	quantity    INT  NOT NULL CHECK (quantity > 0),
	amount      NUMERIC(12,2) NOT NULL,
	status      TEXT NOT NULL,
	reason      TEXT NOT NULL DEFAULT '',
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);`

// Catálogo inicial (productos del video). ON CONFLICT DO NOTHING: si el
// volumen ya tiene datos, se respetan.
var seedCatalog = map[string]float64{"mystery-box": 29, "runner-pro": 59, "studio-buds": 99}

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	Amount     float64   `json:"amount"`
	Status     Status    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

var (
	ErrUnknownProduct = errors.New("producto no existe en el catálogo")
	ErrNotFound       = errors.New("pedido no encontrado")
)

type Service struct {
	db  *pgxpool.Pool
	bus broker.Broker
}

func New(db *pgxpool.Pool, bus broker.Broker) *Service { return &Service{db: db, bus: bus} }

// Start carga el catálogo y se suscribe a los eventos que le interesan.
func (s *Service) Start(ctx context.Context) error {
	for p, price := range seedCatalog {
		if _, err := s.db.Exec(ctx,
			`INSERT INTO catalog (product_id, price) VALUES ($1, $2) ON CONFLICT DO NOTHING`, p, price); err != nil {
			return err
		}
	}
	// Coreografía: Order reacciona a lo que publican Payment e Inventory.
	if err := s.bus.Subscribe(ctx, events.TopicPayment, "order-on-payment", s.onPaymentEvent); err != nil {
		return err
	}
	return s.bus.Subscribe(ctx, events.TopicInventory, "order-on-inventory", s.onInventoryEvent)
}

// PlaceOrder es el PASO ① (T1): en UNA transacción local guarda el pedido en
// PENDING y deja OrderCreated en la outbox. Responde sin esperar a nadie.
func (s *Service) PlaceOrder(ctx context.Context, customerID, productID string, qty int) (Order, error) {
	if qty <= 0 {
		return Order{}, errors.New("la cantidad debe ser mayor a 0")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback(ctx)

	var price float64
	err = tx.QueryRow(ctx, `SELECT price::float8 FROM catalog WHERE product_id = $1`, productID).Scan(&price)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrUnknownProduct
	}
	if err != nil {
		return Order{}, err
	}

	o := Order{CustomerID: customerID, ProductID: productID, Quantity: qty, Amount: price * float64(qty), Status: Pending}
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (id, customer_id, product_id, quantity, amount, status)
		VALUES ('ORD-' || lpad(nextval('order_seq')::text, 3, '0'), $1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at`,
		customerID, productID, qty, o.Amount, o.Status).Scan(&o.ID, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return Order{}, err
	}

	err = store.AddToOutbox(ctx, tx, events.TopicOrder, events.New(events.OrderCreated, events.Event{
		OrderID: o.ID, CustomerID: customerID, ProductID: productID, Quantity: qty, Amount: o.Amount,
	}))
	if err != nil {
		return Order{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, err
	}
	logx.Log("order", logx.Blue, "① Pedido %s creado (%s, %s x%d, $%.2f) → PENDING", o.ID, customerID, productID, qty, o.Amount)
	return o, nil
}

// onPaymentEvent: paso ⑥ del camino feliz.
func (s *Service) onPaymentEvent(ctx context.Context, e events.Event) error {
	if e.Type != events.PaymentProcessed {
		// PaymentFailed se ignora aquí: el pedido se cancela cuando Inventory
		// confirme que ya liberó el stock (StockReleased).
		return nil
	}
	return s.finish(ctx, e, Confirmed, "")
}

// onInventoryEvent: aquí terminan los caminos de fallo.
func (s *Service) onInventoryEvent(ctx context.Context, e events.Event) error {
	switch e.Type {
	case events.StockReleased, events.InventoryRejected:
		// Transacción COMPENSATORIA de T1: no se borra el pedido, se cancela.
		return s.finish(ctx, e, Cancelled, e.Reason)
	}
	return nil
}

func (s *Service) finish(ctx context.Context, e events.Event, st Status, reason string) error {
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
		logx.Log("order", logx.Gray, "evento %s repetido para %s: ignorado (idempotencia)", e.Type, e.OrderID)
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE orders SET status = $2, reason = $3, updated_at = now() WHERE id = $1 AND status = 'PENDING'`,
		e.OrderID, st, reason); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if st == Confirmed {
		logx.Log("order", logx.Blue, "⑥ Pago confirmado → pedido %s CONFIRMED", e.OrderID)
	} else {
		logx.Log("order", logx.Red, "↩ CancelOrder: pedido %s CANCELLED (%s)", e.OrderID, reason)
	}
	return nil
}

// Get devuelve un pedido.
func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	var o Order
	err := s.db.QueryRow(ctx, `
		SELECT id, customer_id, product_id, quantity, amount::float8, status, reason, created_at, updated_at
		FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.CustomerID, &o.ProductID, &o.Quantity, &o.Amount, &o.Status, &o.Reason, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

// List devuelve los últimos pedidos.
func (s *Service) List(ctx context.Context) ([]Order, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, customer_id, product_id, quantity, amount::float8, status, reason, created_at, updated_at
		FROM orders ORDER BY created_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Order, error) {
		var o Order
		err := r.Scan(&o.ID, &o.CustomerID, &o.ProductID, &o.Quantity, &o.Amount, &o.Status, &o.Reason, &o.CreatedAt, &o.UpdatedAt)
		return o, err
	})
}

// SetPrice crea o actualiza un producto del catálogo (API de administración).
func (s *Service) SetPrice(ctx context.Context, productID string, price float64) error {
	if productID == "" || price < 0 {
		return fmt.Errorf("producto o precio inválido")
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO catalog (product_id, price) VALUES ($1, $2)
		ON CONFLICT (product_id) DO UPDATE SET price = EXCLUDED.price`, productID, price)
	return err
}
