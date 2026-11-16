// Package inventory es el Inventory Service. Reserva stock cuando se crea un
// pedido (T2) y lo LIBERA (compensación) si el pago falla.
package inventory

import (
	"context"
	"errors"
	"fmt"

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
);`

var seedStock = map[string]int{"mystery-box": 100, "runner-pro": 50, "studio-buds": 3}

type Product struct {
	ProductID string `json:"product_id"`
	Stock     int    `json:"stock"`
}

type Service struct {
	db  *pgxpool.Pool
	bus broker.Broker
}

func New(db *pgxpool.Pool, bus broker.Broker) *Service { return &Service{db: db, bus: bus} }

func (s *Service) Start(ctx context.Context) error {
	for p, n := range seedStock {
		if _, err := s.db.Exec(ctx,
			`INSERT INTO products (product_id, stock) VALUES ($1, $2) ON CONFLICT DO NOTHING`, p, n); err != nil {
			return err
		}
	}
	if err := s.bus.Subscribe(ctx, events.TopicOrder, "inventory-on-order", s.onOrderEvent); err != nil {
		return err
	}
	return s.bus.Subscribe(ctx, events.TopicPayment, "inventory-on-payment", s.onPaymentEvent)
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

// SetStock crea o ajusta el stock de un producto (API de administración).
func (s *Service) SetStock(ctx context.Context, productID string, stock int) error {
	if productID == "" || stock < 0 {
		return fmt.Errorf("producto o stock inválido")
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO products (product_id, stock) VALUES ($1, $2)
		ON CONFLICT (product_id) DO UPDATE SET stock = EXCLUDED.stock`, productID, stock)
	return err
}
