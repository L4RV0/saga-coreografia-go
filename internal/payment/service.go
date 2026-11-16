// Package payment es el Payment Service. Cobra (T3) una vez reservado el
// stock. Es el último paso: si falla, publica PaymentFailed y eso dispara las
// compensaciones de los demás.
package payment

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

// Schema de la Payment DB (vive en su propio contenedor y volumen).
const Schema = `
CREATE TABLE IF NOT EXISTS accounts (
	customer_id TEXT PRIMARY KEY,
	balance     NUMERIC(12,2) NOT NULL CHECK (balance >= 0)
);
CREATE TABLE IF NOT EXISTS payments (
	order_id    TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL,
	amount      NUMERIC(12,2) NOT NULL,
	status      TEXT NOT NULL, -- PAID | FAILED
	reason      TEXT NOT NULL DEFAULT '',
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);`

var seedAccounts = map[string]float64{"Jeremy Delgado": 500, "Angel Alvarez": 20, "Erick Cevallos": 1000}

type Account struct {
	CustomerID string  `json:"customer_id"`
	Balance    float64 `json:"balance"`
}

type Payment struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Reason     string  `json:"reason,omitempty"`
}

var ErrNotFound = errors.New("pago no encontrado")

type Service struct {
	db  *pgxpool.Pool
	bus broker.Broker
}

func New(db *pgxpool.Pool, bus broker.Broker) *Service { return &Service{db: db, bus: bus} }

func (s *Service) Start(ctx context.Context) error {
	for c, b := range seedAccounts {
		if _, err := s.db.Exec(ctx,
			`INSERT INTO accounts (customer_id, balance) VALUES ($1, $2) ON CONFLICT DO NOTHING`, c, b); err != nil {
			return err
		}
	}
	return s.bus.Subscribe(ctx, events.TopicInventory, "payment-on-inventory", s.onInventoryEvent)
}

// onInventoryEvent: pasos ④ y ⑤. Transacción local T3 = cobrar.
func (s *Service) onInventoryEvent(ctx context.Context, e events.Event) error {
	if e.Type != events.InventoryReserved {
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
		logx.Log("payment", logx.Gray, "evento %s repetido para %s: ignorado (idempotencia)", e.Type, e.OrderID)
		return tx.Commit(ctx)
	}

	// Cobro atómico: solo se descuenta si alcanza el saldo.
	var left float64
	err = tx.QueryRow(ctx,
		`UPDATE accounts SET balance = balance - $2 WHERE customer_id = $1 AND balance >= $2 RETURNING balance::float8`,
		e.CustomerID, e.Amount).Scan(&left)

	if errors.Is(err, pgx.ErrNoRows) {
		var balance float64
		_ = tx.QueryRow(ctx, `SELECT COALESCE((SELECT balance::float8 FROM accounts WHERE customer_id = $1), 0)`, e.CustomerID).Scan(&balance)
		out := events.New(events.PaymentFailed, e)
		out.Reason = fmt.Sprintf("fondos insuficientes (saldo $%.2f, total $%.2f)", balance, e.Amount)
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (order_id, customer_id, amount, status, reason) VALUES ($1, $2, $3, 'FAILED', $4)`,
			e.OrderID, e.CustomerID, e.Amount, out.Reason); err != nil {
			return err
		}
		if err := store.AddToOutbox(ctx, tx, events.TopicPayment, out); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		logx.Log("payment", logx.Red, "④ %s ✗ pago rechazado: %s", e.OrderID, out.Reason)
		return nil
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO payments (order_id, customer_id, amount, status) VALUES ($1, $2, $3, 'PAID')`,
		e.OrderID, e.CustomerID, e.Amount); err != nil {
		return err
	}
	if err := store.AddToOutbox(ctx, tx, events.TopicPayment, events.New(events.PaymentProcessed, e)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logx.Log("payment", logx.Purple, "④⑤ %s: cobrados $%.2f a %s (saldo $%.2f)", e.OrderID, e.Amount, e.CustomerID, left)
	return nil
}

// Accounts devuelve los saldos.
func (s *Service) Accounts(ctx context.Context) ([]Account, error) {
	rows, err := s.db.Query(ctx, `SELECT customer_id, balance::float8 FROM accounts ORDER BY customer_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Account])
}

// Get devuelve el pago de un pedido.
func (s *Service) Get(ctx context.Context, orderID string) (Payment, error) {
	var p Payment
	err := s.db.QueryRow(ctx,
		`SELECT order_id, customer_id, amount::float8, status, reason FROM payments WHERE order_id = $1`, orderID).
		Scan(&p.OrderID, &p.CustomerID, &p.Amount, &p.Status, &p.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	return p, err
}

// SetBalance crea o ajusta el saldo de un cliente (API de administración).
func (s *Service) SetBalance(ctx context.Context, customerID string, balance float64) error {
	if customerID == "" || balance < 0 {
		return fmt.Errorf("cliente o saldo inválido")
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO accounts (customer_id, balance) VALUES ($1, $2)
		ON CONFLICT (customer_id) DO UPDATE SET balance = EXCLUDED.balance`, customerID, balance)
	return err
}
