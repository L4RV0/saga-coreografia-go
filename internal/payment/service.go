// Package payment es el Payment Service. Cobra (T3) una vez reservado el
// stock y, si el cliente cancela una compra confirmada, REEMBOLSA (C3, la
// primera compensación de la cadena). Cada movimiento de dinero queda en la
// tabla account_movements.
package payment

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
	status      TEXT NOT NULL, -- PAID | FAILED | REFUNDED
	reason      TEXT NOT NULL DEFAULT '',
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE payments ADD COLUMN IF NOT EXISTS refunded_at TIMESTAMPTZ;
-- Movimientos de dinero: una fila por cada transacción que toca un saldo.
CREATE TABLE IF NOT EXISTS account_movements (
	id             BIGSERIAL PRIMARY KEY,
	movement_type  TEXT NOT NULL,           -- SALDO_INICIAL | AJUSTE | CARGO | REEMBOLSO | PAGO_RECHAZADO
	kind           TEXT NOT NULL,           -- TRANSACCION | COMPENSACION
	customer_id    TEXT NOT NULL,
	order_id       TEXT,
	amount         NUMERIC(12,2) NOT NULL,  -- negativo sale dinero, positivo entra, 0 sin cambio
	balance_before NUMERIC(12,2) NOT NULL,
	balance_after  NUMERIC(12,2) NOT NULL,
	event_id       TEXT,
	event_type     TEXT,
	detail         TEXT NOT NULL DEFAULT '',
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	CHECK (balance_after = balance_before + amount)
);
CREATE INDEX IF NOT EXISTS account_movements_customer ON account_movements (customer_id, id);
CREATE INDEX IF NOT EXISTS account_movements_order    ON account_movements (order_id);`

// Tipos de movimiento de dinero.
const (
	SaldoInicial  = "SALDO_INICIAL"
	Ajuste        = "AJUSTE"
	Cargo         = "CARGO"          // T3: se cobra el pedido
	Reembolso     = "REEMBOLSO"      // C3: compensación, se devuelve el cobro
	PagoRechazado = "PAGO_RECHAZADO" // T3 falló: no se mueve dinero
)

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

// Movement es una fila de account_movements.
type Movement struct {
	ID            int64     `json:"id"`
	MovementType  string    `json:"movement_type"`
	Kind          string    `json:"kind"`
	CustomerID    string    `json:"customer_id"`
	OrderID       string    `json:"order_id,omitempty"`
	Amount        float64   `json:"amount"`
	BalanceBefore float64   `json:"balance_before"`
	BalanceAfter  float64   `json:"balance_after"`
	EventID       string    `json:"event_id,omitempty"`
	EventType     string    `json:"event_type,omitempty"`
	Detail        string    `json:"detail"`
	CreatedAt     time.Time `json:"created_at"`
}

var ErrNotFound = errors.New("pago no encontrado")

type Service struct {
	db  *pgxpool.Pool
	bus broker.Broker
}

func New(db *pgxpool.Pool, bus broker.Broker) *Service { return &Service{db: db, bus: bus} }

func (s *Service) Start(ctx context.Context) error {
	for c, b := range seedAccounts {
		if err := s.seed(ctx, c, b); err != nil {
			return err
		}
	}
	// Cuentas creadas antes de existir account_movements: su saldo actual pasa
	// a ser su saldo inicial, para que saldo = suma de movimientos.
	if _, err := s.db.Exec(ctx, `
		INSERT INTO account_movements (movement_type, kind, customer_id, amount, balance_before, balance_after, detail)
		SELECT 'SALDO_INICIAL', 'TRANSACCION', a.customer_id, a.balance, 0, a.balance, 'Saldo inicial al habilitar el registro'
		FROM accounts a
		WHERE NOT EXISTS (SELECT 1 FROM account_movements m WHERE m.customer_id = a.customer_id)`); err != nil {
		return err
	}
	if err := s.bus.Subscribe(ctx, events.TopicInventory, "payment-on-inventory", s.onInventoryEvent); err != nil {
		return err
	}
	return s.bus.Subscribe(ctx, events.TopicOrder, "payment-on-order", s.onOrderEvent)
}

func (s *Service) seed(ctx context.Context, customer string, balance float64) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`INSERT INTO accounts (customer_id, balance) VALUES ($1, $2) ON CONFLICT DO NOTHING`, customer, balance)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if err := recordMovement(ctx, tx, Movement{MovementType: SaldoInicial, Kind: "TRANSACCION",
			CustomerID: customer, Amount: balance, BalanceBefore: 0, BalanceAfter: balance,
			Detail: "Saldo inicial del cliente"}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// recordMovement escribe el movimiento dentro de la transacción del cambio de
// saldo: si el cambio se guarda, su movimiento también.
func recordMovement(ctx context.Context, tx pgx.Tx, m Movement) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO account_movements
			(movement_type, kind, customer_id, order_id, amount, balance_before, balance_after, event_id, event_type, detail)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), $10)`,
		m.MovementType, m.Kind, m.CustomerID, m.OrderID, m.Amount, m.BalanceBefore, m.BalanceAfter,
		m.EventID, m.EventType, m.Detail)
	return err
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
		if err := recordMovement(ctx, tx, Movement{MovementType: PagoRechazado, Kind: "TRANSACCION",
			CustomerID: e.CustomerID, OrderID: e.OrderID, Amount: 0, BalanceBefore: balance, BalanceAfter: balance,
			EventID: e.ID, EventType: string(e.Type),
			Detail: fmt.Sprintf("Pago rechazado: total $%.2f, saldo $%.2f", e.Amount, balance)}); err != nil {
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
	if err := recordMovement(ctx, tx, Movement{MovementType: Cargo, Kind: "TRANSACCION",
		CustomerID: e.CustomerID, OrderID: e.OrderID, Amount: -e.Amount, BalanceBefore: left + e.Amount, BalanceAfter: left,
		EventID: e.ID, EventType: string(e.Type),
		Detail: fmt.Sprintf("T3: cobro del pedido %s", e.OrderID)}); err != nil {
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

// onOrderEvent: el cliente canceló una compra confirmada. TRANSACCIÓN
// COMPENSATORIA de T3: se devuelve el dinero y se avisa con PaymentRefunded.
func (s *Service) onOrderEvent(ctx context.Context, e events.Event) error {
	if e.Type != events.OrderCancellationRequested {
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

	var customer string
	var amount float64
	err = tx.QueryRow(ctx, `
		UPDATE payments SET status = 'REFUNDED', refunded_at = now()
		WHERE order_id = $1 AND status = 'PAID'
		RETURNING customer_id, amount::float8`, e.OrderID).Scan(&customer, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		logx.Log("payment", logx.Gray, "%s no tiene un pago PAID: nada que reembolsar", e.OrderID)
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}

	var left float64
	if err := tx.QueryRow(ctx,
		`UPDATE accounts SET balance = balance + $2 WHERE customer_id = $1 RETURNING balance::float8`,
		customer, amount).Scan(&left); err != nil {
		return err
	}
	if err := recordMovement(ctx, tx, Movement{MovementType: Reembolso, Kind: "COMPENSACION",
		CustomerID: customer, OrderID: e.OrderID, Amount: amount, BalanceBefore: left - amount, BalanceAfter: left,
		EventID: e.ID, EventType: string(e.Type),
		Detail: fmt.Sprintf("C3: reembolso de $%.2f (%s)", amount, e.Reason)}); err != nil {
		return err
	}
	if err := store.AddToOutbox(ctx, tx, events.TopicPayment, events.New(events.PaymentRefunded, e)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logx.Log("payment", logx.Yellow, "↩ C3 COMPENSACIÓN %s: reembolsados $%.2f a %s (saldo $%.2f)", e.OrderID, amount, customer, left)
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

// Movements devuelve los movimientos de dinero, filtrados por cliente y/o pedido.
func (s *Service) Movements(ctx context.Context, customerID, orderID string, limit int) ([]Movement, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, movement_type, kind, customer_id, COALESCE(order_id, ''), amount::float8,
		       balance_before::float8, balance_after::float8, COALESCE(event_id, ''), COALESCE(event_type, ''),
		       detail, created_at
		FROM account_movements
		WHERE ($1 = '' OR customer_id = $1) AND ($2 = '' OR order_id = $2)
		ORDER BY id DESC
		LIMIT $3`, customerID, orderID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Movement])
}

// SetBalance crea o ajusta el saldo de un cliente (API de administración) y lo
// registra en account_movements.
func (s *Service) SetBalance(ctx context.Context, customerID string, balance float64) error {
	if customerID == "" || balance < 0 {
		return fmt.Errorf("cliente o saldo inválido")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var before float64
	err = tx.QueryRow(ctx, `SELECT balance::float8 FROM accounts WHERE customer_id = $1 FOR UPDATE`, customerID).Scan(&before)
	isNew := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !isNew {
		return err
	}
	if !isNew && before == balance {
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO accounts (customer_id, balance) VALUES ($1, $2)
		ON CONFLICT (customer_id) DO UPDATE SET balance = EXCLUDED.balance`, customerID, balance); err != nil {
		return err
	}
	m := Movement{MovementType: Ajuste, Kind: "TRANSACCION", CustomerID: customerID,
		Amount: balance - before, BalanceBefore: before, BalanceAfter: balance, Detail: "Ajuste administrativo de saldo"}
	if isNew {
		m.MovementType, m.Detail = SaldoInicial, "Alta del cliente"
	}
	if err := recordMovement(ctx, tx, m); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
