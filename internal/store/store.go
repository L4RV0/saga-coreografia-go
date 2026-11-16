// Package store tiene la parte de base de datos que todos los servicios usan
// igual: conexión a SU Postgres, tabla de eventos procesados (idempotencia) y
// el patrón Transactional Outbox.
//
// Outbox: el cambio de negocio y el evento a publicar se guardan en la MISMA
// transacción local. Un proceso aparte (Relay) lee la tabla outbox y publica
// en el broker. Así nunca queda un cambio guardado sin su evento, ni un evento
// publicado de un cambio que no se guardó.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

// CommonSchema son las tablas que TODA base de datos de servicio tiene.
const CommonSchema = `
CREATE TABLE IF NOT EXISTS processed_events (
	event_id     TEXT PRIMARY KEY,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
	id           BIGSERIAL PRIMARY KEY,
	topic        TEXT NOT NULL,
	event        JSONB NOT NULL,
	created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
	published_at TIMESTAMPTZ
);`

// Open se conecta al Postgres del servicio (reintentando mientras arranca) y
// crea sus tablas.
func Open(ctx context.Context, url, service, schema string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error
	for i := 0; i < 30; i++ {
		pool, err = pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
			pool.Close()
		}
		logx.Log(service, logx.Gray, "esperando a la base de datos (%v)", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("conectando a la base de datos: %w", err)
	}
	if _, err := pool.Exec(ctx, CommonSchema+schema); err != nil {
		return nil, fmt.Errorf("creando tablas: %w", err)
	}
	return pool, nil
}

// MarkProcessed registra el evento dentro de la transacción. Devuelve false si
// ya se había procesado (entrega duplicada): en ese caso no se hace nada más.
func MarkProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// AddToOutbox guarda el evento a publicar dentro de la misma transacción del
// cambio de negocio.
func AddToOutbox(ctx context.Context, tx pgx.Tx, topic string, e events.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, event) VALUES ($1, $2)`, topic, data)
	return err
}

// RunRelay publica en el broker los eventos pendientes de la outbox, en orden.
func RunRelay(ctx context.Context, db *pgxpool.Pool, b broker.Broker, service string) {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := relayOnce(ctx, db, b, service); err != nil && ctx.Err() == nil {
				logx.Log(service, logx.Red, "outbox: %v (se reintentará)", err)
			}
		}
	}
}

func relayOnce(ctx context.Context, db *pgxpool.Pool, b broker.Broker, service string) error {
	rows, err := db.Query(ctx,
		`SELECT id, topic, event FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100`)
	if err != nil {
		return err
	}
	type pending struct {
		id    int64
		topic string
		event events.Event
	}
	var list []pending
	for rows.Next() {
		var p pending
		var raw []byte
		if err := rows.Scan(&p.id, &p.topic, &raw); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(raw, &p.event); err != nil {
			rows.Close()
			return err
		}
		list = append(list, p)
	}
	rows.Close()

	for _, p := range list {
		if err := b.Publish(ctx, p.topic, p.event); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, p.id); err != nil {
			return err
		}
		logx.Log(service, logx.Gray, "📨 %-17s → %s (%s)", p.event.Type, p.topic, p.event.OrderID)
	}
	return nil
}
