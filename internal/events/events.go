// Package events define el "contrato" entre microservicios: los tópicos del
// broker y los eventos que viajan por ellos. Es lo ÚNICO que los servicios
// comparten; ninguno conoce el código ni la base de datos de otro.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Tópicos del broker (igual que en el video: order / inventory / payment topic).
// Cada servicio publica SOLO en su propio tópico.
const (
	TopicOrder     = "order-topic"
	TopicInventory = "inventory-topic"
	TopicPayment   = "payment-topic"
)

// Type es el nombre del evento (algo que YA ocurrió, por eso va en pasado).
type Type string

const (
	// Publicado por Order en order-topic.
	OrderCreated Type = "OrderCreated"

	// Publicados por Inventory en inventory-topic.
	InventoryReserved Type = "InventoryReserved" // transacción local T2 OK
	InventoryRejected Type = "InventoryRejected" // sin stock: T2 falló
	StockReleased     Type = "StockReleased"     // transacción COMPENSATORIA de T2

	// Publicados por Payment en payment-topic.
	PaymentProcessed Type = "PaymentProcessed" // transacción local T3 OK
	PaymentFailed    Type = "PaymentFailed"    // T3 falló -> dispara compensaciones
)

// Event es el mensaje que viaja por el broker.
type Event struct {
	ID         string // identificador único: permite descartar duplicados (idempotencia)
	Type       Type
	OrderID    string // "correlation id": une todos los eventos de una misma saga
	CustomerID string
	ProductID  string
	Quantity   int
	Amount     float64
	Reason     string // motivo de un fallo, si lo hay
	OccurredAt time.Time
}

// New crea un evento nuevo de tipo t copiando los datos de negocio de base.
func New(t Type, base Event) Event {
	base.ID = newID()
	base.Type = t
	base.OccurredAt = time.Now()
	return base
}

func newID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
