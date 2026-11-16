// Package events define el "contrato" entre microservicios: los tópicos del
// broker y los eventos que viajan por ellos. Es lo ÚNICO que los servicios
// comparten; ninguno conoce el código ni la base de datos de otro.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Tópicos (subjects de NATS). Cada servicio publica SOLO en el suyo.
const (
	TopicOrder     = "saga.order"
	TopicInventory = "saga.inventory"
	TopicPayment   = "saga.payment"
)

// Type es el nombre del evento (algo que YA ocurrió, por eso va en pasado).
type Type string

const (
	// Publicado por Order en saga.order.
	OrderCreated Type = "OrderCreated"

	// Publicados por Inventory en saga.inventory.
	InventoryReserved Type = "InventoryReserved" // transacción local T2 OK
	InventoryRejected Type = "InventoryRejected" // sin stock: T2 falló
	StockReleased     Type = "StockReleased"     // transacción COMPENSATORIA de T2

	// Publicados por Payment en saga.payment.
	PaymentProcessed Type = "PaymentProcessed" // transacción local T3 OK
	PaymentFailed    Type = "PaymentFailed"    // T3 falló -> dispara compensaciones
)

// Event es el mensaje que viaja por el broker (serializado como JSON).
type Event struct {
	ID         string    `json:"id"` // único: permite descartar duplicados (idempotencia)
	Type       Type      `json:"type"`
	OrderID    string    `json:"order_id"` // "correlation id": une todos los eventos de una saga
	CustomerID string    `json:"customer_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	Amount     float64   `json:"amount"`
	Reason     string    `json:"reason,omitempty"` // motivo de un fallo, si lo hay
	OccurredAt time.Time `json:"occurred_at"`
}

// New crea un evento nuevo de tipo t copiando los datos de negocio de base.
func New(t Type, base Event) Event {
	base.ID = NewID()
	base.Type = t
	base.OccurredAt = time.Now().UTC()
	return base
}

// NewID genera un identificador aleatorio.
func NewID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
