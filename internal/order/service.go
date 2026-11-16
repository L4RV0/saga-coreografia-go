// Package order es el Order Service. Inicia la saga y es quien decide el
// estado final del pedido (CONFIRMED o CANCELLED) según los eventos que
// escucha de Inventory y Payment.
package order

import (
	"errors"
	"fmt"
	"sync"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

type Status string

const (
	Pending   Status = "PENDING"   // saga en curso
	Confirmed Status = "CONFIRMED" // todos los pasos OK
	Cancelled Status = "CANCELLED" // un paso falló y se compensó
)

type Order struct {
	ID         string
	CustomerID string
	ProductID  string
	Quantity   int
	Amount     float64
	Status     Status
	Reason     string
}

// Service tiene su PROPIA base de datos (Database-per-Service). Nadie más
// puede leerla ni escribirla.
type Service struct {
	bus broker.Broker

	mu        sync.Mutex
	db        map[string]*Order  // "Order DB"
	prices    map[string]float64 // catálogo de precios
	processed map[string]bool    // IDs de eventos ya procesados (idempotencia)
	seq       int
}

func New(bus broker.Broker, prices map[string]float64) *Service {
	s := &Service{
		bus:       bus,
		db:        make(map[string]*Order),
		prices:    prices,
		processed: make(map[string]bool),
	}
	// Coreografía: Order reacciona a lo que publican los demás.
	bus.Subscribe(events.TopicPayment, "order-service", s.onPaymentEvent)
	bus.Subscribe(events.TopicInventory, "order-service", s.onInventoryEvent)
	return s
}

// PlaceOrder es el PASO 1 de la saga (T1): transacción local que crea el
// pedido en estado PENDING y publica OrderCreated. No espera a nadie.
func (s *Service) PlaceOrder(customerID, productID string, qty int) (Order, error) {
	price, ok := s.prices[productID]
	if !ok {
		return Order{}, fmt.Errorf("producto %q no existe", productID)
	}
	if qty <= 0 {
		return Order{}, errors.New("la cantidad debe ser mayor a 0")
	}

	s.mu.Lock()
	s.seq++
	o := &Order{
		ID:         fmt.Sprintf("ORD-%03d", s.seq),
		CustomerID: customerID,
		ProductID:  productID,
		Quantity:   qty,
		Amount:     price * float64(qty),
		Status:     Pending,
	}
	s.db[o.ID] = o // commit local en la Order DB
	snapshot := *o
	s.mu.Unlock()

	logx.Log("order", logx.Blue, "① Pedido %s creado (%s x%d, $%.2f) → PENDING", o.ID, productID, qty, o.Amount)

	s.bus.Publish(events.TopicOrder, events.New(events.OrderCreated, events.Event{
		OrderID:    o.ID,
		CustomerID: customerID,
		ProductID:  productID,
		Quantity:   qty,
		Amount:     o.Amount,
	}))
	return snapshot, nil
}

// onPaymentEvent: paso ⑥ del camino feliz.
func (s *Service) onPaymentEvent(e events.Event) {
	if e.Type != events.PaymentProcessed || s.alreadyProcessed(e.ID) {
		// PaymentFailed lo ignoramos aquí: el pedido se cancela cuando
		// Inventory confirme que ya liberó el stock (StockReleased).
		return
	}
	s.setStatus(e.OrderID, Confirmed, "")
	logx.Log("order", logx.Blue, "⑥ Pago confirmado → pedido %s CONFIRMED ", e.OrderID)
}

// onInventoryEvent: aquí terminan los caminos de fallo.
func (s *Service) onInventoryEvent(e events.Event) {
	switch e.Type {
	case events.StockReleased, events.InventoryRejected:
	default:
		return
	}
	if s.alreadyProcessed(e.ID) {
		return
	}
	// Transacción COMPENSATORIA de T1: no se borra el pedido, se cancela.
	s.setStatus(e.OrderID, Cancelled, e.Reason)
	logx.Log("order", logx.Red, "↩ CancelOrder: pedido %s CANCELLED  (%s)", e.OrderID, e.Reason)
}

func (s *Service) setStatus(id string, st Status, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.db[id]; ok && o.Status == Pending {
		o.Status = st
		o.Reason = reason
	}
}

func (s *Service) alreadyProcessed(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.processed[eventID] {
		return true
	}
	s.processed[eventID] = true
	return false
}

// Get devuelve una copia del pedido.
func (s *Service) Get(id string) (Order, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.db[id]
	if !ok {
		return Order{}, false
	}
	return *o, true
}
