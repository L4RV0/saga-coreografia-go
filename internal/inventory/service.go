// Package inventory es el Inventory Service. Reserva stock cuando se crea un
// pedido y lo LIBERA (compensación) si el pago falla.
package inventory

import (
	"fmt"
	"sync"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

type reservation struct {
	ProductID string
	Quantity  int
	Released  bool
}

type Service struct {
	bus broker.Broker

	mu           sync.Mutex
	stock        map[string]int          // "Inventory DB": unidades disponibles
	reservations map[string]*reservation // orderID -> reserva
	processed    map[string]bool
}

func New(bus broker.Broker, initialStock map[string]int) *Service {
	s := &Service{
		bus:          bus,
		stock:        make(map[string]int),
		reservations: make(map[string]*reservation),
		processed:    make(map[string]bool),
	}
	for k, v := range initialStock {
		s.stock[k] = v
	}
	bus.Subscribe(events.TopicOrder, "inventory-service", s.onOrderEvent)
	bus.Subscribe(events.TopicPayment, "inventory-service", s.onPaymentEvent)
	return s
}

// onOrderEvent: pasos ② y ③. Transacción local T2 = reservar stock.
func (s *Service) onOrderEvent(e events.Event) {
	if e.Type != events.OrderCreated {
		return
	}

	s.mu.Lock()
	if s.processed[e.ID] {
		s.mu.Unlock()
		return
	}
	s.processed[e.ID] = true

	available := s.stock[e.ProductID]
	if available < e.Quantity {
		s.mu.Unlock()
		reason := fmt.Sprintf("sin stock de %s (hay %d, se piden %d)", e.ProductID, available, e.Quantity)
		logx.Log("inventory", logx.Red, "② %s ✗ %s", e.OrderID, reason)
		out := events.New(events.InventoryRejected, e)
		out.Reason = reason
		s.bus.Publish(events.TopicInventory, out)
		return
	}

	s.stock[e.ProductID] = available - e.Quantity // commit local en la Inventory DB
	s.reservations[e.OrderID] = &reservation{ProductID: e.ProductID, Quantity: e.Quantity}
	left := s.stock[e.ProductID]
	s.mu.Unlock()

	logx.Log("inventory", logx.Green, "②③ %s: reservadas %d u. de %s (quedan %d) ✓", e.OrderID, e.Quantity, e.ProductID, left)
	s.bus.Publish(events.TopicInventory, events.New(events.InventoryReserved, e))
}

// onPaymentEvent: si el pago falló ejecuta la TRANSACCIÓN COMPENSATORIA de
// T2 (liberar el stock) y avisa con StockReleased.
func (s *Service) onPaymentEvent(e events.Event) {
	if e.Type != events.PaymentFailed {
		return
	}

	s.mu.Lock()
	if s.processed[e.ID] {
		s.mu.Unlock()
		return
	}
	s.processed[e.ID] = true

	r, ok := s.reservations[e.OrderID]
	if !ok || r.Released {
		s.mu.Unlock()
		return
	}
	s.stock[r.ProductID] += r.Quantity // devolvemos lo reservado
	r.Released = true
	left := s.stock[r.ProductID]
	s.mu.Unlock()

	logx.Log("inventory", logx.Yellow, "↩ COMPENSANDO %s: liberadas %d u. de %s (quedan %d)", e.OrderID, r.Quantity, r.ProductID, left)
	s.bus.Publish(events.TopicInventory, events.New(events.StockReleased, e))
}

// Stock devuelve las unidades disponibles de un producto.
func (s *Service) Stock(productID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stock[productID]
}
