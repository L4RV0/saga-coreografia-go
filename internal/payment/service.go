// Package payment es el Payment Service. Cobra el pedido una vez que el stock
// está reservado. Es el último paso: si falla, dispara las compensaciones.
package payment

import (
	"fmt"
	"sync"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
)

type Status string

const (
	Paid   Status = "PAID"
	Failed Status = "FAILED"
)

type Payment struct {
	OrderID    string
	CustomerID string
	Amount     float64
	Status     Status
}

type Service struct {
	bus broker.Broker

	mu        sync.Mutex
	balances  map[string]float64  // "Payment DB": saldo de cada cliente
	payments  map[string]*Payment // orderID -> pago
	processed map[string]bool
}

func New(bus broker.Broker, initialBalances map[string]float64) *Service {
	s := &Service{
		bus:       bus,
		balances:  make(map[string]float64),
		payments:  make(map[string]*Payment),
		processed: make(map[string]bool),
	}
	for k, v := range initialBalances {
		s.balances[k] = v
	}
	bus.Subscribe(events.TopicInventory, "payment-service", s.onInventoryEvent)
	return s
}

// onInventoryEvent: pasos ④ y ⑤. Transacción local T3 = cobrar.
func (s *Service) onInventoryEvent(e events.Event) {
	if e.Type != events.InventoryReserved {
		return
	}

	s.mu.Lock()
	if s.processed[e.ID] {
		s.mu.Unlock()
		return
	}
	s.processed[e.ID] = true

	balance := s.balances[e.CustomerID]
	if balance < e.Amount {
		// Se registra el intento fallido en la Payment DB.
		s.payments[e.OrderID] = &Payment{OrderID: e.OrderID, CustomerID: e.CustomerID, Amount: e.Amount, Status: Failed}
		s.mu.Unlock()

		reason := fmt.Sprintf("fondos insuficientes (saldo $%.2f, total $%.2f)", balance, e.Amount)
		logx.Log("payment", logx.Red, "④ %s ✗ pago rechazado: %s", e.OrderID, reason)
		out := events.New(events.PaymentFailed, e)
		out.Reason = reason
		s.bus.Publish(events.TopicPayment, out)
		return
	}

	s.balances[e.CustomerID] = balance - e.Amount // commit local en la Payment DB
	s.payments[e.OrderID] = &Payment{OrderID: e.OrderID, CustomerID: e.CustomerID, Amount: e.Amount, Status: Paid}
	s.mu.Unlock()

	logx.Log("payment", logx.Purple, "④⑤ %s: cobrados $%.2f a %s ✓", e.OrderID, e.Amount, e.CustomerID)
	s.bus.Publish(events.TopicPayment, events.New(events.PaymentProcessed, e))
}

// Balance devuelve el saldo de un cliente.
func (s *Service) Balance(customerID string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.balances[customerID]
}

// Get devuelve el pago de un pedido.
func (s *Service) Get(orderID string) (Payment, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.payments[orderID]
	if !ok {
		return Payment{}, false
	}
	return *p, true
}
