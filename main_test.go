package main

import (
	"sync"
	"testing"

	"saga-coreografia/internal/events"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/order"
)

func init() { logx.SetEnabled(false) }

func TestCaminoFeliz(t *testing.T) {
	sys := newSystem(map[string]int{"runner-pro": 10}, map[string]float64{"Jeremy Delgado": 100})
	defer sys.bus.Close()

	o, _ := sys.orders.PlaceOrder("Jeremy Delgado", "runner-pro", 1)
	sys.bus.WaitIdle()

	got, _ := sys.orders.Get(o.ID)
	if got.Status != order.Confirmed {
		t.Fatalf("estado = %s, se esperaba CONFIRMED", got.Status)
	}
	if s := sys.inventory.Stock("runner-pro"); s != 9 {
		t.Fatalf("stock = %d, se esperaba 9", s)
	}
	if b := sys.payments.Balance("Jeremy Delgado"); b != 41 {
		t.Fatalf("saldo = %.2f, se esperaba 41", b)
	}
}

func TestPagoFallidoCompensaInventario(t *testing.T) {
	sys := newSystem(map[string]int{"studio-buds": 3}, map[string]float64{"Angel Alvarez": 20})
	defer sys.bus.Close()

	o, _ := sys.orders.PlaceOrder("Angel Alvarez", "studio-buds", 1)
	sys.bus.WaitIdle()

	got, _ := sys.orders.Get(o.ID)
	if got.Status != order.Cancelled {
		t.Fatalf("estado = %s, se esperaba CANCELLED", got.Status)
	}
	// La compensación devolvió el stock: el inventario NO queda bloqueado.
	if s := sys.inventory.Stock("studio-buds"); s != 3 {
		t.Fatalf("stock = %d, se esperaba 3 (stock liberado)", s)
	}
	if b := sys.payments.Balance("Angel Alvarez"); b != 20 {
		t.Fatalf("saldo = %.2f, no se debió cobrar", b)
	}
}

func TestSinStockCancelaSinCobrar(t *testing.T) {
	sys := newSystem(map[string]int{"studio-buds": 2}, map[string]float64{"Erick Cevallos": 1000})
	defer sys.bus.Close()

	o, _ := sys.orders.PlaceOrder("Erick Cevallos", "studio-buds", 5)
	sys.bus.WaitIdle()

	got, _ := sys.orders.Get(o.ID)
	if got.Status != order.Cancelled {
		t.Fatalf("estado = %s, se esperaba CANCELLED", got.Status)
	}
	if _, ok := sys.payments.Get(o.ID); ok {
		t.Fatal("Payment no debió procesar nada")
	}
}

// Un broker real puede entregar el mismo mensaje dos veces ("at-least-once").
// Los servicios son idempotentes: el duplicado se ignora.
func TestEventoDuplicadoEsIdempotente(t *testing.T) {
	sys := newSystem(map[string]int{"mystery-box": 10}, map[string]float64{"Jeremy Delgado": 1000})
	defer sys.bus.Close()

	e := events.New(events.OrderCreated, events.Event{
		OrderID: "ORD-X", CustomerID: "Jeremy Delgado", ProductID: "mystery-box", Quantity: 2, Amount: 58,
	})
	sys.bus.Publish(events.TopicOrder, e)
	sys.bus.Publish(events.TopicOrder, e) // duplicado
	sys.bus.WaitIdle()

	if s := sys.inventory.Stock("mystery-box"); s != 8 {
		t.Fatalf("stock = %d, se esperaba 8 (reservado una sola vez)", s)
	}
	if b := sys.payments.Balance("Jeremy Delgado"); b != 942 {
		t.Fatalf("saldo = %.2f, se esperaba 942 (cobrado una sola vez)", b)
	}
}

// Black Friday: 20 pedidos simultáneos para 5 unidades. Nunca se vende de más
// y cada pedido termina en un estado consistente.
func TestPedidosConcurrentes(t *testing.T) {
	sys := newSystem(map[string]int{"studio-buds": 5}, map[string]float64{"Erick Cevallos": 100000})
	defer sys.bus.Close()

	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, _ := sys.orders.PlaceOrder("Erick Cevallos", "studio-buds", 1)
			ids <- o.ID
		}()
	}
	wg.Wait()
	close(ids)
	sys.bus.WaitIdle()

	confirmed, cancelled := 0, 0
	for id := range ids {
		o, _ := sys.orders.Get(id)
		switch o.Status {
		case order.Confirmed:
			confirmed++
		case order.Cancelled:
			cancelled++
		default:
			t.Fatalf("pedido %s quedó en %s", id, o.Status)
		}
	}
	if confirmed != 5 || cancelled != 15 {
		t.Fatalf("confirmados=%d cancelados=%d, se esperaba 5/15", confirmed, cancelled)
	}
	if s := sys.inventory.Stock("studio-buds"); s != 0 {
		t.Fatalf("stock = %d, se esperaba 0", s)
	}
}
