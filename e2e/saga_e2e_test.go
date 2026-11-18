//go:build e2e

// Pruebas de extremo a extremo: se ejecutan contra los contenedores reales
// (3 servicios + 3 Postgres + NATS). Requieren el sistema levantado:
//
//	docker compose up -d --build --wait
//	go test -tags e2e -v ./e2e
//
// Cada prueba crea sus propios productos y clientes (con un sufijo único), así
// que se pueden repetir aunque los volúmenes ya tengan datos.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"saga-coreografia/internal/client"
	"saga-coreografia/internal/events"
	"saga-coreografia/internal/inventory"
	"saga-coreografia/internal/order"
	"saga-coreografia/internal/platform"
)

var c = client.FromEnv()

func setup(t *testing.T, price float64, stock int, balance float64) (product, customer string) {
	t.Helper()
	ctx := context.Background()
	if err := c.WaitReady(ctx, 60*time.Second); err != nil {
		t.Fatal(err)
	}
	suffix := events.NewID()[:6]
	product, customer = "prod-"+suffix, "cliente-"+suffix
	for _, err := range []error{
		c.SetPrice(ctx, product, price),
		c.SetStock(ctx, product, stock),
		c.SetBalance(ctx, customer, balance),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	return product, customer
}

func place(t *testing.T, customer, product string, qty int) order.Order {
	t.Helper()
	ctx := context.Background()
	o, err := c.PlaceOrder(ctx, customer, product, qty)
	if err != nil {
		t.Fatal(err)
	}
	if o.Status != order.Pending {
		t.Fatalf("la API debe responder PENDING (consistencia eventual), respondió %s", o.Status)
	}
	final, err := c.WaitFinal(ctx, o.ID, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return final
}

func expectStock(t *testing.T, product string, want int) {
	t.Helper()
	got, err := c.Stock(context.Background(), product)
	if err != nil || got != want {
		t.Fatalf("stock de %s = %d (err %v), se esperaba %d", product, got, err, want)
	}
}

func expectBalance(t *testing.T, customer string, want float64) {
	t.Helper()
	got, err := c.Balance(context.Background(), customer)
	if err != nil || got != want {
		t.Fatalf("saldo de %s = %.2f (err %v), se esperaba %.2f", customer, got, err, want)
	}
}

// expectKardex verifica la tabla inventory_movements del producto: los tipos de
// movimiento en orden, que cada fila parta del stock en que quedó la anterior
// y que la suma de movimientos sea igual al stock actual.
func expectKardex(t *testing.T, product string, wantTypes ...string) []inventory.Movement {
	t.Helper()
	ctx := context.Background()
	list, err := c.Movements(ctx, product)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(list) // la API devuelve el más reciente primero

	var types []string
	sum, prev := 0, 0
	for _, m := range list {
		types = append(types, m.MovementType)
		if m.StockBefore != prev || m.StockAfter != m.StockBefore+m.Quantity {
			t.Fatalf("kardex roto en el movimiento %d: %+v (stock anterior %d)", m.ID, m, prev)
		}
		prev = m.StockAfter
		sum += m.Quantity
	}
	if wantTypes != nil && !slices.Equal(types, wantTypes) {
		t.Fatalf("movimientos = %v, se esperaba %v", types, wantTypes)
	}
	stock, _ := c.Stock(ctx, product)
	if sum != stock {
		t.Fatalf("la suma de movimientos (%d) no cuadra con el stock (%d)", sum, stock)
	}
	return list
}

// CU-01: todos los pasos tienen éxito.
func TestCaminoFeliz(t *testing.T) {
	product, customer := setup(t, 59, 10, 100)
	o := place(t, customer, product, 1)

	if o.Status != order.Confirmed {
		t.Fatalf("estado = %s, se esperaba CONFIRMED", o.Status)
	}
	expectStock(t, product, 9)
	expectBalance(t, customer, 41)
	expectKardex(t, product, "CARGA_INICIAL", "RESERVA")
	if p, err := c.GetPayment(context.Background(), o.ID); err != nil || p.Status != "PAID" {
		t.Fatalf("pago = %+v (err %v), se esperaba PAID", p, err)
	}
}

// CU-02: el pago falla y la compensación libera el stock (caso Black Friday).
func TestPagoFallidoCompensaInventario(t *testing.T) {
	product, customer := setup(t, 99, 3, 20)
	o := place(t, customer, product, 1)

	if o.Status != order.Cancelled {
		t.Fatalf("estado = %s, se esperaba CANCELLED", o.Status)
	}
	expectStock(t, product, 3) // el stock NO queda bloqueado
	expectBalance(t, customer, 20)
	// El kardex muestra la salida (RESERVA -1) y la compensación (LIBERACION +1).
	k := expectKardex(t, product, "CARGA_INICIAL", "RESERVA", "LIBERACION")
	if k[1].Quantity != -1 || k[2].Quantity != 1 || k[2].OrderID != o.ID || k[2].EventType != "PaymentFailed" {
		t.Fatalf("movimientos inesperados: %+v", k)
	}
	if p, err := c.GetPayment(context.Background(), o.ID); err != nil || p.Status != "FAILED" {
		t.Fatalf("pago = %+v (err %v), se esperaba FAILED", p, err)
	}
}

// CU-03: no hay stock; Payment nunca interviene.
func TestSinStockCancelaSinCobrar(t *testing.T) {
	product, customer := setup(t, 99, 2, 1000)
	o := place(t, customer, product, 5)

	if o.Status != order.Cancelled {
		t.Fatalf("estado = %s, se esperaba CANCELLED", o.Status)
	}
	expectStock(t, product, 2)
	expectBalance(t, customer, 1000)
	expectKardex(t, product, "CARGA_INICIAL", "RECHAZO")
	if _, err := c.GetPayment(context.Background(), o.ID); err != client.ErrNotFound {
		t.Fatalf("Payment no debió procesar nada (err %v)", err)
	}
}

// CU-04: el broker entrega el mismo evento dos veces ("al menos una vez").
// Se publica directo en NATS, sin pasar por la deduplicación del broker, para
// probar la idempotencia de los servicios (tabla processed_events).
func TestEventoDuplicadoEsIdempotente(t *testing.T) {
	product, customer := setup(t, 29, 10, 1000)

	nc, err := nats.Connect(platform.Env("NATS_URL", "nats://localhost:4222"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	e := events.New(events.OrderCreated, events.Event{
		OrderID: "DUP-" + product, CustomerID: customer, ProductID: product, Quantity: 2, Amount: 58,
	})
	data, _ := json.Marshal(e)
	for i := 0; i < 2; i++ { // mismo evento, dos entregas
		if err := nc.Publish(events.TopicOrder, data); err != nil {
			t.Fatal(err)
		}
	}
	_ = nc.Flush()

	deadline := time.Now().Add(15 * time.Second)
	for {
		b, _ := c.Balance(context.Background(), customer)
		if b == 942 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)                          // margen por si llegara un segundo cobro
	expectStock(t, product, 8)                           // reservado una sola vez
	expectBalance(t, customer, 942)                      // cobrado una sola vez
	expectKardex(t, product, "CARGA_INICIAL", "RESERVA") // una sola reserva
}

// CU-05: Black Friday, 20 pedidos simultáneos para 5 unidades.
func TestPedidosConcurrentes(t *testing.T) {
	product, customer := setup(t, 10, 5, 100000)

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan order.Order, 20)
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, err := c.PlaceOrder(ctx, customer, product, 1)
			if err == nil {
				o, err = c.WaitFinal(ctx, o.ID, 30*time.Second)
			}
			if err != nil {
				errs <- err
				return
			}
			results <- o
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	count := map[order.Status]int{}
	for o := range results {
		count[o.Status]++
	}
	if count[order.Confirmed] != 5 || count[order.Cancelled] != 15 {
		t.Fatalf("resultado %v, se esperaba 5 CONFIRMED y 15 CANCELLED", count)
	}
	expectStock(t, product, 0)
	expectBalance(t, customer, 100000-5*10)
	movs := expectKardex(t, product)
	kinds := map[string]int{}
	for _, m := range movs {
		kinds[m.MovementType]++
	}
	if kinds["RESERVA"] != 5 || kinds["RECHAZO"] != 15 {
		t.Fatalf("kardex %v, se esperaba 5 RESERVA y 15 RECHAZO", kinds)
	}
	fmt.Printf("    20 pedidos simultáneos → %d CONFIRMED, %d CANCELLED, stock final 0\n",
		count[order.Confirmed], count[order.Cancelled])
}
