// Saga basada en Coreografía — caso "Black Friday" del video
// "How Saga Pattern Solves Distributed Transactions in Microservices".
//
// Tres microservicios (Order, Inventory, Payment), cada uno con su propia base
// de datos, que se coordinan SOLO mediante eventos en un broker. No existe un
// orquestador central.
package main

import (
	"fmt"

	"saga-coreografia/internal/broker"
	"saga-coreografia/internal/inventory"
	"saga-coreografia/internal/logx"
	"saga-coreografia/internal/order"
	"saga-coreografia/internal/payment"
)

// Productos de la tienda del video.
var prices = map[string]float64{
	"mystery-box": 29,
	"runner-pro":  59,
	"studio-buds": 99,
}

type system struct {
	bus       *broker.InMemory
	orders    *order.Service
	inventory *inventory.Service
	payments  *payment.Service
}

func newSystem(stock map[string]int, balances map[string]float64) *system {
	bus := broker.NewInMemory()
	return &system{
		bus:       bus,
		orders:    order.New(bus, prices),
		inventory: inventory.New(bus, stock),
		payments:  payment.New(bus, balances),
	}
}

func main() {
	sys := newSystem(
		map[string]int{"mystery-box": 100, "runner-pro": 50, "studio-buds": 3},
		map[string]float64{"Jeremy Delgado": 500, "Angel Alvarez": 20, "Erick Cevallos": 1000},
	)
	defer sys.bus.Close()

	run(sys, "ESCENARIO 1 · Camino feliz: Jeremy Delgado compra 1 Runner Pro", "Jeremy Delgado", "runner-pro", 1)
	run(sys, "ESCENARIO 2 · Falla el pago (caso Black Friday): Angel Alvarez compra 1 Studio Buds", "Angel Alvarez", "studio-buds", 1)
	run(sys, "ESCENARIO 3 · Sin stock: Erick Cevallos compra 5 Studio Buds", "Erick Cevallos", "studio-buds", 5)
}

func run(sys *system, title, customer, product string, qty int) {
	logx.Title("═══ %s ═══", title)
	stockBefore := sys.inventory.Stock(product)
	balanceBefore := sys.payments.Balance(customer)

	o, err := sys.orders.PlaceOrder(customer, product, qty)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	// La saga es ASÍNCRONA: PlaceOrder ya devolvió el pedido en PENDING.
	// Esperamos a que el broker termine de repartir todos los eventos.
	sys.bus.WaitIdle()

	final, _ := sys.orders.Get(o.ID)
	pay, hasPay := sys.payments.Get(o.ID)
	payStatus := "— (nunca llegó a cobrarse)"
	if hasPay {
		payStatus = string(pay.Status)
	}

	fmt.Printf("\n  Estado final en cada base de datos:\n")
	fmt.Printf("  • Order DB     → %s %s\n", final.ID, final.Status)
	fmt.Printf("  • Inventory DB → %s: %d → %d\n", product, stockBefore, sys.inventory.Stock(product))
	fmt.Printf("  • Payment DB   → pago %s | saldo de %s: $%.2f → $%.2f\n", payStatus, customer, balanceBefore, sys.payments.Balance(customer))
}
