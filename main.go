// Demo del patrón SAGA basado en Coreografía — caso "Black Friday" del video
// "How Saga Pattern Solves Distributed Transactions in Microservices".
//
// Los tres microservicios corren en Docker, cada uno con su propia base de
// datos Postgres en su propio volumen, y se comunican solo por eventos en NATS.
// Este programa actúa como "cliente": hace pedidos por HTTP y muestra el
// estado final de cada base de datos.
//
//	docker compose up -d --build   # levanta los 7 contenedores
//	go run .                       # ejecuta los 3 escenarios
//	docker compose logs -f order-service inventory-service payment-service
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"saga-coreografia/internal/client"
	"saga-coreografia/internal/logx"
)

func main() {
	ctx := context.Background()
	c := client.FromEnv()

	fmt.Println("Conectando con los microservicios…")
	if err := c.WaitReady(ctx, 60*time.Second); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}

	// Datos iniciales del caso (se reinician en cada demo para que sea repetible).
	must(c.SetPrice(ctx, "runner-pro", 59))
	must(c.SetPrice(ctx, "studio-buds", 99))
	must(c.SetStock(ctx, "runner-pro", 50))
	must(c.SetStock(ctx, "studio-buds", 3))
	must(c.SetBalance(ctx, "Jeremy Delgado", 500))
	must(c.SetBalance(ctx, "Angel Alvarez", 20))
	must(c.SetBalance(ctx, "Erick Cevallos", 1000))

	run(ctx, c, "ESCENARIO 1 · Camino feliz: Jeremy Delgado compra 1 Runner Pro", "Jeremy Delgado", "runner-pro", 1)
	run(ctx, c, "ESCENARIO 2 · Falla el pago (caso Black Friday): Angel Alvarez compra 1 Studio Buds", "Angel Alvarez", "studio-buds", 1)
	run(ctx, c, "ESCENARIO 3 · Sin stock: Erick Cevallos compra 5 Studio Buds", "Erick Cevallos", "studio-buds", 5)

	fmt.Println("\nRecorrido de los eventos entre servicios:")
	fmt.Println("  docker compose logs order-service inventory-service payment-service")
}

func run(ctx context.Context, c *client.Client, title, customer, product string, qty int) {
	logx.Title("═══ %s ═══", title)
	stockBefore, _ := c.Stock(ctx, product)
	balanceBefore, _ := c.Balance(ctx, customer)

	o, err := c.PlaceOrder(ctx, customer, product, qty)
	must(err)
	fmt.Printf("  POST /orders → %s %s (la API responde al instante: la saga sigue sola)\n", o.ID, o.Status)

	final, err := c.WaitFinal(ctx, o.ID, 15*time.Second)
	must(err)
	if final.Reason != "" {
		fmt.Printf("  Motivo: %s\n", final.Reason)
	}

	payStatus := "— (nunca llegó a cobrarse)"
	if p, err := c.GetPayment(ctx, o.ID); err == nil {
		payStatus = p.Status
	}
	stockAfter, _ := c.Stock(ctx, product)
	balanceAfter, _ := c.Balance(ctx, customer)

	fmt.Printf("\n  Estado final en cada base de datos (cada una en su propio contenedor y volumen):\n")
	fmt.Printf("  • order-db     → %s %s\n", final.ID, final.Status)
	fmt.Printf("  • inventory-db → %s: %d → %d\n", product, stockBefore, stockAfter)
	fmt.Printf("  • payment-db   → pago %s | saldo de %s: $%.2f → $%.2f\n", payStatus, customer, balanceBefore, balanceAfter)
}

func must(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
