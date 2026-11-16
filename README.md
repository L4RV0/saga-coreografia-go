# Patrón SAGA basado en Coreografía — Go

Implementación del caso **"Black Friday"** del video *How Saga Pattern Solves Distributed Transactions in Microservices* (Visualcoders).

## 1. El problema (lo que plantea el video)

Una tienda tiene 3 microservicios, **cada uno con su propia base de datos** (patrón *Database per Service*):

| Servicio | Base de datos | Transacción local |
|---|---|---|
| Order | Order DB | T1: crear pedido |
| Inventory | Inventory DB | T2: reservar stock |
| Payment | Payment DB | T3: cobrar |

En Black Friday: el pedido **existía**, el stock quedó **bloqueado**, pero el pago **falló**.
En un monolito bastaba un `ROLLBACK` (ACID, todo o nada). Aquí **no hay una transacción que abarque las 3 bases de datos**, así que no hay nada a qué hacer rollback → el inventario quedaba bloqueado para siempre.

## 2. La solución: Saga

Una **saga** es una secuencia de transacciones locales. Cada paso hace *commit* en su propia BD y publica un evento que dispara el siguiente paso. Si un paso falla, se ejecutan **transacciones compensatorias** que deshacen *a nivel de negocio* lo que hicieron los pasos anteriores:

| Paso | Acción | Compensación |
|---|---|---|
| T1 | Crear pedido (PENDING) | Cancelar pedido (CANCELLED) |
| T2 | Reservar stock | Liberar stock (`StockReleased`) |
| T3 | Cobrar | — (es el último paso; si falla, compensa a los demás) |

> **Compensar ≠ rollback.** Un rollback borra como si nunca hubiera pasado. Una compensación es una **nueva** transacción que deja el sistema consistente (el pedido no se borra: queda registrado como `CANCELLED`).

## 3. ¿Por qué Coreografía?

En **coreografía** no hay un jefe central: cada servicio **escucha eventos** del broker y **reacciona** publicando los suyos. En **orquestación** habría un "Saga Orchestrator" que da órdenes. Esta tarea pide coreografía.

## 4. Flujo implementado (igual a los diagramas del video)

```
            ┌──────── MESSAGE BROKER ────────┐
 Order ──①──▶ order-topic      ──②──▶ Inventory
 Order ◀─⑥── payment-topic     ◀─⑤── Payment
 Payment ◀④─ inventory-topic   ◀─③── Inventory
            └────────────────────────────────┘
```

**Camino feliz**
1. Order crea el pedido `PENDING` y publica `OrderCreated` → *order-topic*
2. Inventory consume `OrderCreated` y reserva el stock
3. Inventory publica `InventoryReserved` → *inventory-topic*
4. Payment consume `InventoryReserved` y cobra
5. Payment publica `PaymentProcessed` → *payment-topic*
6. Order consume `PaymentProcessed` → pedido `CONFIRMED`

**Falla de pago (caso Black Friday) → compensación**
1. Payment publica `PaymentFailed` → *payment-topic*
2. Inventory consume `PaymentFailed`, **libera el stock** y publica `StockReleased`
3. Order consume `StockReleased` → **CancelOrder** → pedido `CANCELLED`

**Extra: sin stock** — Inventory publica `InventoryRejected`; Order cancela y Payment nunca cobra.

### ¿Quién escucha qué?

| Servicio | Publica en | Escucha |
|---|---|---|
| Order | order-topic | payment-topic (`PaymentProcessed`), inventory-topic (`StockReleased`, `InventoryRejected`) |
| Inventory | inventory-topic | order-topic (`OrderCreated`), payment-topic (`PaymentFailed`) |
| Payment | payment-topic | inventory-topic (`InventoryReserved`) |

## 5. Estructura del código

```
saga-coreografia/
├── main.go                      # arma el sistema y corre 3 escenarios
├── main_test.go                 # 5 pruebas automáticas
└── internal/
    ├── events/events.go         # tópicos y eventos (el contrato compartido)
    ├── broker/broker.go         # broker pub/sub asíncrono (goroutines + canales)
    ├── order/service.go         # Order Service   + Order DB
    ├── inventory/service.go     # Inventory Service + Inventory DB
    ├── payment/service.go       # Payment Service + Payment DB
    └── logx/logx.go             # logs con color por servicio
```

- **Ningún servicio importa a otro.** Solo conocen `events` y la interfaz `broker.Broker`. Eso es el bajo acoplamiento de la coreografía.
- **Cada servicio tiene su "base de datos" privada** (un `map` protegido con `sync.Mutex`).
- **El broker es asíncrono**: `Publish` no espera la respuesta; cada suscriptor tiene su goroutine y su cola (canal). Por eso `PlaceOrder` devuelve el pedido en `PENDING` y el estado final llega después (*consistencia eventual*).
- **Idempotencia**: cada evento lleva un `ID` único y cada servicio guarda los IDs ya procesados. Si el broker entrega un mensaje dos veces, no se reserva ni se cobra dos veces.
- **`OrderID` como correlation ID**: une todos los eventos de una misma saga.

## 6. Cómo ejecutarlo

Requisito: Go 1.22 o superior.

```bash
go run .
```

```bash
go test -v ./...
```

Con Docker (compila, prueba y ejecuta dentro del contenedor):

```bash
docker compose up --build
```

Pipeline completo en local (formato, `go vet`, pruebas, cobertura y demo; guarda todo en `evidencia/`):

```bash
powershell -ExecutionPolicy Bypass -File scripts\verificar.ps1
```

El mismo pipeline está configurado para GitHub Actions en `.github/workflows/ci.yml`.

Las pruebas verifican: camino feliz, compensación por pago fallido, cancelación por falta de stock, idempotencia ante eventos duplicados, y 20 pedidos simultáneos sobre 5 unidades (se confirman exactamente 5, nunca se vende de más).

## 7. Limitaciones (y cómo se resolverían en producción)

- **Broker en memoria**: en producción sería Kafka, RabbitMQ o NATS. Solo habría que implementar la interfaz `Broker`; los servicios no cambian.
- **Guardar en la BD y publicar el evento no es atómico**: si el servicio se cae entre ambas cosas, el evento se pierde. En producción se usa el patrón **Transactional Outbox**.
- **Falta de aislamiento**: mientras la saga está en curso, otros pueden ver datos intermedios (stock reservado de un pedido que luego se cancela). Por eso el pedido usa el estado `PENDING`.
- **Coreografía a gran escala**: con muchos servicios se vuelve una "telaraña invisible" difícil de seguir y depurar (lo advierte el video); ahí conviene la orquestación.
