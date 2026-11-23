# Patrón SAGA basado en Coreografía — Go + Docker

Implementación del caso **"Black Friday"** del video *How Saga Pattern Solves Distributed Transactions in Microservices* (Visualcoders).

Tres microservicios independientes (**Order**, **Inventory**, **Payment**), cada uno con **su propia base de datos Postgres en su propio contenedor, volumen y red**, que se coordinan **solo mediante eventos** en un broker **NATS JetStream**. No hay orquestador central.

## 1. Cómo ejecutarlo

Requisitos: Docker Desktop encendido y Go 1.26 o superior. Todos los comandos se ejecutan **dentro de la carpeta `saga-coreografia`** (donde está `go.mod`).

Levantar los 7 contenedores (3 servicios + 3 Postgres + NATS):

```bash
docker compose up -d --build --wait
```

Ejecutar la demo con los 3 escenarios:

```bash
go run .
```

Ver cómo viajan los eventos entre servicios:

```bash
docker compose logs order-service inventory-service payment-service
```

Pruebas de extremo a extremo contra los contenedores:

```bash
go test -tags e2e -v ./e2e
```

Apagar (los datos se conservan en los volúmenes):

```bash
docker compose down
```

Apagar y borrar también los datos:

```bash
docker compose down -v
```

> Si `go run .` dice `go.mod file not found`, estás en la carpeta equivocada: entra primero a `saga-coreografia`.

### Demostración guiada (escenarios con cambios en las bases de datos)

Borrar todos los datos y empezar desde cero (bases vacías con los datos iniciales):

```bash
.\scripts\reiniciar.ps1
```

Ejecutar la demostración, con pausa entre escenarios (`-Reiniciar` borra todo antes, `-SinPausa` corre todo seguido):

```bash
.\scripts\escenarios.ps1 -Reiniciar
```

| Escenario | Qué pasa | Qué se ve en las bases |
|---|---|---|
| 1. Compra exitosa (Jeremy Delgado) | CONFIRMED | `orders` CONFIRMED, kardex `RESERVA -1`, `payments` PAID, saldo −$59 |
| 2. Pago fallido (Angel Alvarez) | Compensación de la saga | `orders` CANCELLED, reserva `RELEASED`, kardex `RESERVA -1` y `LIBERACION +1`, `payments` FAILED |
| 3. Sin stock (Erick Cevallos) | Cancelación | `orders` CANCELLED, kardex `RECHAZO 0`, sin registro en `payments` |
| 4. Black Friday: 5 pedidos para 3 unidades | 3 CONFIRMED, 2 CANCELLED | Stock 3 → 0 sin quedar negativo |
| 5. ROLLBACK local en cada base | `consultas/rollback-*.sql` | Un cambio dentro de `BEGIN … ROLLBACK` desaparece; un error (CHECK) anula toda la transacción |

Los archivos `consultas/rollback-order.sql`, `rollback-inventory.sql` y `rollback-payment.sql` también se pueden abrir y ejecutar en VS Code con la extensión de PostgreSQL.

### Consultas a las bases de datos

Cada base vive en su contenedor; `scripts/consultar.ps1` entra con `docker compose exec` y ejecuta `psql` (el cliente de PostgreSQL que trae la imagen). Las consultas están en `consultas/*.sql`.

```bash
.\scripts\consultar.ps1 order
```

```bash
.\scripts\consultar.ps1 inventory
```

```bash
.\scripts\consultar.ps1 payment
```

Seguir un pedido (una saga completa) por las 3 bases:

```bash
.\scripts\consultar.ps1 -Pedido ORD-002
```

Una consulta propia, o la consola interactiva de psql (salir con `\q`):

```bash
.\scripts\consultar.ps1 order -Sql "SELECT id, status FROM orders"
```

```bash
.\scripts\consultar.ps1 payment -Consola
```

Herramientas gráficas (DBeaver, pgAdmin, extensión PostgreSQL de VS Code): levantar con el archivo opcional que abre cada base solo en tu computador:

```bash
docker compose -f docker-compose.yml -f docker-compose.dbtools.yml up -d
```

| Base | Host | Puerto | Base de datos | Usuario | Contraseña |
|---|---|---|---|---|---|
| Order | 127.0.0.1 | 5441 | orders | order_svc | order_pass |
| Inventory | 127.0.0.1 | 5442 | inventory | inventory_svc | inventory_pass |
| Payment | 127.0.0.1 | 5443 | payments | payment_svc | payment_pass |

Los productos y clientes con prefijo `prod-` y `cliente-` los crean las pruebas automáticas.

## 2. Arquitectura

| Contenedor | Imagen | Puerto | Volumen | Redes |
|---|---|---|---|---|
| `order-service` | Go (distroless) | 8081 | — | order-net, broker-net |
| `order-db` | postgres:16 | interno | `order-data` | order-net |
| `inventory-service` | Go (distroless) | 8082 | — | inventory-net, broker-net |
| `inventory-db` | postgres:16 | interno | `inventory-data` | inventory-net |
| `payment-service` | Go (distroless) | 8083 | — | payment-net, broker-net |
| `payment-db` | postgres:16 | interno | `payment-data` | payment-net |
| `nats` | nats:2.10 (JetStream) | 4222, 8222 | `nats-data` | broker-net |

- **Un volumen por base de datos**: los datos sobreviven a reinicios y a `docker compose down`; cada base se respalda, escala y restaura por separado.
- **Una red por servicio**: `order-service` no puede ni siquiera resolver `inventory-db`. La única vía entre servicios es el broker.
- **Sin cuello de botella compartido**: si hubiera una sola base de datos para todos, todas las escrituras competirían por ella y su caída tumbaría todo el sistema. Con una por servicio, una carga alta de pagos no frena a pedidos, y la caída de `payment-db` no afecta a `order-db`.

## 3. Flujo de la saga

**Camino feliz**
1. `POST /orders` → Order guarda el pedido `PENDING` y publica `OrderCreated` en `saga.order`
2. Inventory reserva el stock en su base de datos
3. Inventory publica `InventoryReserved` en `saga.inventory`
4. Payment cobra en su base de datos
5. Payment publica `PaymentProcessed` en `saga.payment`
6. Order marca el pedido `CONFIRMED`

**Falla el pago (caso Black Friday) → compensación**
1. Payment publica `PaymentFailed`
2. Inventory devuelve el stock (transacción compensatoria) y publica `StockReleased`
3. Order cancela el pedido (`CANCELLED`)

**Sin stock**: Inventory publica `InventoryRejected` y Order cancela; Payment nunca interviene.

| Servicio | Publica en | Escucha |
|---|---|---|
| Order | `saga.order` | `saga.payment` (`PaymentProcessed`), `saga.inventory` (`StockReleased`, `InventoryRejected`) |
| Inventory | `saga.inventory` | `saga.order` (`OrderCreated`), `saga.payment` (`PaymentFailed`) |
| Payment | `saga.payment` | `saga.inventory` (`InventoryReserved`) |

## 4. Garantías implementadas

- **Transacciones locales**: cada paso hace commit solo en su Postgres.
- **Transactional Outbox**: el cambio de negocio y el evento se guardan en la misma transacción (tabla `outbox`); un relay los publica después en NATS. Nunca queda un cambio sin su evento.
- **Idempotencia**: tabla `processed_events` por servicio. Si NATS entrega un evento dos veces, el segundo se ignora.
- **Entrega al menos una vez**: consumidores durables de JetStream; si un servicio está caído, sus eventos lo esperan en el volumen `nats-data`.
- **Concurrencia segura**: `UPDATE … WHERE stock >= cantidad` y `WHERE balance >= monto` son atómicos en Postgres; nunca se vende ni se cobra de más.

### Movimientos de inventario (kardex)

La tabla `inventory_movements` de la Inventory DB registra **cada transacción que toca el stock**, en la misma transacción que el cambio (si el cambio se guarda, su movimiento también):

| Tipo | Cuándo | Cantidad |
|---|---|---|
| `CARGA_INICIAL` | Alta de un producto | + stock inicial |
| `AJUSTE` | Cambio manual por la API de administración | + o − |
| `RESERVA` | T2: se aparta stock para un pedido (`OrderCreated`) | − unidades |
| `LIBERACION` | C2: compensación, el pago falló (`PaymentFailed`) | + unidades |
| `RECHAZO` | Pedido sin stock suficiente | 0 (no cambia el stock) |

Cada fila guarda el pedido, el cliente, la cantidad, el stock antes y después, el evento de NATS que la provocó y un detalle en texto. La suma de los movimientos de un producto siempre es igual a su stock actual.

```bash
.\scripts\consultar.ps1 inventory -Sql "SELECT * FROM inventory_movements ORDER BY id DESC LIMIT 20"
```

También por API: `GET http://localhost:8082/movements?product=studio-buds` o `?order=ORD-002`.

## 5. Estructura

```
saga-coreografia/
├── main.go                 # demo: cliente HTTP que ejecuta los 3 escenarios
├── cmd/
│   ├── order/main.go       # microservicio Order (API HTTP)
│   ├── inventory/main.go   # microservicio Inventory
│   └── payment/main.go     # microservicio Payment
├── internal/
│   ├── events/             # contrato: tópicos y eventos
│   ├── broker/             # NATS JetStream (interfaz Broker)
│   ├── store/              # Postgres, outbox e idempotencia
│   ├── platform/           # arranque común de un servicio
│   ├── order/ inventory/ payment/   # lógica de cada servicio
│   ├── client/             # cliente HTTP (demo y pruebas)
│   └── logx/               # logs por servicio
├── e2e/saga_e2e_test.go    # 5 pruebas de extremo a extremo
├── Dockerfile              # una receta, 3 imágenes (ARG SERVICE)
├── docker-compose.yml      # 7 contenedores, 4 volúmenes, 4 redes
├── .env.example            # credenciales de las bases de datos
├── .github/workflows/ci.yml
├── scripts/verificar.ps1   # pipeline local + evidencia
└── evidencia/              # salidas reales de las pruebas y Docker
```

## 6. APIs

| Servicio | Método y ruta | Uso |
|---|---|---|
| Order `:8081` | `POST /orders` | Crear pedido `{"customer_id","product_id","quantity"}` → 202 PENDING |
| | `GET /orders/{id}`, `GET /orders` | Consultar pedidos |
| Inventory `:8082` | `GET /stock` | Stock por producto |
| Payment `:8083` | `GET /accounts`, `GET /payments/{order}` | Saldos y pagos |
| Todos | `GET /health` | Salud (lo usa Docker) |
| Todos | `PUT /admin/...` | Cargar datos de prueba |

## 7. Pruebas

`go test -tags e2e -v ./e2e` corre contra los contenedores reales:

| Prueba | Verifica |
|---|---|
| `TestCaminoFeliz` | CONFIRMED, stock 10 → 9, saldo 100 → 41 |
| `TestPagoFallidoCompensaInventario` | CANCELLED y el stock vuelve a 3 |
| `TestSinStockCancelaSinCobrar` | CANCELLED sin registro de pago |
| `TestEventoDuplicadoEsIdempotente` | Un evento entregado 2 veces se aplica 1 vez |
| `TestPedidosConcurrentes` | 20 pedidos simultáneos con 5 unidades → 5 CONFIRMED, 15 CANCELLED |

## 8. Limitaciones

- La coreografía con muchos servicios se vuelve difícil de seguir; con más pasos convendría orquestación.
- Mientras la saga corre, otros pueden ver datos intermedios (stock reservado de un pedido que luego se cancela). Por eso existe el estado `PENDING`.
- Las credenciales de `.env.example` son de desarrollo.
