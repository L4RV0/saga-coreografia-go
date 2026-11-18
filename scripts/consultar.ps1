<#
Ejecuta consultas SQL dentro de la base de datos de cada servicio.
Cada base vive en su propio contenedor; este script entra con "docker compose exec"
y usa psql, el cliente oficial de PostgreSQL que ya viene en la imagen.

Ejemplos (desde la carpeta saga-coreografia):
  .\scripts\consultar.ps1 order                    # corre consultas\order.sql
  .\scripts\consultar.ps1 inventory                # corre consultas\inventory.sql
  .\scripts\consultar.ps1 payment                  # corre consultas\payment.sql
  .\scripts\consultar.ps1 order -Sql "SELECT id, status FROM orders"
  .\scripts\consultar.ps1 -Pedido ORD-002          # sigue un pedido por las 3 bases
  .\scripts\consultar.ps1 payment -Consola         # abre psql interactivo (salir con \q)
#>
param(
    [ValidateSet("order", "inventory", "payment")]
    [string]$Base,
    [string]$Sql,
    [string]$Pedido,
    [switch]$Consola
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Join-Path $PSScriptRoot "..")

# Contenedor, usuario y base de datos de cada servicio (ver docker-compose.yml).
$bases = @{
    order     = @{ Contenedor = "order-db";     Usuario = "order_svc";     Db = "orders" }
    inventory = @{ Contenedor = "inventory-db"; Usuario = "inventory_svc"; Db = "inventory" }
    payment   = @{ Contenedor = "payment-db";   Usuario = "payment_svc";   Db = "payments" }
}

function Invoke-Psql($nombre, [string[]]$argumentos) {
    $b = $bases[$nombre]
    docker compose exec -T -e PGCLIENTENCODING=UTF8 $b.Contenedor psql -U $b.Usuario -d $b.Db -P pager=off @argumentos
    if ($LASTEXITCODE -ne 0) { throw "La consulta en $($b.Contenedor) falló. ¿Están encendidos los contenedores? (docker compose up -d)" }
}

# Seguir un pedido (una saga) a través de las 3 bases de datos.
if ($Pedido) {
    $id = $Pedido.Replace("'", "''")
    Write-Host "`n=== order-db: el pedido ===" -ForegroundColor Cyan
    Invoke-Psql "order" @("-c", "SELECT id, customer_id, product_id, quantity, amount, status, reason FROM orders WHERE id = '$id';")
    Write-Host "=== inventory-db: la reserva de stock ===" -ForegroundColor Green
    Invoke-Psql "inventory" @("-c", "SELECT order_id, product_id, quantity, status FROM reservations WHERE order_id = '$id';")
    Write-Host "=== inventory-db: movimientos de inventario de este pedido ===" -ForegroundColor Green
    Invoke-Psql "inventory" @("-c", "SELECT id, movement_type AS tipo, product_id AS producto, quantity AS cantidad, stock_before AS antes, stock_after AS despues, event_type AS evento, detail AS detalle FROM inventory_movements WHERE order_id = '$id' ORDER BY id;")
    Write-Host "=== payment-db: el pago ===" -ForegroundColor Magenta
    Invoke-Psql "payment" @("-c", "SELECT order_id, customer_id, amount, status, reason FROM payments WHERE order_id = '$id';")
    Write-Host "=== Eventos de esta saga en cada outbox ===" -ForegroundColor Yellow
    foreach ($n in "order", "inventory", "payment") {
        Invoke-Psql $n @("-t", "-c", "SELECT '$n-service publicó ' || (event->>'type') || ' en ' || topic FROM outbox WHERE event->>'order_id' = '$id' ORDER BY id;")
    }
    exit 0
}

if (-not $Base) {
    Write-Host "Indica la base: order, inventory o payment. Ejemplo: .\scripts\consultar.ps1 order"
    exit 1
}

$b = $bases[$Base]
if ($Consola) {
    # Sin -T para que psql sea interactivo.
    docker compose exec $b.Contenedor psql -U $b.Usuario -d $b.Db
}
elseif ($Sql) {
    Invoke-Psql $Base @("-c", $Sql)
}
else {
    Invoke-Psql $Base @("-f", "/consultas/$Base.sql")
}
