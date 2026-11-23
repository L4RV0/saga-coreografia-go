<#
Demostración guiada de la Saga por coreografía. Hace compras REALES por la API
de Order y, después de cada una, muestra qué cambió en CADA base de datos.

  Escenario 1  Compra exitosa ......................... Jeremy Delgado
  Escenario 2  Pago fallido -> compensación (rollback   Angel Alvarez
               distribuido de la saga)
  Escenario 3  Cancelación por falta de stock ......... Erick Cevallos
  Escenario 4  Black Friday: 5 pedidos para 3 unidades  Erick Cevallos
  Escenario 5  Cancelación de una compra confirmada:    Jeremy Delgado
               compensaciones C3 reembolso -> C2 stock -> C1 pedido
  Escenario 6  ROLLBACK local dentro de cada base de datos
  Resumen      Todas las compensaciones, kardex y cuadres

Uso (desde la carpeta saga-coreografia):
  .\scripts\escenarios.ps1               # pausa entre escenarios (para presentar)
  .\scripts\escenarios.ps1 -Reiniciar    # borra todo antes de empezar
  .\scripts\escenarios.ps1 -SinPausa     # corre todo seguido
#>
param(
    [switch]$Reiniciar,
    [switch]$SinPausa
)

$ErrorActionPreference = "Continue"  # Docker y psql escriben progreso en stderr; se valida $LASTEXITCODE
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Join-Path $PSScriptRoot "..")

$OrderUrl = "http://localhost:8081"
$consultar = Join-Path $PSScriptRoot "consultar.ps1"

function Titulo($texto) {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor Cyan
    Write-Host "  $texto" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor Cyan
}

function Paso($texto) { Write-Host "`n>> $texto" -ForegroundColor Yellow }

function Pausa {
    if (-not $SinPausa) { [void](Read-Host "`nPresiona Enter para continuar") }
}

function Consulta($base, $sql) { & $consultar $base -Sql $sql }

function Nuevo-Pedido($cliente, $producto, $cantidad) {
    $body = @{ customer_id = $cliente; product_id = $producto; quantity = $cantidad } | ConvertTo-Json
    $o = Invoke-RestMethod -Method Post -Uri "$OrderUrl/orders" -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($body))
    Write-Host ("   POST /orders -> {0} {1}  ({2} compra {3} x {4}, `${5})" -f $o.id, $o.status, $cliente, $cantidad, $producto, $o.amount) -ForegroundColor Gray
    return $o.id
}

function Esperar-Final($id) {
    for ($i = 0; $i -lt 100; $i++) {
        $o = Invoke-RestMethod -Uri "$OrderUrl/orders/$id"
        if ($o.status -ne "PENDING") { return $o }
        Start-Sleep -Milliseconds 150
    }
    throw "El pedido $id sigue en PENDING"
}

function Estado($producto, $cliente) {
    Paso "inventory-db: stock de $producto"
    Consulta inventory "SELECT product_id AS producto, stock FROM products WHERE product_id = '$producto';"
    Paso "payment-db: saldo de $cliente"
    Consulta payment "SELECT customer_id AS cliente, balance AS saldo FROM accounts WHERE customer_id = '$cliente';"
}

function Saga($id) {
    Paso "order-db: el pedido y su historial de estados"
    Consulta order "SELECT id, customer_id AS cliente, product_id AS producto, quantity AS cant, amount AS total, status AS estado, reason AS motivo FROM orders WHERE id = '$id';"
    Consulta order "SELECT id, from_status AS de, to_status AS a, kind AS tipo, event_type AS evento, detail AS detalle FROM order_status_history WHERE order_id = '$id' ORDER BY id;"
    Paso "inventory-db: reserva y movimientos de inventario de este pedido"
    Consulta inventory "SELECT order_id AS pedido, product_id AS producto, quantity AS cant, status AS estado FROM reservations WHERE order_id = '$id';"
    Consulta inventory "SELECT id, movement_type AS tipo, quantity AS cantidad, stock_before AS antes, stock_after AS despues, event_type AS evento, detail AS detalle FROM inventory_movements WHERE order_id = '$id' ORDER BY id;"
    Paso "payment-db: el pago y sus movimientos de dinero"
    Consulta payment "SELECT order_id AS pedido, customer_id AS cliente, amount AS monto, status AS estado, reason AS motivo FROM payments WHERE order_id = '$id';"
    Consulta payment "SELECT id, movement_type AS tipo, kind AS clase, amount AS monto, balance_before AS saldo_antes, balance_after AS saldo_despues, event_type AS evento, detail AS detalle FROM account_movements WHERE order_id = '$id' ORDER BY id;"
}

# ---------------------------------------------------------------------------
if ($Reiniciar) { & (Join-Path $PSScriptRoot "reiniciar.ps1") }

try { Invoke-RestMethod "$OrderUrl/health" | Out-Null }
catch { Write-Host "Los servicios no responden. Ejecuta primero: .\scripts\reiniciar.ps1" -ForegroundColor Red; exit 1 }

Titulo "ESTADO INICIAL de las tres bases de datos"
Paso "order-db: catálogo"
Consulta order "SELECT product_id AS producto, price AS precio FROM catalog ORDER BY product_id;"
Paso "inventory-db: stock"
Consulta inventory "SELECT product_id AS producto, stock FROM products ORDER BY product_id;"
Paso "payment-db: saldos"
Consulta payment "SELECT customer_id AS cliente, balance AS saldo FROM accounts ORDER BY customer_id;"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 1 - Compra exitosa: Jeremy Delgado compra 1 Runner Pro"
Write-Host "Esperado: CONFIRMED, el stock baja 1 y el saldo baja `$59."
Estado "runner-pro" "Jeremy Delgado"
Paso "Haciendo la compra..."
$id1 = Nuevo-Pedido "Jeremy Delgado" "runner-pro" 1
$o = Esperar-Final $id1
Write-Host "   Resultado: $($o.id) $($o.status)" -ForegroundColor Green
Saga $id1
Estado "runner-pro" "Jeremy Delgado"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 2 - Pago fallido y COMPENSACIÓN: Angel Alvarez compra 1 Studio Buds"
Write-Host "Angel tiene `$20 y el producto cuesta `$99. Inventory alcanza a reservar la"
Write-Host "unidad, el pago falla y la saga la DEVUELVE (no hay un ROLLBACK que cruce"
Write-Host "las tres bases: se compensa con una transacción nueva)."
Estado "studio-buds" "Angel Alvarez"
Paso "Haciendo la compra..."
$id2 = Nuevo-Pedido "Angel Alvarez" "studio-buds" 1
$o = Esperar-Final $id2
Write-Host "   Resultado: $($o.id) $($o.status) - $($o.reason)" -ForegroundColor Red
Saga $id2
Write-Host "`nObserva en el kardex: RESERVA -1 y luego LIBERACION +1 (la compensación)." -ForegroundColor Yellow
Estado "studio-buds" "Angel Alvarez"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 3 - Cancelación por falta de stock: Erick Cevallos compra 5 Studio Buds"
Write-Host "Solo hay 3 unidades. Inventory rechaza, Order cancela y Payment nunca cobra."
Estado "studio-buds" "Erick Cevallos"
Paso "Haciendo la compra..."
$id3 = Nuevo-Pedido "Erick Cevallos" "studio-buds" 5
$o = Esperar-Final $id3
Write-Host "   Resultado: $($o.id) $($o.status) - $($o.reason)" -ForegroundColor Red
Saga $id3
Estado "studio-buds" "Erick Cevallos"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 4 - Black Friday: 5 pedidos seguidos para 3 unidades de Studio Buds"
Write-Host "Esperado: 3 CONFIRMED y 2 CANCELLED. Nunca se vende más de lo que hay."
Paso "Haciendo 5 compras seguidas (no se espera a que termine una para hacer la otra)..."
$ids = @()
for ($i = 1; $i -le 5; $i++) { $ids += Nuevo-Pedido "Erick Cevallos" "studio-buds" 1 }
foreach ($id in $ids) {
    $o = Esperar-Final $id
    $color = "Green"; if ($o.status -eq "CANCELLED") { $color = "Red" }
    Write-Host "   $($o.id) $($o.status) $($o.reason)" -ForegroundColor $color
}
$lista = ($ids | ForEach-Object { "'$_'" }) -join ","
Paso "order-db: los 5 pedidos"
Consulta order "SELECT id, status AS estado, reason AS motivo FROM orders WHERE id IN ($lista) ORDER BY id;"
Paso "inventory-db: movimientos de esos 5 pedidos"
Consulta inventory "SELECT id, order_id AS pedido, movement_type AS tipo, quantity AS cantidad, stock_before AS antes, stock_after AS despues FROM inventory_movements WHERE order_id IN ($lista) ORDER BY id;"
Paso "payment-db: pagos de esos 5 pedidos"
Consulta payment "SELECT order_id AS pedido, amount AS monto, status AS estado FROM payments WHERE order_id IN ($lista) ORDER BY order_id;"
Estado "studio-buds" "Erick Cevallos"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 5 - TRANSACCIONES COMPENSATORIAS: Jeremy Delgado cancela su compra $id1"
Write-Host "La compra ya está CONFIRMED: se cobró, se descontó el stock y se confirmó el"
Write-Host "pedido, cada cosa en una base distinta. No existe un ROLLBACK que deshaga las"
Write-Host "tres, así que la saga ejecuta una transacción COMPENSATORIA por cada paso, en"
Write-Host "orden inverso:  C3 Payment reembolsa  ->  C2 Inventory devuelve el stock  ->  C1 Order cancela"
Estado "runner-pro" "Jeremy Delgado"
Paso "POST /orders/$id1/cancel"
$body = @{ reason = "el cliente se arrepintió de la compra" } | ConvertTo-Json
$c = Invoke-RestMethod -Method Post -Uri "$OrderUrl/orders/$id1/cancel" -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($body))
Write-Host "   La API responde: $($c.id) $($c.status) (las compensaciones siguen solas)" -ForegroundColor Gray
for ($i = 0; $i -lt 100; $i++) {
    $o = Invoke-RestMethod -Uri "$OrderUrl/orders/$id1"
    if ($o.status -eq "CANCELLED") { break }
    Start-Sleep -Milliseconds 150
}
Write-Host "   Resultado: $($o.id) $($o.status) - $($o.reason)" -ForegroundColor Yellow
Saga $id1
Write-Host "`nCada base registra su compensación:" -ForegroundColor Yellow
Write-Host "  payment-db   CARGO -59  y luego REEMBOLSO +59 (C3), pago REFUNDED" -ForegroundColor Yellow
Write-Host "  inventory-db RESERVA -1 y luego LIBERACION +1 (C2), reserva RELEASED" -ForegroundColor Yellow
Write-Host "  order-db     CONFIRMED -> CANCELLING -> CANCELLED (C1)" -ForegroundColor Yellow
Estado "runner-pro" "Jeremy Delgado"
Pausa

# ---------------------------------------------------------------------------
Titulo "ESCENARIO 6 - ROLLBACK local dentro de cada base de datos"
Write-Host "Dentro de UNA base, ROLLBACK deshace todo (ACID). Por eso cada paso de la saga"
Write-Host "es una transacción local. Lo que NO existe es un ROLLBACK que abarque las 3."
foreach ($base in "order", "inventory", "payment") {
    Paso "Rollback en $base-db (consultas/rollback-$base.sql)"
    & $consultar $base -Sql "\i /consultas/rollback-$base.sql"
}
Pausa

# ---------------------------------------------------------------------------
Titulo "RESUMEN FINAL"
Paso "Todas las transacciones compensatorias ejecutadas (.\scripts\consultar.ps1 -Compensaciones)"
& $consultar -Compensaciones
Paso "order-db: pedidos por estado"
Consulta order "SELECT status AS estado, COUNT(*) AS pedidos, SUM(amount) AS monto FROM orders GROUP BY status ORDER BY status;"
Paso "inventory-db: kardex completo de studio-buds"
Consulta inventory "SELECT id, movement_type AS tipo, order_id AS pedido, customer_id AS cliente, quantity AS cantidad, stock_before AS antes, stock_after AS despues FROM inventory_movements WHERE product_id = 'studio-buds' ORDER BY id;"
Paso "inventory-db: cuadre (stock actual = suma de movimientos)"
Consulta inventory "SELECT p.product_id AS producto, p.stock AS stock_actual, SUM(m.quantity) AS suma_movimientos, p.stock = SUM(m.quantity) AS cuadra FROM products p JOIN inventory_movements m USING (product_id) GROUP BY p.product_id, p.stock ORDER BY 1;"
Paso "payment-db: pagos por estado y saldos finales"
Consulta payment "SELECT status AS estado, COUNT(*) AS pagos, SUM(amount) AS monto FROM payments GROUP BY status ORDER BY status;"
Consulta payment "SELECT customer_id AS cliente, balance AS saldo FROM accounts ORDER BY customer_id;"
Paso "payment-db: cuadre (saldo actual = suma de movimientos de dinero)"
Consulta payment "SELECT a.customer_id AS cliente, a.balance AS saldo, SUM(m.amount) AS suma_movimientos, a.balance = SUM(m.amount) AS cuadra FROM accounts a JOIN account_movements m USING (customer_id) GROUP BY a.customer_id, a.balance ORDER BY 1;"
Write-Host "`nFin de la demostración." -ForegroundColor Green
