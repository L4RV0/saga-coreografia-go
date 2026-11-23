<#
Hace UNA compra por la API de Order y muestra cómo terminó la saga.

Uso (desde la carpeta saga-coreografia):
  .\scripts\comprar.ps1 "Jeremy Delgado" runner-pro 1
  .\scripts\comprar.ps1 "Angel Alvarez" studio-buds 1     # pago fallido -> compensación
  .\scripts\comprar.ps1 "Erick Cevallos" studio-buds 5    # sin stock -> cancelación

Productos: mystery-box ($29), runner-pro ($59), studio-buds ($99)
Clientes:  Jeremy Delgado ($500), Angel Alvarez ($20), Erick Cevallos ($1000)
#>
param(
    [Parameter(Mandatory = $true)][string]$Cliente,
    [Parameter(Mandatory = $true)][string]$Producto,
    [int]$Cantidad = 1
)
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OrderUrl = "http://localhost:8081"

$body = @{ customer_id = $Cliente; product_id = $Producto; quantity = $Cantidad } | ConvertTo-Json
try {
    $o = Invoke-RestMethod -Method Post -Uri "$OrderUrl/orders" -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($body))
}
catch {
    Write-Host "No se pudo crear el pedido: $($_.ErrorDetails.Message)" -ForegroundColor Red
    exit 1
}
Write-Host "POST /orders -> $($o.id) $($o.status) ($Cliente, $Cantidad x $Producto, `$$($o.amount))" -ForegroundColor Gray

for ($i = 0; $i -lt 100; $i++) {
    $o = Invoke-RestMethod -Uri "$OrderUrl/orders/$($o.id)"
    if ($o.status -ne "PENDING") { break }
    Start-Sleep -Milliseconds 150
}
$color = "Green"; if ($o.status -ne "CONFIRMED") { $color = "Red" }
Write-Host "Resultado: $($o.id) $($o.status) $($o.reason)" -ForegroundColor $color
Write-Host "`nPara ver la saga en las 3 bases:   .\scripts\consultar.ps1 -Pedido $($o.id)"
if ($o.status -eq "CONFIRMED") {
    Write-Host "Para cancelarla (compensaciones):  .\scripts\cancelar.ps1 $($o.id)"
}
