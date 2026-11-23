<#
Cancela una compra CONFIRMED y dispara las TRANSACCIONES COMPENSATORIAS en
orden inverso:  C3 Payment reembolsa -> C2 Inventory devuelve el stock -> C1 Order cancela.

Uso (desde la carpeta saga-coreografia):
  .\scripts\cancelar.ps1 ORD-009
  .\scripts\cancelar.ps1 ORD-009 "el cliente se arrepintió"
#>
param(
    [Parameter(Mandatory = $true)][string]$Pedido,
    [string]$Motivo = "cancelado por el cliente"
)
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OrderUrl = "http://localhost:8081"

$body = @{ reason = $Motivo } | ConvertTo-Json
try {
    $o = Invoke-RestMethod -Method Post -Uri "$OrderUrl/orders/$Pedido/cancel" -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($body))
}
catch {
    Write-Host "No se pudo cancelar: $($_.ErrorDetails.Message)" -ForegroundColor Red
    exit 1
}
Write-Host "POST /orders/$Pedido/cancel -> $($o.status) (se inician las compensaciones)" -ForegroundColor Yellow

for ($i = 0; $i -lt 100; $i++) {
    $o = Invoke-RestMethod -Uri "$OrderUrl/orders/$Pedido"
    if ($o.status -eq "CANCELLED") { break }
    Start-Sleep -Milliseconds 150
}
Write-Host "Resultado: $($o.id) $($o.status) - $($o.reason)" -ForegroundColor Yellow
Write-Host "`nPara ver las compensaciones de este pedido:  .\scripts\consultar.ps1 -Pedido $Pedido"
Write-Host "Para ver todas las compensaciones:           .\scripts\consultar.ps1 -Compensaciones"
