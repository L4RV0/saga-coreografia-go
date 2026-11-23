<#
Borra TODOS los datos (las 3 bases de datos y los mensajes de NATS) y levanta
el sistema desde cero, con los datos iniciales del caso:
  - Productos: mystery-box ($29, 100 u.), runner-pro ($59, 50 u.), studio-buds ($99, 3 u.)
  - Clientes:  Jeremy Delgado $500, Angel Alvarez $20, Erick Cevallos $1000

Uso (desde la carpeta saga-coreografia):
  .\scripts\reiniciar.ps1
#>
$ErrorActionPreference = "Continue"  # Docker y psql escriben progreso en stderr; se valida $LASTEXITCODE
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Set-Location (Join-Path $PSScriptRoot "..")

$compose = @("-f", "docker-compose.yml", "-f", "docker-compose.dbtools.yml")

Write-Host "1/2 Apagando contenedores y BORRANDO los volúmenes (order-data, inventory-data, payment-data, nats-data)..." -ForegroundColor Yellow
docker compose @compose down -v
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "2/2 Levantando los 7 contenedores con bases de datos vacías..." -ForegroundColor Yellow
docker compose @compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "`nListo: bases de datos limpias con los datos iniciales." -ForegroundColor Green
Write-Host "Si tenías VS Code conectado, actualiza las conexiones (botón de refrescar)."
