# Ejecuta localmente los mismos controles que el pipeline de CI y guarda la
# evidencia en la carpeta "evidencia". Requiere Docker Desktop encendido.
#   Uso:  powershell -ExecutionPolicy Bypass -File scripts\verificar.ps1
$ErrorActionPreference = "Stop"
$env:NO_COLOR = "1"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force evidencia | Out-Null

Write-Host "1/6 Formato (gofmt)"
$sinFormato = gofmt -l .
if ($sinFormato) { Write-Host "Archivos sin formato: $sinFormato"; exit 1 }

Write-Host "2/6 Analisis estatico (go vet)"
go vet ./...
go vet -tags e2e ./...
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "3/6 Levantar los 7 contenedores (3 servicios + 3 Postgres + NATS)"
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { exit 1 }
docker compose ps | Tee-Object evidencia/contenedores.txt

Write-Host "4/6 Pruebas de extremo a extremo"
go test -tags e2e -count=1 -v ./e2e | Tee-Object evidencia/pruebas-e2e.txt
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "5/6 Demo de los 3 escenarios"
go run . | Tee-Object evidencia/demo.txt

Write-Host "6/6 Volumenes"
docker volume ls --filter name=saga-coreografia | Tee-Object evidencia/volumenes.txt

Write-Host "Listo. Evidencia en la carpeta 'evidencia'. Para apagar: docker compose down"
