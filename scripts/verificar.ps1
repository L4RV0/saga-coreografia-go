# Ejecuta localmente los mismos controles que el pipeline de CI y guarda la
# evidencia en la carpeta "evidencia".
#   Uso:  powershell -ExecutionPolicy Bypass -File scripts\verificar.ps1
$ErrorActionPreference = "Stop"
$env:NO_COLOR = "1"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force evidencia | Out-Null

Write-Host "1/5 Formato (gofmt)"
$sinFormato = gofmt -l .
if ($sinFormato) { Write-Host "Archivos sin formato: $sinFormato"; exit 1 }

Write-Host "2/5 Analisis estatico (go vet)"
go vet ./...
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "3/5 Pruebas + cobertura"
go test -count=1 -v "-coverpkg=./..." "-coverprofile=evidencia/coverage.out" ./... | Tee-Object evidencia/pruebas.txt
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "4/5 Reporte de cobertura"
go tool cover "-func=evidencia/coverage.out" | Tee-Object evidencia/cobertura.txt
go tool cover "-html=evidencia/coverage.out" -o evidencia/coverage.html

Write-Host "5/5 Demo de los 3 escenarios"
go run . | Tee-Object evidencia/ejecucion.txt

Write-Host "Listo. Evidencia en la carpeta 'evidencia'."
