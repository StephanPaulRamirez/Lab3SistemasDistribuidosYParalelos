# Script de carga normal en PowerShell
# Uso: powershell -ExecutionPolicy Bypass -File .\scripts\run_load.ps1

$ErrorActionPreference = 'Stop'

Write-Host "[run_load.ps1] Reiniciando stack en modo normal..." -ForegroundColor Cyan

$env:PUBLISHER_MODE = "normal"
$env:PUBLISHER_EVENT_RATE = "5"

# Asumimos que se ejecuta desde la raíz del repo
# Si no, ajusta el directorio antes de llamar este script.

docker compose down
docker compose up --build
