# Script de carga tipo burst en PowerShell
# Uso: powershell -ExecutionPolicy Bypass -File .\scripts\run_burst.ps1

$ErrorActionPreference = 'Stop'

Write-Host "[run_burst.ps1] Reiniciando stack en modo burst..." -ForegroundColor Cyan

$env:PUBLISHER_MODE = "burst"
$env:PUBLISHER_EVENT_RATE = "20"
$env:PUBLISHER_BURST_FACTOR = "10"
$env:BACKPRESSURE_MAX_INFLIGHT = "5000"
$env:BACKPRESSURE_CHECK_EVERY = "20"
$env:BACKPRESSURE_PAUSE_SECONDS = "2.0"

docker compose down
docker compose up --build
