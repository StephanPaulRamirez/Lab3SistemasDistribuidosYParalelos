param(
    [string]$StartId = "0-0"
)

# Script de replay en PowerShell
# Uso: powershell -ExecutionPolicy Bypass -File .\scripts\replay.ps1 [-StartId "0-0"]

$ErrorActionPreference = 'Stop'

Write-Host "[replay.ps1] Ejecutando Aggregator en modo replay desde ID=$StartId..." -ForegroundColor Cyan

$env:AGGREGATOR_GROUP = "aggregator-replay"
$env:AGGREGATOR_NAME = "aggregator-replay-1"
$env:AGGREGATOR_START_ID = $StartId
$env:DEDUP_SET_KEY = "replay:processed:event_ids"

# Asumimos ejecución desde la raíz del repo
docker compose run --rm aggregator

Write-Host "[replay.ps1] Finalizado. Las métricas reprocesadas se escriben nuevamente en metrics.daily y SQLite." -ForegroundColor Green
