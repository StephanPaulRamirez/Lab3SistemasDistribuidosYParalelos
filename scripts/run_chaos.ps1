# Script de caos en PowerShell
# Uso: powershell -ExecutionPolicy Bypass -File .\scripts\run_chaos.ps1

$ErrorActionPreference = 'Stop'

Write-Host "[run_chaos.ps1] Matando validator..." -ForegroundColor Cyan
try {
    docker compose kill validator
} catch {
    Write-Warning "No se pudo matar validator (puede no estar corriendo): $_"
}
Start-Sleep -Seconds 5

Write-Host "[run_chaos.ps1] Volviendo a levantar validator..." -ForegroundColor Cyan
docker compose up -d validator
Start-Sleep -Seconds 5

Write-Host "[run_chaos.ps1] Reiniciando aggregator..." -ForegroundColor Cyan
docker compose restart aggregator
Start-Sleep -Seconds 5

Write-Host "[run_chaos.ps1] Reiniciando broker Redis..." -ForegroundColor Cyan
docker compose restart redis

Write-Host "[run_chaos.ps1] Listo. Revisa logs con: docker compose logs -f validator aggregator audit" -ForegroundColor Green
