param(
    [int]$NormalSeconds = 90,
    [int]$BurstSeconds = 90,
    [int]$RecoverySeconds = 60
)

# Script make_demo para PowerShell (Windows)
# Uso (desde la raíz del repo):
#   powershell -ExecutionPolicy Bypass -File .\scripts\make_demo.ps1

$ErrorActionPreference = 'Stop'

Write-Host "[make_demo.ps1] 1) Reiniciando stack en modo carga normal..." -ForegroundColor Cyan
$env:PUBLISHER_MODE = "normal"
$env:PUBLISHER_EVENT_RATE = "5"

docker compose down
# Modo normal
docker compose up --build -d

Write-Host "[make_demo.ps1] Esperando $NormalSeconds segundos para generar eventos normales..." -ForegroundColor Yellow
Start-Sleep -Seconds $NormalSeconds

Write-Host "[make_demo.ps1] 2) Cambiando a modo burst (picos de carga)..." -ForegroundColor Cyan
$env:PUBLISHER_MODE = "burst"
$env:PUBLISHER_EVENT_RATE = "20"
$env:PUBLISHER_BURST_FACTOR = "10"
$env:BACKPRESSURE_MAX_INFLIGHT = "5000"
$env:BACKPRESSURE_CHECK_EVERY = "20"
$env:BACKPRESSURE_PAUSE_SECONDS = "2.0"

# Reiniciamos para aplicar las nuevas variables
docker compose down
docker compose up --build -d

Write-Host "[make_demo.ps1] Esperando $BurstSeconds segundos en modo burst..." -ForegroundColor Yellow
Start-Sleep -Seconds $BurstSeconds

Write-Host "[make_demo.ps1] 3) Ejecutando caos (reinicio selectivo de servicios)..." -ForegroundColor Cyan

try {
    Write-Host "[make_demo.ps1] Matando validator..."
    docker compose kill validator
} catch {
    Write-Warning "No se pudo matar validator (puede no estar corriendo): $_"
}
Start-Sleep -Seconds 5

Write-Host "[make_demo.ps1] Levantando validator en segundo plano..."
docker compose up -d validator
Start-Sleep -Seconds 5

Write-Host "[make_demo.ps1] Reiniciando aggregator..."
docker compose restart aggregator
Start-Sleep -Seconds 5

Write-Host "[make_demo.ps1] Reiniciando broker Redis..."
docker compose restart redis

Write-Host "[make_demo.ps1] Esperando $RecoverySeconds segundos para observar la recuperación..." -ForegroundColor Yellow
Start-Sleep -Seconds $RecoverySeconds

Write-Host ""
Write-Host "[make_demo.ps1] 4) BONUS: Demostrando detección de anomalías..." -ForegroundColor Cyan
Write-Host "[make_demo.ps1] Inyectando anomalías en métricas para disparar alertas..." -ForegroundColor Yellow

# Inyecta anomalías en modo automático
python scripts/inject_anomalies.py demo

Write-Host "[make_demo.ps1] ✨ Anomalías inyectadas. Las alertas deberían aparecer en 10 segundos..." -ForegroundColor Green
Start-Sleep -Seconds 10

Write-Host ""
Write-Host "[make_demo.ps1] Demo completada." -ForegroundColor Green
Write-Host "[make_demo.ps1] Abre el dashboard en: http://localhost:8000/" -ForegroundColor Cyan
Write-Host "[make_demo.ps1] Verás:" -ForegroundColor Cyan
Write-Host "  ✓ Métricas agregadas en tiempo real" -ForegroundColor Green
Write-Host "  ✓ Comportamiento normal y bajo carga (modo burst)" -ForegroundColor Green
Write-Host "  ✓ Recuperación ante fallos (caos)" -ForegroundColor Green
Write-Host "  ✓ 🎁 Alertas de anomalías detectadas (BONUS)" -ForegroundColor Magenta
Write-Host ""
Write-Host "[make_demo.ps1] Para ver logs en detalle:" -ForegroundColor Yellow
Write-Host "  docker compose logs -f anomaly-detector" -ForegroundColor White