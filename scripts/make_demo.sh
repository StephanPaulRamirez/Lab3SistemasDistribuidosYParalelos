#!/usr/bin/env bash
set -euo pipefail

# Script "make demo": orquesta una demo completa en ~8 minutos.
# Pensado para ejecutarse desde la raíz del repo (donde está docker-compose.yml).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[make_demo] 1) Iniciando stack en modo carga normal..."
./scripts/run_load.sh &
LOAD_PID=$!

# Dar tiempo para que el stack levante y se generen eventos
sleep 90

echo "[make_demo] 2) Cambiando a modo burst para simular picos de carga..."
./scripts/run_burst.sh &
BURST_PID=$!

sleep 90

echo "[make_demo] 3) Ejecutando caos (reinicio de servicios y broker)..."
./scripts/run_chaos.sh

# Espera opcional para observar la recuperación
sleep 60

echo
echo "[make_demo] 4) BONUS: Demostrando detección de anomalías..."
echo "[make_demo] Inyectando anomalías en métricas para disparar alertas..."

# Inyecta anomalías en modo automático
python scripts/inject_anomalies.py demo

echo "[make_demo] ✨ Anomalías inyectadas. Las alertas deberían aparecer en 10 segundos..."
sleep 10

echo
echo "[make_demo] Demo completada. Abre el dashboard en: http://localhost:8000/"
echo "[make_demo] Verás:"
echo "  ✓ Métricas agregadas en tiempo real"
echo "  ✓ Comportamiento normal y bajo carga (modo burst)"
echo "  ✓ Recuperación ante fallos (caos)"
echo "  ✓ 🎁 Alertas de anomalías detectadas (BONUS)"
echo
echo "[make_demo] Para ver logs en detalle:"
echo "  docker compose logs -f anomaly-detector"
