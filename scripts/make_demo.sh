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
echo "[make_demo] Demo completada. Ahora puedes abrir el dashboard en: http://localhost:8000/"
echo "[make_demo] También puedes consultar métricas desde otra terminal, por ejemplo:"
echo "  curl \"http://localhost:8000/metrics?date=2025-12-29\""
