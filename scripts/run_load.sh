#!/usr/bin/env bash
set -euo pipefail

# Script de carga normal: levanta todo el sistema con el publisher en modo normal.
# Uso: ./scripts/run_load.sh

cd "$(dirname "$0")/.."

# Valores por defecto pensados para la demo
export PUBLISHER_MODE="normal"
export PUBLISHER_EVENT_RATE="5"

echo "[run_load] Reiniciando stack en modo normal..."
docker compose down
docker compose up --build
