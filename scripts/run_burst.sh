#!/usr/bin/env bash
set -euo pipefail

# Script de carga tipo burst: levanta el sistema con el publisher en modo burst.
# Uso: ./scripts/run_burst.sh

cd "$(dirname "$0")/.."

export PUBLISHER_MODE="burst"
export PUBLISHER_EVENT_RATE="20"
export PUBLISHER_BURST_FACTOR="10"

echo "[run_burst] Reiniciando stack en modo burst..."
docker compose down
BACKPRESSURE_MAX_INFLIGHT=${BACKPRESSURE_MAX_INFLIGHT:-5000} \
BACKPRESSURE_CHECK_EVERY=${BACKPRESSURE_CHECK_EVERY:-20} \
BACKPRESSURE_PAUSE_SECONDS=${BACKPRESSURE_PAUSE_SECONDS:-2.0} \
  docker compose up --build
