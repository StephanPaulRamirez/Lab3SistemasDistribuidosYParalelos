#!/usr/bin/env bash
set -euo pipefail

# Script de replay: vuelve a procesar eventos desde un ID específico de Redis Stream.
# Uso: ./scripts/replay.sh [START_ID]
#  - START_ID por defecto es "0-0" (desde el inicio del stream validated.events).

cd "$(dirname "$0")/.."

START_ID=${1:-"0-0"}

echo "[replay] Ejecutando Aggregator en modo replay desde ID=${START_ID}..."

docker compose run --rm \
  -e AGGREGATOR_GROUP=aggregator-replay \
  -e AGGREGATOR_NAME=aggregator-replay-1 \
  -e AGGREGATOR_START_ID="${START_ID}" \
  -e DEDUP_SET_KEY="replay:processed:event_ids" \
  aggregator

cat <<EOF
[replay] Finalizado.
Las métricas reprocesadas se escriben nuevamente en el stream metrics.daily y luego en SQLite.
Puedes consultarlas vía API en http://localhost:8000/ o /metrics.
EOF
