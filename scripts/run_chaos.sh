#!/usr/bin/env bash
set -euo pipefail

# Script de caos: mata/reinicia consumidores y el broker para demostrar tolerancia a fallas.
# Uso: ./scripts/run_chaos.sh (con el stack ya levantado en otra terminal)

cd "$(dirname "$0")/.."

echo "[run_chaos] Matando validator..."
docker compose kill validator || true
sleep 5

echo "[run_chaos] Volviendo a levantar validator..."
docker compose up -d validator
sleep 5

echo "[run_chaos] Reiniciando aggregator..."
docker compose restart aggregator
sleep 5

echo "[run_chaos] Reiniciando broker Redis..."
docker compose restart redis

cat <<'EOF'
[run_chaos] Listo.
Revisa los logs con:
  docker compose logs -f validator aggregator audit
para observar cómo los servicios se recuperan y reprocesan mensajes pendientes.
EOF
