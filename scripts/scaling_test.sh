#!/usr/bin/env bash
# =============================================================================
# Experimento de ESCALADO HORIZONTAL de consumidores (req. 5.2).
#
# Corre la misma carga saturante con distinta cantidad de consumidores para
# medir el impacto de agregar consumidores sobre throughput, latencia y backlog.
#
# Para que el efecto sea visible:
#   - El tópico tiene 24 particiones (admite hasta 24 consumidores activos).
#   - RATE alto + PROCESS_DELAY_MS > 0  ->  el consumidor es el cuello de botella:
#     1 consumidor no da abasto y se acumula backlog; más consumidores drenan
#     más rápido y bajan la latencia, hasta cubrir la tasa ofrecida.
#
# Uso:    ./scripts/scaling_test.sh
# Config: variables de entorno (con sus valores por defecto):
#   CONSUMER_COUNTS="1 3 6 10 20"   DURATION=300   RATE=150   PROCESS_DELAY_MS=40
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=data/resultados
mkdir -p "$OUT"

DC="docker compose"
CONSUMER_COUNTS="${CONSUMER_COUNTS:-1 3 6 10 20}"
export DURATION="${DURATION:-300}"
export RATE="${RATE:-150}"
export PROCESS_DELAY_MS="${PROCESS_DELAY_MS:-40}"

echo "=== Experimento de escalado ==="
echo "    consumidores: $CONSUMER_COUNTS"
echo "    DURATION=$DURATION  RATE=$RATE  PROCESS_DELAY_MS=$PROCESS_DELAY_MS"

# Build una sola vez (las corridas siguientes reutilizan la imagen).
$DC build

for N in $CONSUMER_COUNTS; do
  tag=$(printf "scaling_%02d_consumers" "$N")
  echo ""
  echo ">>> [$tag] levantando $N consumidores..."
  rm -f data/metrics_log.csv data/backlog_log.csv

  $DC up -d --scale consumer="$N"

  echo ">>> [$tag] esperando a que termine el generador de tráfico (~${DURATION}s)..."
  $DC wait traffic_generator || true
  # Margen extra para que los consumidores drenen la cola tras dejar de producir.
  echo ">>> [$tag] drenando backlog 30s..."
  sleep 30

  cp -f data/metrics_log.csv "$OUT/${tag}_metrics.csv" 2>/dev/null || true
  cp -f data/backlog_log.csv "$OUT/${tag}_backlog.csv" 2>/dev/null || true
  $DC down -v --remove-orphans
  echo ">>> [$tag] guardado en $OUT/"
done

echo ""
echo "=== Listo. Analiza con:  python data/analisis_kafka.py ==="
