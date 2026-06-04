#!/usr/bin/env bash
# =============================================================================
# Ejecuta los 7 escenarios de evaluación de la Tarea 2.
#
# Cada escenario:
#   1. Limpia los logs de métricas (metrics_log.csv / backlog_log.csv).
#   2. Levanta el stack con la configuración del escenario.
#   3. Espera a que el traffic_generator termine.
#   4. Guarda los CSV resultantes con un nombre propio en data/resultados/.
#
# Uso:   ./scripts/run_scenarios.sh <numero|all>
# Ej.:   ./scripts/run_scenarios.sh 3
#        ./scripts/run_scenarios.sh all
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=data/resultados
mkdir -p "$OUT"

DC="docker compose"
DURATION="${DURATION:-300}"      # duración por defecto de cada escenario (s)

reset_logs() {
  rm -f data/metrics_log.csv data/backlog_log.csv
}

# Guarda los CSV del escenario y baja el stack.
save_and_down() {
  local name="$1"
  cp -f data/metrics_log.csv  "$OUT/${name}_metrics.csv"  2>/dev/null || true
  cp -f data/backlog_log.csv  "$OUT/${name}_backlog.csv"  2>/dev/null || true
  $DC down -v --remove-orphans
  $DC -f docker-compose.base.yml down -v --remove-orphans 2>/dev/null || true
  echo ">>> Escenario '$name' guardado en $OUT/"
}

# Espera a que el contenedor traffic_generator termine (exit).
wait_traffic() {
  echo ">>> Esperando a que termine el generador de tráfico..."
  $DC wait traffic_generator || true
}

scenario_1_base() {
  echo "=== Escenario 1: Sistema Base (síncrono, sin Kafka) ==="
  reset_logs
  DURATION=$DURATION $DC -f docker-compose.base.yml up --build -d
  # El traffic_generator base corre 300s fijos; lo dejamos correr DURATION y paramos.
  sleep "$DURATION"
  cp -f data/metrics_log.csv "$OUT/01_base_metrics.csv" 2>/dev/null || true
  $DC -f docker-compose.base.yml down -v --remove-orphans
  echo ">>> Escenario 1 guardado."
}

scenario_2_one_consumer() {
  echo "=== Escenario 2: Kafka + 1 Consumer ==="
  reset_logs
  DURATION=$DURATION RATE=20 $DC up --build -d --scale consumer=1
  wait_traffic
  save_and_down "02_kafka_1consumer"
}

scenario_3_multi_consumer() {
  echo "=== Escenario 3: Kafka + Múltiples Consumers (3) ==="
  reset_logs
  DURATION=$DURATION RATE=20 $DC up --build -d --scale consumer=3
  wait_traffic
  save_and_down "03_kafka_3consumers"
}

scenario_4_failure() {
  echo "=== Escenario 4: Falla Temporal del Generador de Respuestas ==="
  reset_logs
  DURATION=$DURATION RATE=20 $DC up --build -d --scale consumer=3
  echo ">>> Corriendo 40s normal, luego se detiene response_generator 40s..."
  sleep 40
  $DC stop response_generator
  sleep 40
  echo ">>> Reiniciando response_generator (recuperación)..."
  $DC start response_generator
  wait_traffic
  save_and_down "04_falla_temporal"
}

scenario_5_retries() {
  echo "=== Escenario 5: Reintentos (FAILURE_RATE=0.4) ==="
  reset_logs
  DURATION=$DURATION RATE=20 FAILURE_RATE=0.4 $DC up --build -d --scale consumer=3
  wait_traffic
  save_and_down "05_reintentos"
}

scenario_6_spike() {
  echo "=== Escenario 6: Spike de Tráfico ==="
  reset_logs
  DURATION=$DURATION RATE=10 SPIKE=1 SPIKE_RATE=200 SPIKE_DURATION=20 \
    $DC up --build -d --scale consumer=3
  wait_traffic
  save_and_down "06_spike"
}

scenario_7_recovery() {
  echo "=== Escenario 7: Recuperación ante Fallos (FAILURE_RATE alto + caída) ==="
  reset_logs
  DURATION=$DURATION RATE=20 FAILURE_RATE=0.6 $DC up --build -d --scale consumer=3
  echo ">>> Caída total a los 30s por 30s..."
  sleep 30
  $DC stop response_generator
  sleep 30
  $DC start response_generator
  wait_traffic
  save_and_down "07_recuperacion"
}

case "${1:-all}" in
  1) scenario_1_base ;;
  2) scenario_2_one_consumer ;;
  3) scenario_3_multi_consumer ;;
  4) scenario_4_failure ;;
  5) scenario_5_retries ;;
  6) scenario_6_spike ;;
  7) scenario_7_recovery ;;
  all)
     scenario_1_base
     scenario_2_one_consumer
     scenario_3_multi_consumer
     scenario_4_failure
     scenario_5_retries
     scenario_6_spike
     scenario_7_recovery
     ;;
  *) echo "Uso: $0 <1-7|all>"; exit 1 ;;
esac

echo "=== Listo. Resultados en $OUT/ ==="
