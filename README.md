# Tarea 2 — Procesamiento y Fallback con Apache Kafka

Plataforma distribuida para el análisis de consultas geoespaciales sobre el dataset de
edificios de Santiago (Google Open Buildings). Esta segunda entrega evoluciona la
arquitectura síncrona de la Tarea 1 hacia un procesamiento **asíncrono y tolerante a
fallos** usando **Apache Kafka**, con cola de reintentos, *Dead Letter Queue* (DLQ) y
escalamiento horizontal mediante múltiples consumidores.

## Arquitectura

```
traffic_generator ──> [Kafka: queries] ──> consumer (grupo, escalable)
   (producer)                                  │
                                     ¿cache hit en Redis?
                              hit ◄────────────┤
                                               └─ miss ─> response_generator (solo cálculo)
                                                              │ éxito ─> guarda en caché
                                                              │ fallo ─> reintento / DLQ
                                          metrics  <── registra eventos + backlog
```

| Servicio | Rol |
|---|---|
| `traffic_generator` | Genera consultas Q1–Q5 y las publica en Kafka (productor). |
| `kafka` | Cola de mensajes (modo KRaft, sin Zookeeper). |
| `consumer` | Consume de Kafka, consulta la caché y aplica reintentos/DLQ. **Escalable.** |
| `response_generator` | Calcula la respuesta en caso de *cache miss*. |
| `cache` | Redis (respuestas precalculadas). |
| `metrics` | Registra métricas y mide el backlog. |

## Requisitos

- **Docker** y **Docker Compose v2** (`docker compose ...`).
- (Opcional, solo para analizar resultados) **Python 3.10+** con `pandas` y `matplotlib`.

No se necesita instalar Kafka, Redis ni las dependencias de Python a mano: todo corre
dentro de contenedores.

## Puesta en marcha

```bash
# 1. Construir y levantar todo el sistema
docker compose up --build

# 2. (en otra terminal) Levantar con varios consumidores en paralelo
docker compose up --build --scale consumer=3

# 3. Detener y limpiar
docker compose down -v
```

Al iniciar, el contenedor `kafka-init` crea automáticamente los tópicos
(`queries`, `queries.retry`, `queries.dlq`). Las métricas se escriben en
`data/metrics_log.csv` y el backlog en `data/backlog_log.csv`.

### Parámetros configurables

Se pasan como variables de entorno antes del comando. Ejemplos:

```bash
# Simular que el generador falla el 40% de las veces (dispara reintentos)
FAILURE_RATE=0.4 docker compose up --build --scale consumer=3

# Cambiar tasa de generación y duración de la corrida
RATE=50 DURATION=120 docker compose up --build
```

| Variable | Descripción | Default |
|---|---|---|
| `RATE` | Consultas por segundo del generador. | `10` |
| `DURATION` | Duración de la simulación (s). | `300` |
| `DIST` | Distribución de zonas: `uniform` o `zipf`. | `uniform` |
| `FAILURE_RATE` | Probabilidad de fallo del generador (0.0–1.0). | `0.0` |
| `MAX_RETRIES` | Máximo de reintentos antes de la DLQ. | `3` |
| `PROCESS_DELAY_MS` | Costo de proceso por mensaje (para pruebas de escalado). | `0` |

## Inspeccionar la cola de fallidos (DLQ)

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic queries.dlq --from-beginning
```

## Sistema base (arquitectura síncrona de la Tarea 1)

Para comparar contra la arquitectura síncrona original (sin Kafka):

```bash
docker compose -f docker-compose.base.yml up --build
```

## Experimentos y análisis

> ⚠️ **Los scripts `.sh` solo funcionan en Linux/macOS (Bash).** En Windows no se ejecutan
> directamente; usa **WSL2** (Windows Subsystem for Linux) o **Git Bash**, o bien lanza los
> comandos `docker compose` de las tablas anteriores a mano.

```bash
# Ejecutar los 7 escenarios de evaluación (o uno solo: ./scripts/run_scenarios.sh 3)
./scripts/run_scenarios.sh all

# Experimento de escalado (1, 3, 6, 10 y 20 consumidores)
./scripts/scaling_test.sh

# Procesar los CSV y generar tablas + gráficos
python data/analisis_kafka.py
```

Los resultados quedan en `data/resultados/` (CSV por escenario), el resumen en
`data/resumen_kafka.csv` y los gráficos en `data/graficos/`.

## Estructura del repositorio

```
.
├── docker-compose.yml          # Stack con Kafka (arquitectura Tarea 2)
├── docker-compose.base.yml     # Stack síncrono (arquitectura Tarea 1)
├── traffic_generator/          # Productor Kafka
├── consumer/                   # Consumidor: caché, reintentos y DLQ
├── response_generator/         # Cálculo de respuestas (+ inyección de fallos)
├── metrics/                    # Registro de métricas y backlog
├── base_system/                # Código original de la Tarea 1 (escenario base)
├── scripts/                    # run_scenarios.sh, scaling_test.sh
├── data/                       # Dataset, logs, análisis y gráficos
```

## Notas

- El sistema está pensado para Docker; los puertos `8000` (generador), `8001` (métricas),
  `6379` (Redis) y `9092` (Kafka) quedan expuestos para inspección.
- Detalle de las decisiones de diseño en [`CAMBIOS_TAREA2.md`](CAMBIOS_TAREA2.md).
