"""
Consumidor Kafka (Tarea 2).

Núcleo de la arquitectura asíncrona. Cada consumidor:
  1. Lee consultas desde el tópico principal y desde el tópico de reintento.
  2. Consulta el Sistema Caché (Redis) directamente  ->  cache hit / cache miss.
  3. En miss, delega el cálculo al Generador de Respuestas vía HTTP.
  4. Ante un fallo temporal aplica la estrategia de reintentos -> DLQ.
  5. Reporta cada evento al Sistema de Métricas.

Varios consumidores comparten el mismo `group_id`, por lo que Kafka reparte
las particiones entre ellos -> escalado horizontal (req. 5.2).
"""
import os
import json
import time
import uuid

import redis
import requests
from kafka import KafkaConsumer, KafkaProducer

# --------------------------- Configuración ---------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "queries")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "queries.retry")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "queries.dlq")
GROUP_ID = os.getenv("GROUP_ID", "query-workers")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "0.5"))   # segundos * retry_count
CACHE_TTL = int(os.getenv("CACHE_TTL", "90"))
# Costo de procesamiento simulado por mensaje (experimento de escalado de
# consumidores). Con 0 no afecta; con >0 el consumidor se vuelve el cuello de
# botella y se hace visible el efecto de agregar consumidores.
PROCESS_DELAY_MS = float(os.getenv("PROCESS_DELAY_MS", "0"))

REDIS_HOST = os.getenv("REDIS_HOST", "cache")
GENERATOR_URL = os.getenv("GENERATOR_URL", "http://response_generator:8000")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8001/log")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "3.0"))

WORKER_ID = os.getenv("HOSTNAME", str(uuid.uuid4())[:8])

cache = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


# --------------------------- Utilidades ------------------------------------
def build_cache_key(query, params):
    """Mismo esquema de claves que la Tarea 1, para mantener coherencia."""
    if query == "q1_count":
        return f"q1:{params['zone_id']}:{params['confidence_min']}"
    if query == "q2_area":
        return f"q2:{params['zone_id']}:{params['confidence_min']}"
    if query == "q3_density":
        return f"q3:{params['zone_id']}:{params['confidence_min']}"
    if query == "q4_compare":
        return f"q4:{params['zone_a']}:{params['zone_b']}:{params['confidence_min']}"
    if query == "q5_confidence_dist":
        return f"q5:{params['zone_id']}:{params.get('bins', 5)}"
    return f"{query}:{json.dumps(params, sort_keys=True)}"


def zone_of(params):
    return params.get("zone_id", params.get("zone_a", "NA"))


def log_metric(event_type, msg, source=None, latency_ms=None):
    """Reporta un evento al Sistema de Métricas (best-effort)."""
    payload = {
        "event_type": event_type,
        "query": msg["query"],
        "zone": zone_of(msg["params"]),
        "source": source or "",
        "latency": latency_ms if latency_ms is not None else 0.0,
        "retry_count": msg.get("retry_count", 0),
        "query_id": msg["query_id"],
    }
    try:
        requests.post(METRICS_URL, json=payload, timeout=HTTP_TIMEOUT)
    except Exception as e:
        print(f"[{WORKER_ID}] No se pudo registrar métrica: {e}", flush=True)


def connect_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_MAIN, TOPIC_RETRY,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=50,
            )
            print(f"[{WORKER_ID}] Conectado a Kafka, grupo='{GROUP_ID}'", flush=True)
            return consumer
        except Exception as e:
            print(f"[{WORKER_ID}] Kafka no disponible ({e}); reintento en 3s...", flush=True)
            time.sleep(3)


def connect_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            print(f"[{WORKER_ID}] Producer no disponible ({e}); reintento en 3s...", flush=True)
            time.sleep(3)


# --------------------------- Lógica principal ------------------------------
def process(msg, producer):
    """Procesa una consulta. Devuelve True si se resolvió (hit o cálculo ok)."""
    query = msg["query"]
    params = msg["params"]
    retry_count = msg.get("retry_count", 0)
    # Latencia extremo a extremo: incluye el tiempo en cola (clave para comparar
    # contra el sistema síncrono y para evaluar el impacto del backlog).
    latency_ms = (time.time() - msg["created_at"]) * 1000.0

    # Costo de procesamiento simulado por mensaje (escalado de consumidores).
    if PROCESS_DELAY_MS > 0:
        time.sleep(PROCESS_DELAY_MS / 1000.0)

    cache_key = build_cache_key(query, params)

    # 1) Sistema Caché
    cached = cache.get(cache_key)
    if cached is not None:
        log_metric("processed", msg, source="cache", latency_ms=latency_ms)
        return True

    # 2) Cache miss -> Generador de Respuestas
    try:
        resp = requests.get(f"{GENERATOR_URL}/{query}", params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"status {resp.status_code}")
        result = resp.json().get("result")
        cache.setex(cache_key, CACHE_TTL, json.dumps(result))

        latency_ms = (time.time() - msg["created_at"]) * 1000.0
        log_metric("processed", msg, source="generator", latency_ms=latency_ms)
        # Si venía de un reintento y ahora tuvo éxito -> consulta recuperada.
        if retry_count > 0:
            log_metric("recovered", msg, source="generator", latency_ms=latency_ms)
        return True

    except Exception as e:
        handle_failure(msg, producer, str(e))
        return False


def handle_failure(msg, producer, reason):
    """Estrategia de reintentos / DLQ (req. 5.1)."""
    msg["retry_count"] = msg.get("retry_count", 0) + 1

    if msg["retry_count"] >= MAX_RETRIES:
        producer.send(TOPIC_DLQ, msg)
        producer.flush()
        log_metric("dlq", msg)
        print(f"[{WORKER_ID}] DLQ {msg['query_id']} ({reason}) tras {msg['retry_count']} intentos", flush=True)
    else:
        # Backoff simple proporcional al número de reintentos.
        time.sleep(RETRY_BACKOFF * msg["retry_count"])
        producer.send(TOPIC_RETRY, msg)
        producer.flush()
        log_metric("retry", msg)
        print(f"[{WORKER_ID}] RETRY {msg['query_id']} (intento {msg['retry_count']}) -> {reason}", flush=True)


def main():
    consumer = connect_consumer()
    producer = connect_producer()
    print(f"[{WORKER_ID}] Esperando consultas...", flush=True)
    for record in consumer:
        try:
            process(record.value, producer)
        except Exception as e:
            print(f"[{WORKER_ID}] Error procesando mensaje: {e}", flush=True)


if __name__ == "__main__":
    main()
