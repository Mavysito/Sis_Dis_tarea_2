"""
Generador de Tráfico (Tarea 2) — ahora actúa como Kafka Producer.

En lugar de llamar de forma síncrona al Generador de Respuestas, publica las
consultas Q1-Q5 en el tópico principal de Kafka. Cada mensaje incluye los campos
exigidos por el req. 5.1: identificador único, número de reintentos y timestamp
de creación.

Parámetros por variables de entorno (para los distintos escenarios):
  DIST      uniform | zipf        (distribución de zonas)
  RATE      consultas por segundo (sostenido)
  DURATION  duración total en segundos
  SPIKE     1 para activar un pico de tráfico a mitad de la simulación
  SPIKE_RATE / SPIKE_DURATION     intensidad y duración del pico
"""
import os
import time
import json
import uuid
import random

import numpy as np
from kafka import KafkaProducer

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["q1_count", "q2_area", "q3_density", "q4_compare", "q5_confidence_dist"]

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "queries")

DIST = os.getenv("DIST", "uniform")
RATE = float(os.getenv("RATE", "10"))            # consultas/segundo
DURATION = int(os.getenv("DURATION", "300"))     # segundos
SPIKE = os.getenv("SPIKE", "0") == "1"
SPIKE_RATE = float(os.getenv("SPIKE_RATE", "100"))
SPIKE_DURATION = int(os.getenv("SPIKE_DURATION", "20"))


def generate_request(distribution="uniform"):
    """Genera una consulta siguiendo la distribución seleccionada (Tarea 1)."""
    if distribution == "uniform":
        zona = random.choice(ZONAS)
    else:
        idx = np.random.zipf(a=1.5)
        zona = ZONAS[(idx - 1) % len(ZONAS)]

    query = random.choice(QUERIES)
    conf = round(random.uniform(0.1, 0.9), 1)
    return query, zona, conf


def build_message(distribution):
    query, zona, conf = generate_request(distribution)
    if query == "q4_compare":
        z1, z2 = random.sample(ZONAS, 2)
        params = {"zone_a": z1, "zone_b": z2, "confidence_min": conf}
    else:
        params = {"zone_id": zona, "confidence_min": conf}

    return {
        "query_id": str(uuid.uuid4()),
        "query": query,
        "params": params,
        "retry_count": 0,
        "created_at": time.time(),
    }


def connect_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Producer conectado a {BOOTSTRAP}", flush=True)
            return producer
        except Exception as e:
            print(f"Kafka no disponible ({e}); reintento en 3s...", flush=True)
            time.sleep(3)


def run_simulation():
    producer = connect_producer()
    print(f"Iniciando simulación dist={DIST} rate={RATE}/s duración={DURATION}s spike={SPIKE}", flush=True)

    start = time.time()
    sent = 0
    spike_window = (DURATION / 2, DURATION / 2 + SPIKE_DURATION)

    while time.time() - start < DURATION:
        elapsed = time.time() - start
        rate = RATE
        if SPIKE and spike_window[0] <= elapsed <= spike_window[1]:
            rate = SPIKE_RATE

        msg = build_message(DIST)
        producer.send(TOPIC_MAIN, msg)
        sent += 1

        if sent % 200 == 0:
            print(f"Publicadas {sent} consultas...", flush=True)

        time.sleep(1.0 / rate if rate > 0 else 0.1)

    producer.flush()
    print(f"Simulación terminada. Total publicadas: {sent}", flush=True)


if __name__ == "__main__":
    run_simulation()
