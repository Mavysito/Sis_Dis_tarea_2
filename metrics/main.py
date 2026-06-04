"""
Sistema de Métricas (Tarea 2).

Registra los eventos del sistema asíncrono:
  - processed : consulta resuelta (source = cache | generator) con su latencia.
  - retry     : consulta reenviada al tópico de reintento.
  - recovered : consulta que tras uno o más reintentos terminó resolviéndose.
  - dlq       : consulta enviada a la Dead Letter Queue.

Además, un hilo de fondo muestrea periódicamente el backlog (lag del grupo de
consumo) de los tópicos Kafka y lo escribe en data/backlog_log.csv (req. 5.3).
"""
import os
import csv
import time
import threading

import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

app = FastAPI()

LOG_FILE = "data/metrics_log.csv"
BACKLOG_FILE = "data/backlog_log.csv"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.getenv("GROUP_ID", "query-workers")
TOPICS = os.getenv("TOPICS", "queries,queries.retry,queries.dlq").split(",")
BACKLOG_INTERVAL = float(os.getenv("BACKLOG_INTERVAL", "2.0"))

LOG_HEADER = ["event_type", "query", "zone", "source", "latency",
              "retry_count", "query_id", "timestamp"]

os.makedirs("data", exist_ok=True)
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w', newline='') as f:
        csv.writer(f).writerow(LOG_HEADER)
if not os.path.exists(BACKLOG_FILE):
    with open(BACKLOG_FILE, 'w', newline='') as f:
        csv.writer(f).writerow(["timestamp", "topic", "lag"])


class MetricEntry(BaseModel):
    event_type: str = "processed"
    query: str
    zone: str = "NA"
    source: str = ""
    latency: float = 0.0
    retry_count: int = 0
    query_id: str = ""


@app.post("/log")
def log_metric(entry: MetricEntry):
    with open(LOG_FILE, 'a', newline='') as f:
        csv.writer(f).writerow([
            entry.event_type, entry.query, entry.zone, entry.source,
            entry.latency, entry.retry_count, entry.query_id,
            pd.Timestamp.now(),
        ])
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


# --------------------------- Monitor de backlog ----------------------------
def sample_backlog():
    """Calcula el lag = (último offset producido) - (offset committeado por el grupo)."""
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)

    while True:
        try:
            rows = []
            now = pd.Timestamp.now()
            for topic in TOPICS:
                parts = consumer.partitions_for_topic(topic)
                if not parts:
                    continue
                tps = [TopicPartition(topic, p) for p in parts]
                end_offsets = consumer.end_offsets(tps)

                committed = admin.list_consumer_group_offsets(GROUP_ID)
                lag = 0
                for tp in tps:
                    end = end_offsets.get(tp, 0)
                    com = committed.get(tp)
                    com_off = com.offset if com else 0
                    lag += max(0, end - com_off)
                rows.append([now, topic, lag])

            if rows:
                with open(BACKLOG_FILE, 'a', newline='') as f:
                    csv.writer(f).writerows(rows)
        except Exception as e:
            print(f"[backlog] error muestreando lag: {e}", flush=True)
        time.sleep(BACKLOG_INTERVAL)


@app.on_event("startup")
def start_backlog_monitor():
    t = threading.Thread(target=sample_backlog, daemon=True)
    t.start()
    print("Monitor de backlog iniciado.", flush=True)
