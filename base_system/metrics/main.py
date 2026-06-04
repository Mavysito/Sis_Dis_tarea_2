from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
import os
import csv

app = FastAPI()
LOG_FILE = "data/metrics_log.csv"
if not os.path.exists(LOG_FILE):
    os.makedirs("data", exist_ok=True)
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["query", "zone", "source", "latency", "timestamp"])

class MetricEntry(BaseModel):
    query: str
    zone: str
    source: str
    latency: float

@app.post("/log")
def log_metric(entry: MetricEntry):
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([entry.query, entry.zone, entry.source, entry.latency, pd.Timestamp.now()])
    return {"status": "ok"}

@app.get("/report")
def get_report():
    if not os.path.exists(LOG_FILE):
        return {"error": "No hay datos acumulados"}
    
    df = pd.read_csv(LOG_FILE)
    if df.empty: return {"error": "Archivo vacío"}

    hit_rate = len(df[df['source'] == 'cache']) / len(df)
    
    return {
        "hit_rate": f"{hit_rate:.2%}",
        "p50_latency_ms": round(df['latency'].quantile(0.5), 2),
        "p95_latency_ms": round(df['latency'].quantile(0.95), 2),
        "total_requests": len(df),
        "cache_hits": len(df[df['source'] == 'cache']),
        "cache_misses": len(df[df['source'] == 'generator'])
    }

