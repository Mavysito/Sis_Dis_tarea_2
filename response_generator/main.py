from fastapi import FastAPI, HTTPException
import pandas as pd
import os
import time
import random
import numpy as np

app = FastAPI()

# ---------------------------------------------------------------------------
# Inyección de fallos (Tarea 2, escenarios 4-7).
# Se controla por variables de entorno para poder simular degradación sin
# tocar el código: probabilidad de error 503 y latencia artificial.
# ---------------------------------------------------------------------------
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))          # 0.0 - 1.0
ARTIFICIAL_LATENCY_MS = float(os.getenv("ARTIFICIAL_LATENCY_MS", "0.0"))

ZONE_AREAS = {
    "Z1": 5.5, "Z2": 6.2, "Z3": 8.1, "Z4": 4.8, "Z5": 7.3
}

print("Cargando y optimizando dataset de Santiago...")
try:
    df = pd.read_csv("data/santiago_buildings.csv")

    df['zone_id'] = df['zone_id'].astype('category')
    df['confidence'] = df['confidence'].astype('float32')
    df['area_in_meters'] = df['area_in_meters'].astype('float32')

    print(f"Dataset listo: {len(df)} registros cargados.")
except Exception as e:
    print(f"Error al cargar datos: {e}")
    df = pd.DataFrame(columns=["latitude", "longitude", "area_in_meters", "confidence", "zone_id"])


def _maybe_fail():
    """Simula degradación temporal del Generador de Respuestas."""
    if ARTIFICIAL_LATENCY_MS > 0:
        time.sleep(ARTIFICIAL_LATENCY_MS / 1000.0)
    if FAILURE_RATE > 0 and random.random() < FAILURE_RATE:
        # 503: el consumidor lo interpretará como fallo temporal y reintentará.
        raise HTTPException(status_code=503, detail="Fallo temporal simulado")


@app.get("/health")
def health():
    return {"status": "ok"}


# Endpoints SOLO de cálculo: la caché vive ahora en el consumidor (Tarea 2).
@app.get("/q1_count")
def q1_count(zone_id: str, confidence_min: float = 0.0):
    _maybe_fail()
    count = len(df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)])
    return {"result": count}


@app.get("/q2_area")
def q2_area(zone_id: str, confidence_min: float = 0.0):
    _maybe_fail()
    subset = df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)]
    res = {
        "avg_area": float(subset['area_in_meters'].mean()) if not subset.empty else 0,
        "total_area": float(subset['area_in_meters'].sum())
    }
    return {"result": res}


@app.get("/q3_density")
def q3_density(zone_id: str, confidence_min: float = 0.0):
    _maybe_fail()
    count = len(df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)])
    area_km2 = ZONE_AREAS.get(zone_id, 1.0)
    density = count / area_km2
    return {"result": density}


@app.get("/q4_compare")
def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    _maybe_fail()

    def get_d(z):
        c = len(df[(df['zone_id'] == z) & (df['confidence'] >= confidence_min)])
        return c / ZONE_AREAS.get(z, 1.0)

    d_a, d_b = get_d(zone_a), get_d(zone_b)
    res = {
        "densities": {zone_a: d_a, zone_b: d_b},
        "diff": abs(d_a - d_b),
        "more_dense": zone_a if d_a > d_b else zone_b
    }
    return {"result": res}


@app.get("/q5_confidence_dist")
def q5_confidence_dist(zone_id: str, bins: int = 5):
    _maybe_fail()
    scores = df[df['zone_id'] == zone_id]['confidence']

    if scores.empty:
        return {"result": []}

    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))

    hist = []
    for i in range(bins):
        hist.append({
            "range": f"{edges[i]:.2f}-{edges[i+1]:.2f}",
            "count": int(counts[i])
        })

    return {"result": hist}
