from fastapi import FastAPI
import pandas as pd
import redis
import os
import numpy as np
import json

app = FastAPI()
cache = redis.Redis(host=os.getenv("REDIS_HOST", "cache"), port=6379, decode_responses=True)


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


@app.get("/q1_count")
def q1_count(zone_id: str, confidence_min: float = 0.0):
    cache_key = f"q1:{zone_id}:{confidence_min}"
    cached = cache.get(cache_key)
    if cached:
        return {"source": "cache", "result": int(cached)}

    count = len(df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)])
    
    cache.setex(cache_key, 60, str(count))
    return {"source": "generator", "result": count}

@app.get("/q2_area")
def q2_area(zone_id: str, confidence_min: float = 0.0):
    cache_key = f"q2:{zone_id}:{confidence_min}"
    cached = cache.get(cache_key)
    if cached:
        return {"source": "cache", "result": json.loads(cached)}

    subset = df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)]
    
    res = {
        "avg_area": float(subset['area_in_meters'].mean()) if not subset.empty else 0,
        "total_area": float(subset['area_in_meters'].sum())
    }
    
    cache.setex(cache_key, 60, json.dumps(res))
    return {"source": "generator", "result": res}

@app.get("/q3_density")
def q3_density(zone_id: str, confidence_min: float = 0.0):
    cache_key = f"q3:{zone_id}:{confidence_min}"
    cached = cache.get(cache_key)
    if cached:
        return {"source": "cache", "density": float(cached)}

    count = len(df[(df['zone_id'] == zone_id) & (df['confidence'] >= confidence_min)])
    area_km2 = ZONE_AREAS.get(zone_id, 1.0)
    density = count / area_km2
    
    cache.setex(cache_key, 60, str(density))
    return {"source": "generator", "density": density}

@app.get("/q4_compare")
def q4_compare(zone_a: str, zone_b: str, confidence_min: float = 0.0):
    cache_key = f"q4:{zone_a}:{zone_b}:{confidence_min}"
    cached = cache.get(cache_key)
    if cached:
        return {"source": "cache", "result": json.loads(cached)}

    def get_d(z):
        c = len(df[(df['zone_id'] == z) & (df['confidence'] >= confidence_min)])
        return c / ZONE_AREAS.get(z, 1.0)

    d_a, d_b = get_d(zone_a), get_d(zone_b)
    res = {
        "densities": {zone_a: d_a, zone_b: d_b},
        "diff": abs(d_a - d_b),
        "more_dense": zone_a if d_a > d_b else zone_b
    }
    
    cache.setex(cache_key, 60, json.dumps(res))
    return {"source": "generator", "result": res}



@app.get("/q5_confidence_dist")
def q5_confidence_dist(zone_id: str, bins: int = 5):
    cache_key = f"q5:{zone_id}:{bins}"
    cached = cache.get(cache_key)
    if cached:
        return {"source": "cache", "histogram": json.loads(cached)}

    scores = df[df['zone_id'] == zone_id]['confidence']
    
    if scores.empty:
        return {"source": "generator", "histogram": []}

    counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
    
    hist = []
    for i in range(bins):
        hist.append({
            "range": f"{edges[i]:.2f}-{edges[i+1]:.2f}",
            "count": int(counts[i])
        })
    
    cache.setex(cache_key, 60, json.dumps(hist))
    return {"source": "generator", "histogram": hist}