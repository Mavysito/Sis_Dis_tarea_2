import time
import random
import requests
import numpy as np

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["q1_count", "q2_area", "q3_density", "q4_compare", "q5_confidence_dist"]
ENDPOINT = "http://response_generator:8000"

def generate_request(distribution="uniform"):
    """
    Genera una consulta siguiendo la distribución seleccionada.
    """
    if distribution == "uniform":
        zona = random.choice(ZONAS)
    else: 
        idx = np.random.zipf(a=1.2)
        zona = ZONAS[(idx - 1) % len(ZONAS)]
    
    query = random.choice(QUERIES)
    conf = round(random.uniform(0.1, 0.9), 1)
    
    return query, zona, conf


def run_simulation(duration_sec=300, dist="uniform"):
    start_time = time.time()
    print(f"Iniciando simulación ({dist}) por {duration_sec} segundos...")
    
    while time.time() - start_time < duration_sec:
        query, zona, conf = generate_request(dist)
        
        if query == "q4_compare":
            z1, z2 = random.sample(ZONAS, 2)
            params = {"zone_a": z1, "zone_b": z2, "confidence_min": conf}
        else:
            params = {"zone_id": zona, "confidence_min": conf}
        
        try:
            t_start = time.perf_counter()
            response = requests.get(f"{ENDPOINT}/{query}", params=params)
            t_end = time.perf_counter()
            
            if response.status_code == 200:
                data = response.json()
                fuente_dato = data.get("source")
                if fuente_dato is None:
                    fuente_dato = "generator"

                requests.post("http://metrics:8001/log", json={
                    "query": query,
                    "zone": zona,
                    "source": fuente_dato,
                    "latency": (t_end - t_start) * 1000  # ms
                })
            else:
                print(f"Error {response.status_code} en {query}: {response.text}")

        except Exception as e:
            print(f"Error de conexión: {e}")
        
        time.sleep(0.1)

if __name__ == "__main__":
    print("Iniciando simulación Uniforme...")
    run_simulation(dist="uniform")