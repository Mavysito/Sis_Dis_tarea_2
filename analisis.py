import pandas as pd
import os

Distribuciones = ["Uniforme", "ZIPF"]
TTL_values = ["TTL_30", "TTL_60", "TTL_90"]
Cache_Sizes = ["50mb", "200mb", "500mb"]
Politicas = ["lru", "lfu"]

resumen_stats = []

for dist in Distribuciones:
    for ttl in TTL_values:
        for size in Cache_Sizes:
            for pol in Politicas:
                filepath = os.path.join(dist, ttl, f"{size}_{pol}.csv")
                
                if os.path.exists(filepath):
                    try:
                        # Leemos el CSV
                        df = pd.read_csv(filepath)
                        
                        # LIMPIEZA CRÍTICA: Elimina espacios en blanco en los nombres de columnas
                        df.columns = df.columns.str.strip()
                        
                        if df.empty or 'source' not in df.columns:
                            print(f"Saltando {filepath}: Archivo vacío o sin columna 'source'")
                            continue

                        # 1. Estadísticas Generales
                        total_reqs = len(df)
                        hits_totales = len(df[df['source'].str.strip() == 'cache'])
                        hr_total = (hits_totales / total_reqs * 100)
                        
                        # 2. Estadísticas por cada Query (qx)
                        stats_por_q = {}
                        for q_name in ['q1_count', 'q2_area', 'q3_density', 'q4_compare', 'q5_confidence_dist']:
                            df_q = df[df['query'].str.strip() == q_name]
                            q_total = len(df_q)
                            
                            if q_total > 0:
                                # Usamos strip() también en los valores por si acaso
                                q_hits = len(df_q[df_q['source'].str.strip() == 'cache'])
                                q_hr = (q_hits / q_total) * 100
                                q_lat = df_q['latency'].mean()
                            else:
                                q_hr, q_lat = 0, 0
                            
                            stats_por_q[q_name] = {"hr": q_hr, "count": q_total, "lat": q_lat}

                            total_reqs = len(df)
                            throughput = total_reqs / 300  # Req/seg

                            # Para el Eviction Rate, contamos cuántas veces el sistema tuvo que 
                            # escribir en la caché (Cache Misses que luego se guardan)
                            misses = len(df[df['source'].str.strip() == 'generator'])
                            eviction_rate = misses / 300 # Evicciones estimadas por segundo

                        resumen_stats.append({
                            "Distribucion": dist,
                            "TTL": ttl,
                            "Cache": size,
                            "Politica": pol.upper(),
                            "HR_Total": round(hr_total, 2),
                            "Q1_HR": round(stats_por_q['q1_count']['hr'], 2),
                            "Q1_Cnt": stats_por_q['q1_count']['count'],
                            "Q2_HR": round(stats_por_q['q2_area']['hr'], 2),
                            "Q2_Cnt": stats_por_q['q2_area']['count'],
                            "Q3_HR": round(stats_por_q['q3_density']['hr'], 2),
                            "Q3_Cnt": stats_por_q['q3_density']['count'],
                            "Q4_HR": round(stats_por_q['q4_compare']['hr'], 2),
                            "Q4_Cnt": stats_por_q['q4_compare']['count'],
                            "Q5_HR": round(stats_por_q['q5_confidence_dist']['hr'], 2),
                            "Q5_Cnt": stats_por_q['q5_confidence_dist']['count'],
                            "Avg_Lat": round(df['latency'].mean(), 2),
                            "Throughput": round(throughput, 2),
                            "Eviction_Rate": round(eviction_rate, 2)
                        })
                    except Exception as e:
                        print(f"Error procesando {filepath}: {e}")

# Crear DataFrame y guardar
if resumen_stats:
    df_resumen = pd.DataFrame(resumen_stats)
    df_resumen.to_csv("resumen_metricas.csv", index=False)
    print("Éxito: Archivo 'resumen_metricas.csv' generado.")
else:
    print("No se generaron estadísticas. Revisa las rutas de tus carpetas.")