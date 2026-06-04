import pandas as pd

# Definición de las zonas según las coordenadas del PDF de la tarea
ZONAS_BOX = {
    "Z1": {"lat": (-33.45, -33.41), "lon": (-70.62, -70.58)}, # Providencia
    "Z2": {"lat": (-33.43, -33.39), "lon": (-70.58, -70.50)}, # Las Condes
    "Z3": {"lat": (-33.55, -33.48), "lon": (-70.82, -70.74)}, # Maipú
    "Z4": {"lat": (-33.47, -33.43), "lon": (-70.68, -70.63)}, # Santiago Centro
    "Z5": {"lat": (-33.48, -33.42), "lon": (-70.80, -70.72)}  # Pudahuel
}

def asignar_zona(row):
    for zona, coords in ZONAS_BOX.items():
        if (coords["lat"][0] <= row['latitude'] <= coords["lat"][1] and 
            coords["lon"][0] <= row['longitude'] <= coords["lon"][1]):
            return zona
    return None

def procesar_dataset(input_csv):
    print("Leyendo dataset original...")
    # Leemos solo las columnas necesarias para ahorrar RAM
    chunks = pd.read_csv(input_csv, usecols=['latitude', 'longitude', 'area_in_meters', 'confidence'], chunksize=100000)
    
    filtered_data = []
    
    for chunk in chunks:
        # Filtro rápido por rango general de Santiago para no procesar todo el país
        santiago_chunk = chunk[
            (chunk['latitude'] >= -33.6) & (chunk['latitude'] <= -33.3) &
            (chunk['longitude'] >= -70.9) & (chunk['longitude'] <= -70.4)
        ].copy()
        
        if not santiago_chunk.empty:
            santiago_chunk['zone_id'] = santiago_chunk.apply(asignar_zona, axis=1)
            filtered_data.append(santiago_chunk[santiago_chunk['zone_id'].notna()])

    final_df = pd.concat(filtered_data)
    final_df.to_csv("data/santiago_buildings.csv", index=False)
    print(f"Proceso terminado. Se guardaron {len(final_df)} edificios etiquetados.")

if __name__ == "__main__":
    # Cambia 'dataset_original.csv' por el nombre del archivo que descargaste
    procesar_dataset("/home/sudomavy/Escritorio/sis_dis/data/Data.csv")
    pass