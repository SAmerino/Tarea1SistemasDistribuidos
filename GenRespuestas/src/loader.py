import pandas as pd
from src.config import ZONES

def load_data(csv_path):
    data = {z: [] for z in ZONES}
    
    chunk_size = 100_000 
    print(f"Cargando archivo pesado: {csv_path}...")

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, engine='c'):
        for zone_id, (lat_min, lat_max, lon_min, lon_max) in ZONES.items():
            subset = chunk[
                (chunk["latitude"] >= lat_min) & (chunk["latitude"] <= lat_max) &
                (chunk["longitude"] >= lon_min) & (chunk["longitude"] <= lon_max)
            ]
            if not subset.empty:
                relevant = subset[["latitude", "longitude", "area_in_meters", "confidence"]]
                relevant = relevant.rename(columns={"latitude": "lat", "longitude": "lon", "area_in_meters": "area"})
                data[zone_id].extend(relevant.to_dict("records"))
    
    print("Carga finalizada con éxito.")
    return data