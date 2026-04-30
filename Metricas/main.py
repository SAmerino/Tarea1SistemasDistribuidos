import time
import os
import redis
import numpy as np
from fastapi import FastAPI

app = FastAPI()
# Conexión a Redis para obtener las evicciones reales del motor
r_stats = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)

# Estado en memoria
metrics_data = {
    "hits": 0,
    "misses": 0,
    "latencies": [],
    "timestamps": [], # Para calcular throughput y eviction rate
    "start_time": time.time()
}

@app.post("/event")
def add_event(event: dict):
    t = event.get("type")
    now = time.time()
    
    if t == "hit":
        metrics_data["hits"] += 1
    elif t == "miss":
        metrics_data["misses"] += 1
    elif t == "latency":
        metrics_data["latencies"].append(event.get("value", 0))
        metrics_data["timestamps"].append(now)
    
    return {"status": "ok"}

@app.get("/stats")
def get_stats():
    total = metrics_data["hits"] + metrics_data["misses"]
    if total == 0:
        return {"message": "Sin datos"}

    # 1. Hit Rate
    hit_rate = metrics_data["hits"] / total

    # 2. Throughput (Consultas totales / tiempo transcurrido)
    elapsed_seconds = time.time() - metrics_data["start_time"]
    throughput = total / elapsed_seconds if elapsed_seconds > 0 else 0

    # 3. Latencia p50 y p95
    if metrics_data["latencies"]:
        lats = np.array(metrics_data["latencies"])
        p50 = np.percentile(lats, 50)
        p95 = np.percentile(lats, 95)
    else:
        p50 = p95 = 0

    # 4. Eviction Rate (Evictions / minuto)
    # Obtenemos evicciones totales desde Redis
    info = r_stats.info("stats")
    total_evictions = info.get("evicted_keys", 0)
    elapsed_minutes = elapsed_seconds / 60
    eviction_rate = total_evictions / elapsed_minutes if elapsed_minutes > 0 else 0

    # 5. Cache Efficiency mejorada
    # Definimos constantes realistas (en segundos)
    t_miss_avg = 0.050  # 50ms (simulado como costo de base de datos)
    t_hit_avg = 0.001   # 1ms (costo de Redis)
    
    t_total_sin_cache = total * t_miss_avg
    
    # Calculamos el tiempo real estimado
    t_total_con_cache = (metrics_data["hits"] * t_hit_avg) + (metrics_data["misses"] * t_miss_avg)
    
    # IMPORTANTE: Verificamos que el denominador no sea cero antes de dividir
    if t_total_sin_cache > 0:
        efficiency = (t_total_sin_cache - t_total_con_cache) / t_total_sin_cache
    else:
        efficiency = 0.0

    return {
        "hit_rate": f"{hit_rate:.4f}",
        "throughput_req_sec": f"{throughput:.2f}",
        "latency_p50_ms": f"{p50*1000:.2f}ms",
        "latency_p95_ms": f"{p95*1000:.2f}ms",
        "eviction_rate_min": f"{eviction_rate:.2f}",
        "total_evictions": total_evictions,
        "cache_efficiency": f"{efficiency:.4f}"
    }