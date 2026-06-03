import time
import os
import json
import threading
import redis
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

SNAPSHOT_PATH = "/data/snapshot.json"

app = FastAPI()
_lock = threading.Lock()

# ==================================================
# CONEXIÓN A REDIS (para evicciones)
# ==================================================
r_stats = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)

# ==================================================
# MÉTRICAS DEL CACHÉ (originales)
# ==================================================
metrics_data = {
    "hits": 0,
    "misses": 0,
    "latencies": [],
    "timestamps": [],
    "start_time": time.time()
}

# ==================================================
# NUEVAS MÉTRICAS PARA KAFKA (reintentos, DLQ, backlog, etc.)
# ==================================================
kafka_metrics = {
    "total_processed": 0,
    "success": 0,
    "failures": 0,           # mensajes que fueron a DLQ
    "retries_total": 0,      # suma de reintentos usados en todas las consultas
    "dlq_total": 0,          # número de mensajes enviados a DLQ
    "recovered": 0,          # consultas exitosas que requirieron al menos 1 reintento
    "latencies": [],         # latencias de consultas exitosas (segundos)
    "backlog_samples": [],   # lista de (timestamp, backlog_size)
    "recovery_times": []     # lista de tiempos de recuperación (segundos)
}

# Estado para calcular recovery time (falla del generador de respuestas)
recovery_state = {
    "failure_start": None,
    "backlog_before_failure": 0,
    "recovery_end": None
}

# ==================================================
# MODELOS DE DATOS PARA LOS ENDPOINTS DE KAFKA
# ==================================================
class KafkaMetric(BaseModel):
    consulta_id: str
    exito: bool
    latencia: float          # en segundos
    reintentos_usados: int
    topico_origen: str
    backlog_size: Optional[int] = None

class BacklogReport(BaseModel):
    timestamp: float
    backlog_size: int

class FailureEvent(BaseModel):
    event_type: str          # "start" o "end"
    backlog_at_failure: Optional[int] = None

# ==================================================
# ENDPOINT ORIGINAL PARA EVENTOS DEL CACHÉ
# ==================================================
@app.post("/event")
def add_event(event: dict):
    t = event.get("type")
    now = time.time()
    with _lock:
        if t == "hit":
            metrics_data["hits"] += 1
        elif t == "miss":
            metrics_data["misses"] += 1
        elif t == "latency":
            metrics_data["latencies"].append(event.get("value", 0))
            metrics_data["timestamps"].append(now)
    return {"status": "ok"}

# ==================================================
# NUEVOS ENDPOINTS PARA MÉTRICAS DE KAFKA
# ==================================================
@app.post("/metric")
def add_kafka_metric(metric: KafkaMetric):
    with _lock:
        kafka_metrics["total_processed"] += 1
        if metric.exito:
            kafka_metrics["success"] += 1
            kafka_metrics["latencies"].append(metric.latencia)
            if metric.reintentos_usados > 0:
                kafka_metrics["recovered"] += 1
        else:
            kafka_metrics["failures"] += 1
            kafka_metrics["dlq_total"] += 1
        kafka_metrics["retries_total"] += metric.reintentos_usados
        if metric.backlog_size is not None:
            kafka_metrics["backlog_samples"].append((time.time(), metric.backlog_size))
    return {"status": "ok"}

@app.post("/backlog")
def add_backlog(report: BacklogReport):
    with _lock:
        kafka_metrics["backlog_samples"].append((report.timestamp, report.backlog_size))
    return {"status": "ok"}

@app.post("/failure_event")
def failure_event(event: FailureEvent):
    with _lock:
        if event.event_type == "start":
            # Only record the first start — don't overwrite if already in failure mode
            if recovery_state["failure_start"] is None:
                recovery_state["failure_start"] = time.time()
                recovery_state["backlog_before_failure"] = event.backlog_at_failure or 0
        elif event.event_type == "end":
            if recovery_state["failure_start"] is not None:
                recovery_time = time.time() - recovery_state["failure_start"]
                kafka_metrics["recovery_times"].append(recovery_time)
                recovery_state["failure_start"] = None
    return {"status": "ok"}

# ==================================================
# ENDPOINT PARA ESTADÍSTICAS DE KAFKA
# ==================================================
@app.get("/kafka_stats")
def get_kafka_stats():
    total = kafka_metrics["total_processed"]
    if total == 0:
        return {"message": "No hay métricas Kafka aún"}
    
    # Tasa de reintentos (promedio de reintentos por consulta)
    retry_rate = kafka_metrics["retries_total"] / total
    
    # Tasa de recuperación (consultas que fallaron pero luego se recuperaron)
    recovery_rate = kafka_metrics["recovered"] / total
    
    # Tasa de DLQ (fallos definitivos)
    dlq_rate = kafka_metrics["dlq_total"] / total
    
    # Latencia p50 y p95 (en milisegundos)
    latencies_ms = [l * 1000 for l in kafka_metrics["latencies"]]
    if latencies_ms:
        p50 = np.percentile(latencies_ms, 50)
        p95 = np.percentile(latencies_ms, 95)
    else:
        p50 = p95 = 0.0
    
    # Backlog: último valor conocido y promedio
    backlog_values = [b for _, b in kafka_metrics["backlog_samples"]]
    current_backlog = backlog_values[-1] if backlog_values else 0
    avg_backlog = np.mean(backlog_values) if backlog_values else 0.0
    
    # Tiempo de recuperación promedio
    avg_recovery_time = np.mean(kafka_metrics["recovery_times"]) if kafka_metrics["recovery_times"] else 0.0
    
    # Throughput (consultas procesadas por segundo)
    elapsed = time.time() - metrics_data["start_time"]
    throughput = total / elapsed if elapsed > 0 else 0
    
    return {
        "total_processed": total,
        "success": kafka_metrics["success"],
        "failures": kafka_metrics["failures"],
        "retry_rate": round(retry_rate, 4),
        "recovery_rate": round(recovery_rate, 4),
        "dlq_rate": round(dlq_rate, 4),
        "throughput_req_sec": round(throughput, 2),
        "latency_p50_ms": round(p50, 2),
        "latency_p95_ms": round(p95, 2),
        "current_backlog": current_backlog,
        "avg_backlog": round(avg_backlog, 2),
        "avg_recovery_time_sec": round(avg_recovery_time, 2),
        "recovery_times": [round(t, 2) for t in kafka_metrics["recovery_times"]]
    }

# ==================================================
# ENDPOINT ORIGINAL PARA ESTADÍSTICAS DEL CACHÉ
# ==================================================
@app.get("/stats")
def get_stats():
    total = metrics_data["hits"] + metrics_data["misses"]
    if total == 0:
        return {"message": "Sin datos"}

    # Hit rate
    hit_rate = metrics_data["hits"] / total

    # Throughput del caché
    elapsed_seconds = time.time() - metrics_data["start_time"]
    throughput = total / elapsed_seconds if elapsed_seconds > 0 else 0

    # Latencia p50 y p95 del caché (en milisegundos)
    if metrics_data["latencies"]:
        lats = np.array(metrics_data["latencies"])
        p50 = np.percentile(lats, 50)
        p95 = np.percentile(lats, 95)
    else:
        p50 = p95 = 0

    # Eviction rate (evicciones por minuto)
    info = r_stats.info("stats")
    total_evictions = info.get("evicted_keys", 0)
    elapsed_minutes = elapsed_seconds / 60
    eviction_rate = total_evictions / elapsed_minutes if elapsed_minutes > 0 else 0

    # Cache efficiency (estimación)
    t_miss_avg = 0.050   # 50ms
    t_hit_avg = 0.001    # 1ms
    t_total_sin_cache = total * t_miss_avg
    t_total_con_cache = (metrics_data["hits"] * t_hit_avg) + (metrics_data["misses"] * t_miss_avg)
    efficiency = (t_total_sin_cache - t_total_con_cache) / t_total_sin_cache if t_total_sin_cache > 0 else 0.0

    return {
        "hit_rate": f"{hit_rate:.4f}",
        "throughput_req_sec": f"{throughput:.2f}",
        "latency_p50_ms": f"{p50*1000:.2f}ms",
        "latency_p95_ms": f"{p95*1000:.2f}ms",
        "eviction_rate_min": f"{eviction_rate:.2f}",
        "total_evictions": total_evictions,
        "cache_efficiency": f"{efficiency:.4f}"
    }

# ==================================================
# SNAPSHOT: guarda el estado final del experimento
# ==================================================
@app.post("/snapshot")
def save_snapshot():
    stats = get_kafka_stats()
    if "message" in stats:
        return {"status": "no_data", "message": stats["message"]}
    with _lock:
        snapshot = {
            "saved_at": time.time(),
            "stats": stats,
            "raw": {
                "total_processed": kafka_metrics["total_processed"],
                "success": kafka_metrics["success"],
                "failures": kafka_metrics["failures"],
                "retries_total": kafka_metrics["retries_total"],
                "dlq_total": kafka_metrics["dlq_total"],
                "recovered": kafka_metrics["recovered"],
                "recovery_times": [round(t, 2) for t in kafka_metrics["recovery_times"]],
            }
        }
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshot, f, indent=2)
    return {"status": "saved", "saved_at": snapshot["saved_at"]}

@app.get("/snapshot")
def get_snapshot():
    if not os.path.exists(SNAPSHOT_PATH):
        return {"message": "No hay snapshot guardado aún. El generador debe terminar primero."}
    with open(SNAPSHOT_PATH) as f:
        return json.load(f)

# ==================================================
# INICIO DEL SERVIDOR (si se ejecuta directamente)
# ==================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)