import time
import random
import os
from fastapi import FastAPI, HTTPException
from src.loader import load_data
from src import queries

app = FastAPI()

DATA = load_data("data/967_buildings.csv")

AREA_MAP = {
    "Z1": 10.37,
    "Z2": 15.52,
    "Z3": 20.69,
    "Z4": 12.42,
    "Z5": 20.69
}

# 0.0 = never fail, 0.3 = 30% of requests fail, 1.0 = always fail
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))
# Extra seconds added to every response (simulates overload/slowness)
EXTRA_LATENCY = float(os.getenv("EXTRA_LATENCY", "0.0"))

@app.post("/compute")
def compute(req: dict):
    if FAILURE_RATE > 0.0 and random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="Simulated failure")

    start = time.time()

    if EXTRA_LATENCY > 0.0:
        time.sleep(EXTRA_LATENCY)
    q = req.get("query")

    if q == "Q1":
        result = queries.q1_count(DATA, req["zone_id"], req["confidence_min"])

    elif q == "Q2":
        result = queries.q2_area(DATA, req["zone_id"], req["confidence_min"])

    elif q == "Q3":
        result = queries.q3_density(
            DATA,
            req["zone_id"],
            AREA_MAP[req["zone_id"]],
            req["confidence_min"]
        )

    elif q == "Q4":
        result = queries.q4_compare(
            DATA,
            req["zone_a"],
            req["zone_b"],
            AREA_MAP,
            req["confidence_min"]
        )

    elif q == "Q5":
        result = queries.q5_confidence_dist(
            DATA,
            req["zone_id"],
            req["bins"]
        )

    else:
        return {"error": "Invalid query"}


    time.sleep(0.02)

    return {
        "result": result,
        "latency": time.time() - start
    }