
import requests
from src.cache import get, set

RESPONSE_URL = "http://response:8001/compute"
METRICS_URL = "http://metrics:8002/event"


def build_key(req):
    q = req["query"]
    if q == "Q1":
        return f"count:{req['zone_id']}:conf={req['confidence_min']}"
    elif q == "Q2":
        return f"area:{req['zone_id']}:conf={req['confidence_min']}"
    elif q == "Q3":
        return f"density:{req['zone_id']}:conf={req['confidence_min']}"
    elif q == "Q4":
        return f"compare:{req['zone_a']}:{req['zone_b']}:conf={req['confidence_min']}"
    elif q == "Q5":
        return f"confidence:{req['zone_id']}:bins={req['bins']}"
    return "unknown_key"


def handle_request(req):
    key = build_key(req)

    cached = get(key)
    if cached:
        try:
            requests.post(f"{METRICS_URL}", json={"type": "hit"})
        except: pass
        return {"source": "cache", "result": cached}

    # MISS — report before calling response so it's counted even on failure
    try:
        requests.post(f"{METRICS_URL}", json={"type": "miss"})
    except: pass

    response = requests.post(RESPONSE_URL, json=req, timeout=15)
    if response.status_code != 200:
        raise Exception(f"Response service returned {response.status_code}: {response.text[:100]}")

    result = response.json()["result"]
    set(key, result)
    return {"source": "response", "result": result}