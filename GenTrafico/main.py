import time
import requests
from src.generator import TrafficGenerator

CACHE_URL = "http://cache:8000/query"

#MODO DE GENERADOR (uniform o zipf)
gen = TrafficGenerator(mode="uniform")

def send_request(req):
    try:
        response = requests.post(CACHE_URL, json=req, timeout=2)
        return response.json()
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    for i in range(10000):
        req = gen.generate()
        print(f"Enviando consulta {i+1}", flush=True) 
        res = send_request(req)
        time.sleep(0.05)

    final_stats = requests.get("http://metrics:8002/stats").json()
    print("\n" + "="*20 + " MÉTRICAS FINALES " + "="*20)
    print(final_stats)