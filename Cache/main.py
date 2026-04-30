from fastapi import FastAPI
from src.cache_system import handle_request

app = FastAPI()

@app.post("/query")
def query(req: dict):
    return handle_request(req)