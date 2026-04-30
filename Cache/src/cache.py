import redis
import json
import os

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

def get(key):
    value = r.get(key)
    return json.loads(value) if value else None

def set(key, value, ttl=60):
    r.setex(key, ttl, json.dumps(value)) 