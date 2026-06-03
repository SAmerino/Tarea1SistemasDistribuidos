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

CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))

def set(key, value, ttl=None):
    r.setex(key, ttl if ttl is not None else CACHE_TTL, json.dumps(value))