import random
import numpy as np
from src.config import ZONES 

QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

class TrafficGenerator:

    def __init__(self, mode="zipf"):
        self.mode = mode
        self.zones = list(ZONES.keys())

    def pick_zone(self):
        if self.mode == "zipf":
            ranks = np.arange(1, len(self.zones)+1)
            probs = 1 / np.power(ranks, 1.5)
            probs /= probs.sum()
            return np.random.choice(self.zones, p=probs)
        else:
            return random.choice(self.zones)

    def generate(self):
        q = random.choice(QUERY_TYPES)
        zone = self.pick_zone()

        random_conf = round(random.uniform(0, 1), 2)

        if q == "Q1":
            return {
                "query": q,
                "zone_id": zone,
                "confidence_min": 0.0 
            }

        elif q in ["Q2", "Q3"]:
            return {
                "query": q,
                "zone_id": zone,
                "confidence_min": random_conf
            }

        elif q == "Q4":
            return {
                "query": "Q4",
                "zone_a": zone,
                "zone_b": random.choice(self.zones),
                "confidence_min": random_conf
            }

        elif q == "Q5":
            return {
                "query": "Q5",
                "zone_id": zone,
                "bins": random.randint(5, 10) 
            }