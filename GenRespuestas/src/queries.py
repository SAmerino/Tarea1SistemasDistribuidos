import numpy as np
from statistics import mean

# Q1
def q1_count(data, zone_id, confidence_min=0.0):
    return sum(1 for r in data[zone_id] if r["confidence"] >= confidence_min)

# Q2
def q2_area(data, zone_id, confidence_min=0.0):
    areas = [r["area"] for r in data[zone_id] if r["confidence"] >= confidence_min]

    if len(areas) == 0:
        return {"avg_area": 0, "total_area": 0, "n": 0}

    return {
        "avg_area": mean(areas),
        "total_area": sum(areas),
        "n": len(areas)
    }

# Q3
def q3_density(data, zone_id, area_km2, confidence_min=0.0):
    count = q1_count(data, zone_id, confidence_min)
    return count / area_km2 if area_km2 > 0 else 0

# Q4
def q4_compare(data, zone_a, zone_b, area_map, confidence_min=0.0):
    da = q3_density(data, zone_a, area_map[zone_a], confidence_min)
    db = q3_density(data, zone_b, area_map[zone_b], confidence_min)

    return {
        "zone_a": da,
        "zone_b": db,
        "winner": zone_a if da > db else zone_b
    }

# Q5
def q5_confidence_dist(data, zone_id, bins=5):
    scores = [r["confidence"] for r in data[zone_id]]

    if len(scores) == 0:
        return []

    hist, edges = np.histogram(scores, bins=bins, range=(0, 1))

    return [
        {"min": edges[i], "max": edges[i+1], "count": int(hist[i])}
        for i in range(len(hist))
    ]