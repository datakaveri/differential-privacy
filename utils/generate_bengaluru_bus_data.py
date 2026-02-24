import random
import math
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def _point_along_route(route_points, t):
    # route_points: [(lat, lon), ...], t in [0, 1]
    if t <= 0:
        return route_points[0]
    if t >= 1:
        return route_points[-1]

    seg_count = len(route_points) - 1
    x = t * seg_count
    i = min(int(x), seg_count - 1)
    frac = x - i
    lat1, lon1 = route_points[i]
    lat2, lon2 = route_points[i + 1]
    lat = lat1 + frac * (lat2 - lat1)
    lon = lon1 + frac * (lon2 - lon1)
    return lat, lon


def _route_stops(route_points, n_stops=18):
    return [_point_along_route(route_points, i / (n_stops - 1)) for i in range(n_stops)]


def _sample_point_in_circle(center_lat, center_lon, radius_km):
    """
    Uniform random sample in a disk centered at (center_lat, center_lon).
    """
    # sqrt for area-uniform radius sampling
    r_km = radius_km * math.sqrt(random.random())
    theta = random.uniform(0, 2 * math.pi)

    # Approx conversion at Bengaluru latitude
    dlat = r_km / 111.0
    dlon = r_km / (111.0 * max(0.2, math.cos(math.radians(center_lat))))

    lat = center_lat + dlat * math.sin(theta)
    lon = center_lon + dlon * math.cos(theta)
    return lat, lon


def generate_dataset(
    out_csv="data/BengaluruBusSynthetic.csv",
    seed=42,
    n_rows=20000,
    layout="corridor",
    circle_center=(12.9716, 77.5946),
    circle_radius_km=8.0,
):
    random.seed(seed)

    # Coarse Bengaluru corridors (synthetic, city spread)
    routes = {
        "R1_Majestic_Whitefield": [
            (12.9760, 77.5720), (12.9850, 77.6100), (12.9980, 77.6800)
        ],
        "R2_Majestic_ElectronicCity": [
            (12.9760, 77.5720), (12.9350, 77.6100), (12.8450, 77.6650)
        ],
        "R3_Hebbal_SilkBoard": [
            (13.0350, 77.5970), (12.9850, 77.6050), (12.9180, 77.6230)
        ],
        "R4_Banashankari_ITPL": [
            (12.9250, 77.5600), (12.9700, 77.6000), (12.9970, 77.6960)
        ],
        "R5_Yelahanka_KRMarket": [
            (13.1000, 77.5960), (13.0400, 77.5900), (12.9650, 77.5750)
        ],
    }

    bus_ids = [f"BUS_{i:03d}" for i in range(1, 61)]
    drivers = [f"DRIVER_{i:03d}" for i in range(1, 121)]
    genders = ["Male", "Female"]

    start = datetime(2025, 1, 15, 6, 0, 0)
    end = datetime(2025, 1, 15, 23, 0, 0)
    minutes_span = int((end - start).total_seconds() // 60)

    route_stops = {name: _route_stops(points, n_stops=18) for name, points in routes.items()}

    rows = []

    for _ in range(n_rows):
        if layout == "circle":
            route_name = "CIRCLE_CITY_SPREAD"
            lat, lon = _sample_point_in_circle(
                center_lat=circle_center[0],
                center_lon=circle_center[1],
                radius_km=circle_radius_km,
            )
        else:
            route_name = random.choice(list(routes.keys()))
            # Pick from repeated stop locations to create meaningful HAT aggregation.
            lat, lon = random.choice(route_stops[route_name])
            # tiny jitter to avoid perfectly identical visuals while preserving aggregation
            lat += random.uniform(-0.00005, 0.00005)
            lon += random.uniform(-0.00005, 0.00005)

        # realistic urban bus speed with congestion noise
        base_speed = random.gauss(30, 15)
        if random.random() < 0.18:
            base_speed += random.uniform(20, 25)
        speed = max(5, min(95, base_speed))

        age = random.randint(22, 60)
        gender = random.choice(genders)
        driver = random.choice(drivers)
        bus = random.choice(bus_ids)

        # snap to 10-minute buckets to increase temporal overlap
        minute = random.randint(0, minutes_span)
        minute = (minute // 10) * 10
        dt = start + timedelta(minutes=minute)

        rows.append(
            {
                "FULLNAMEENGLISH": driver,
                "AGE": age,
                "GENDER": gender,
                "speed": round(speed, 2),
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "bus_id": bus,
                "route_id": route_name,
            }
        )

    df = pd.DataFrame(rows)
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return str(out_path), len(df)


if __name__ == "__main__":
    # Default dataset (corridor layout)
    path, n = generate_dataset()
    print(f"Wrote {n} rows to {path}")

    # Circle layout dataset for neat hex tiling visualization
    circle_path, circle_n = generate_dataset(
        out_csv="data/BengaluruBusCircularSynthetic.csv",
        seed=42,
        n_rows=20000,
        layout="circle",
        circle_center=(12.9716, 77.5946),
        circle_radius_km=8.0,
    )
    print(f"Wrote {circle_n} rows to {circle_path}")
