import numpy as np
import pandas as pd

from DP.DPError import DPError
from DP.utils.clipping import resolve_clip_bounds


def _cell_index(lat: float, lon: float, resolution: int) -> str:
    """
    Preferred: H3 index.
    Fallback: rounded lat/lon grid key if h3 is unavailable.
    """
    try:
        import h3  # type: ignore

        return h3.geo_to_h3(lat=lat, lng=lon, resolution=resolution)
    except Exception:
        # Approximate fallback cell if h3 package is missing.
        # Use coarser rounding to mimic spatial aggregation.
        # res=8 -> digits=3 (roughly city-block scale in degrees)
        digits = max(1, min(5, int(round(1 + resolution / 4))))
        return f"grid_{round(lat, digits)}_{round(lon, digits)}"


def _hex_radius_deg(resolution: int, size_scale: float = 1.0) -> float:
    # Coarser at low resolution, finer at high resolution.
    base = max(0.00035, 0.022 / (2 ** max(0, resolution - 3)))
    return base * max(0.1, float(size_scale))


def _hex_boundary(lat: float, lon: float, radius_deg: float):
    pts = []
    for i in range(6):
        ang = (np.pi / 3) * i + (np.pi / 6)
        d_lat = radius_deg * np.sin(ang)
        d_lon = (radius_deg * np.cos(ang)) / max(0.2, np.cos(np.deg2rad(lat)))
        pts.append([float(lat + d_lat), float(lon + d_lon)])
    return pts


def _build_precomputed_hex_grid(
    lat: pd.Series, lon: pd.Series, resolution: int, size_scale: float = 1.0
):
    """
    Pre-calculate hexagon centers and boundaries over min/max lat/lon bounding box.
    """
    radius = _hex_radius_deg(resolution, size_scale=size_scale)
    lon_step = np.sqrt(3) * radius
    lat_step = 1.5 * radius

    lat_min, lat_max = float(lat.min()), float(lat.max())
    lon_min, lon_max = float(lon.min()), float(lon.max())

    # Small padding so edge points are included.
    lat_min -= radius
    lat_max += radius
    lon_min -= lon_step
    lon_max += lon_step

    cells = []
    row = 0
    cur_lat = lat_min
    while cur_lat <= lat_max + 1e-12:
        lon_offset = (lon_step / 2.0) if (row % 2 == 1) else 0.0
        cur_lon = lon_min + lon_offset
        while cur_lon <= lon_max + 1e-12:
            cell_id = f"hex_{row}_{len(cells)}"
            cells.append(
                {
                    "cell_id": cell_id,
                    "center_lat": float(cur_lat),
                    "center_lon": float(cur_lon),
                    "boundary": _hex_boundary(float(cur_lat), float(cur_lon), radius),
                }
            )
            cur_lon += lon_step
        row += 1
        cur_lat += lat_step
    return cells


def _assign_points_to_hexes(lat: pd.Series, lon: pd.Series, cells: list):
    """
    Assign each point to nearest precomputed hex center.
    """
    centers_lat = np.array([c["center_lat"] for c in cells], dtype=float)
    centers_lon = np.array([c["center_lon"] for c in cells], dtype=float)

    assigned = []
    for la, lo in zip(lat.values.astype(float), lon.values.astype(float)):
        d2 = (centers_lat - la) ** 2 + (centers_lon - lo) ** 2
        idx = int(np.argmin(d2))
        assigned.append(cells[idx]["cell_id"])
    return assigned


def _extract_lat_lon(df: pd.DataFrame, dp_cfg: dict):
    spatial_cfg = dp_cfg.get("spatial", {})

    lat_col = spatial_cfg.get("lat_column")
    lon_col = spatial_cfg.get("lon_column")
    location_col = spatial_cfg.get("location_column")

    if lat_col and lon_col:
        if lat_col not in df.columns or lon_col not in df.columns:
            raise DPError(
                "INVALID_CONFIG",
                f"Missing spatial columns: lat={lat_col}, lon={lon_col}",
            )
        lat = df[lat_col].astype(float)
        lon = df[lon_col].astype(float)
        return lat, lon

    if location_col:
        if location_col not in df.columns:
            raise DPError("INVALID_CONFIG", f"Missing location column: {location_col}")
        split_lat_lon = (
            df[location_col]
            .astype(str)
            .str.strip("[]")
            .str.split(",")
        )
        lon = split_lat_lon.apply(lambda x: float(x[0].strip()))
        lat = split_lat_lon.apply(lambda x: float(x[1].strip()))
        return lat, lon

    raise DPError(
        "INVALID_CONFIG",
        "Provide either spatial.lat_column+spatial.lon_column or spatial.location_column",
    )


def _build_hats(df: pd.DataFrame, dp_cfg: dict) -> pd.DataFrame:
    spatial_cfg = dp_cfg.get("spatial", {})
    temporal_cfg = dp_cfg.get("temporal", {})

    timestamp_col = temporal_cfg.get("timestamp_column")
    if not timestamp_col or timestamp_col not in df.columns:
        raise DPError(
            "INVALID_CONFIG",
            f"Missing temporal.timestamp_column or column not found: {timestamp_col}",
        )

    timeslot_minutes = int(temporal_cfg.get("timeslot_minutes", 60))
    if timeslot_minutes <= 0 or timeslot_minutes > 1440:
        raise DPError("INVALID_CONFIG", "temporal.timeslot_minutes must be in 1..1440")

    h3_resolution = int(spatial_cfg.get("h3_resolution", 8))
    if h3_resolution < 0 or h3_resolution > 15:
        raise DPError("INVALID_CONFIG", "spatial.h3_resolution must be in 0..15")
    hex_size_scale = float(spatial_cfg.get("hex_size_scale", 1.0))

    start_hour = temporal_cfg.get("start_hour")
    end_hour = temporal_cfg.get("end_hour")

    lat, lon = _extract_lat_lon(df, dp_cfg)

    out = df.copy()
    out["_timestamp"] = pd.to_datetime(out[timestamp_col], errors="coerce")
    out = out.dropna(subset=["_timestamp"]).copy()

    out["_hour"] = out["_timestamp"].dt.hour
    out["_minute"] = out["_timestamp"].dt.minute

    if start_hour is not None and end_hour is not None:
        out = out[(out["_hour"] >= int(start_hour)) & (out["_hour"] <= int(end_hour))]

    # Bucket by total minutes-of-day so timeslot_minutes can be > 60.
    total_min = out["_hour"] * 60 + out["_minute"]
    slot_start = (total_min // timeslot_minutes) * timeslot_minutes
    slot_h = (slot_start // 60).astype(int)
    slot_m = (slot_start % 60).astype(int)
    out["_timeslot"] = slot_h.astype(str) + "_" + slot_m.astype(str)

    # Precomputed hex grid over bbox, then assignment of each point to nearest hex.
    cells = _build_precomputed_hex_grid(
        lat.loc[out.index],
        lon.loc[out.index],
        h3_resolution,
        size_scale=hex_size_scale,
    )
    out["_hex_cell_id"] = _assign_points_to_hexes(lat.loc[out.index], lon.loc[out.index], cells)
    out["HAT"] = out["_timeslot"].astype(str) + " " + out["_hex_cell_id"].astype(str)

    # Keep only active cells (cells that received at least one point).
    used = set(out["_hex_cell_id"].astype(str).unique().tolist())
    active_cells = [c for c in cells if c["cell_id"] in used]

    # Attach grid metadata for downstream plotting.
    out.attrs["hex_cells"] = active_cells
    return out


def run_spatiotemporal_dp(config: dict) -> dict:
    data_cfg = config.get("data", {})
    dp_cfg = config.get("differential_privacy", {})

    csv_path = data_cfg.get("csv")
    if not csv_path:
        raise DPError("INVALID_CONFIG", "Missing data.csv path")

    query = dp_cfg.get("query")
    if query != "mean":
        raise DPError(
            "INVALID_QUERY",
            "spatio_temporal_analysis currently supports only query='mean'",
        )

    attr = dp_cfg.get("attribute")
    if not attr:
        raise DPError("INVALID_CONFIG", "Missing differential_privacy.attribute")

    epsilon = float(dp_cfg["epsilon"])
    min_clip, max_clip = resolve_clip_bounds(dp_cfg)
    if min_clip is None or max_clip is None:
        if "U" not in dp_cfg or "V" not in dp_cfg:
            raise DPError(
                "INVALID_CONFIG",
                "For spatio-temporal mean, provide min_value/max_value or U/V",
            )
        V = float(dp_cfg["V"])
        U = float(dp_cfg["U"])
    else:
        V = float(min_clip)
        U = float(max_clip)
    if U <= V:
        raise DPError("INVALID_CONFIG", "Require U > V for spatio-temporal mean")

    df = pd.read_csv(csv_path)
    if attr not in df.columns:
        raise DPError("INVALID_CONFIG", f"attribute '{attr}' not found in CSV")

    st_df = _build_hats(df, dp_cfg)
    if st_df.empty:
        raise DPError("EMPTY_DATA", "No rows left after temporal/spatial preprocessing")

    st_df["_value"] = st_df[attr].astype(float).clip(lower=V, upper=U)
    grouped = st_df.groupby(["_timeslot", "_hex_cell_id"], as_index=False).agg(
        true_mean=("_value", "mean"),
        count=("_value", "count"),
    )
    grouped["HAT"] = grouped["_timeslot"].astype(str) + " " + grouped["_hex_cell_id"].astype(str)

    sensitivity = U - V
    scale = sensitivity / epsilon
    noise = np.random.laplace(0.0, scale, len(grouped))
    grouped["dp_mean"] = grouped["true_mean"] + noise

    return {
        "query": "mean",
        "privacy_level": "spatio-temporal-item",
        "spatio_temporal_analysis": True,
        "attribute": attr,
        "epsilon": epsilon,
        "sensitivity": sensitivity,
        "clipping": {"min_value": V, "max_value": U},
        "num_hats": int(len(grouped)),
        "timeslots": sorted(grouped["_timeslot"].astype(str).unique().tolist()),
        "hex_cells": st_df.attrs.get("hex_cells", []),
        "hat_means": grouped[
            ["HAT", "_timeslot", "_hex_cell_id", "true_mean", "dp_mean", "count"]
        ].rename(
            columns={"_timeslot": "timeslot", "_hex_cell_id": "cell_id"}
        ).to_dict(
            orient="records"
        ),
    }
