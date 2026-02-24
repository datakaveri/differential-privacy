import pandas as pd


def resolve_clip_bounds(dp_cfg: dict):
    """
    Resolve clipping bounds from config.
    Global clipping is only controlled by explicit min_value/max_value.
    Query-specific numeric ranges (for example histogram U/V) are handled
    in their respective query implementations.
    """
    if "min_value" in dp_cfg and "max_value" in dp_cfg:
        min_v = float(dp_cfg["min_value"])
        max_v = float(dp_cfg["max_value"])
        if max_v <= min_v:
            raise ValueError("Require max_value > min_value")
        return min_v, max_v

    return None, None


def clip_series_from_cfg(series: pd.Series, dp_cfg: dict):
    """
    Clip numeric series to config bounds if provided.
    Returns clipped series and used bounds.
    """
    min_v, max_v = resolve_clip_bounds(dp_cfg)
    num = series.astype(float)
    if min_v is None or max_v is None:
        return num, None, None
    return num.clip(lower=min_v, upper=max_v), min_v, max_v
