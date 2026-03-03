import math
import numpy as np
import pandas as pd
from DP.utils.clipping import clip_series_from_cfg


def laplace_noise(scale: float) -> float:
    return np.random.laplace(0.0, scale)


def _build_numeric_bin_spec(dp_cfg: dict):
    if "U" not in dp_cfg or "V" not in dp_cfg:
        raise ValueError("Numeric histogram requires U and V")

    U = float(dp_cfg["U"])
    V = float(dp_cfg["V"])
    if V <= U:
        raise ValueError("V must be greater than U")

    if "bin_width" in dp_cfg:
        bin_width = float(dp_cfg["bin_width"])
        if bin_width <= 0:
            raise ValueError("bin_width must be > 0")
        bins = int(math.ceil((V - U) / bin_width))
    else:
        bins = int(dp_cfg.get("bins", 100))
        if bins <= 0:
            raise ValueError("bins must be > 0")
        bin_width = (V - U) / bins

    labels = []
    for i in range(bins):
        left = U + i * bin_width
        right = min(U + (i + 1) * bin_width, V)
        labels.append(f"[{left:g}, {right:g})")

    return U, V, bins, bin_width, labels


def _resolve_histogram_mode(series: pd.Series, dp_cfg: dict):
    """
    Determine histogram mode.
    Priority:
    1) categories provided => categorical
    2) explicit histogram_mode
    3) numeric if convertible and has bounds
    4) categorical fallback
    """
    if dp_cfg.get("categories") is not None:
        categories = dp_cfg["categories"]
        if not isinstance(categories, list) or len(categories) == 0:
            raise ValueError("categories must be a non-empty list")
        return "categorical", [str(c) for c in categories]

    mode = dp_cfg.get("histogram_mode")
    if mode in {"categorical", "numeric"}:
        if mode == "categorical":
            values = sorted(series.astype(str).dropna().unique().tolist())
            return "categorical", values
        _ = _build_numeric_bin_spec(dp_cfg)
        return "numeric", None

    # Auto detect
    try:
        _ = series.astype(float)
    except Exception:
        values = sorted(series.astype(str).dropna().unique().tolist())
        return "categorical", values

    if "U" in dp_cfg and "V" in dp_cfg:
        _ = _build_numeric_bin_spec(dp_cfg)
        return "numeric", None

    values = sorted(series.astype(str).dropna().unique().tolist())
    return "categorical", values


def user_level_histogram(df, dp_cfg: dict, user_col: str) -> dict:
    """
    User-level DP histogram with vector clipping.
    """

    attr = dp_cfg["attribute"]
    epsilon = dp_cfg["epsilon"]
    if attr not in df.columns:
        raise ValueError(f"Attribute '{attr}' not found in data")
    if user_col not in df.columns:
        raise ValueError(f"user_column '{user_col}' not found in data")

    working_df = df.copy()
    series = working_df[attr]
    mode, cat_labels = _resolve_histogram_mode(series, dp_cfg)
    if mode == "numeric":
        U, V, k, bin_width, bin_labels = _build_numeric_bin_spec(dp_cfg)
        working_df[attr], min_clip, max_clip = clip_series_from_cfg(working_df[attr], dp_cfg)
    else:
        bin_labels = cat_labels
        k = len(bin_labels)
        label_to_idx = {label: i for i, label in enumerate(bin_labels)}
        min_clip, max_clip = None, None

    # --------------------------------------------------
    # Group by user
    # --------------------------------------------------
    groups = working_df.groupby(user_col)

    user_histograms = []
    for _, g in groups:
        hist = [0] * k
        if mode == "numeric":
            for v in g[attr].astype(float).values:
                if U <= v <= V:
                    idx = min(int((v - U) // bin_width), k - 1)
                    hist[idx] += 1
        else:
            for v in g[attr].astype(str).values:
                idx = label_to_idx.get(v)
                if idx is not None:
                    hist[idx] += 1
        user_histograms.append(hist)

    if not user_histograms:
        raise ValueError("No users found after grouping")

    # --------------------------------------------------
    # Sort users by contribution
    # --------------------------------------------------
    user_histograms.sort(key=lambda h: sum(h), reverse=True)

    n_users = len(user_histograms)

    # --------------------------------------------------
    # index and clipping threshold C
    # --------------------------------------------------
    index = min(math.ceil((2 * k) / epsilon), n_users)
    C = sum(user_histograms[index - 1])

    # --------------------------------------------------
    # Clip per-user histograms
    # --------------------------------------------------
    clipped_hists = []
    for h in user_histograms:
        m_l = sum(h)
        if m_l <= C:
            clipped_hists.append(h)
        else:
            scale = C / m_l
            clipped_hists.append([scale * x for x in h])

    # --------------------------------------------------
    # Merge histograms
    # --------------------------------------------------
    merged = [0.0] * k
    for h in clipped_hists:
        for i in range(k):
            merged[i] += h[i]

    true_histogram = [float(v) for v in merged]

    # --------------------------------------------------
    # Noise
    # --------------------------------------------------
    sensitivity = 2 * C
    scale = sensitivity / epsilon

    noisy_histogram = [
        merged[i] + laplace_noise(scale)
        for i in range(k)
    ]
    noisy_histogram = [max(0.0, float(v)) for v in noisy_histogram]

    return {
        "query": "histogram",
        "privacy_level": "user",
        "attribute": attr,
        "user_column": user_col,
        "epsilon": epsilon,
        "histogram_mode": mode,
        "bin_labels": bin_labels,
        "bins": k,
        "true_histogram": true_histogram,
        "histogram": noisy_histogram,
        "sensitivity": sensitivity,
        "clipping": {"min_value": min_clip, "max_value": max_clip},
    }


def item_level_histogram(df, dp_cfg: dict) -> dict:
    """
    Item-level DP histogram.
    Supports:
    1) Numeric equal-width bins using bins/U/V
    2) Categorical bins using categories list
    """
    attr = dp_cfg["attribute"]
    epsilon = dp_cfg["epsilon"]

    if attr not in df.columns:
        raise ValueError(f"Attribute '{attr}' not found in data")
    series = df[attr]
    mode, cat_labels = _resolve_histogram_mode(series, dp_cfg)
    if mode == "categorical":
        labels = cat_labels
        counts = []
        str_series = series.astype(str)
        for label in labels:
            counts.append(int((str_series == label).sum()))
        min_clip, max_clip = None, None
    else:
        U, V, k, bin_width, labels = _build_numeric_bin_spec(dp_cfg)
        clipped_series, min_clip, max_clip = clip_series_from_cfg(series, dp_cfg)
        counts = [0] * k
        for v in clipped_series.astype(float).values:
            if U <= v <= V:
                idx = min(int((v - U) // bin_width), k - 1)
                counts[idx] += 1

    true_histogram = [float(c) for c in counts]

    sensitivity = 1.0
    scale = sensitivity / epsilon
    noisy_histogram = [c + laplace_noise(scale) for c in counts]
    noisy_histogram = [max(0.0, float(v)) for v in noisy_histogram]

    return {
        "query": "histogram",
        "privacy_level": "item",
        "attribute": attr,
        "epsilon": epsilon,
        "histogram_mode": mode,
        "bin_labels": labels,
        "bins": len(labels),
        "true_histogram": true_histogram,
        "histogram": noisy_histogram,
        "sensitivity": sensitivity,
        "clipping": {"min_value": min_clip, "max_value": max_clip},
    }
