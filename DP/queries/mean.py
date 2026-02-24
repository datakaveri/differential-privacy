import math
import numpy as np
from DP.utils.clipping import clip_series_from_cfg, resolve_clip_bounds


def laplace_noise(scale: float) -> float:
    return np.random.laplace(0.0, scale)


def user_level_mean(df, dp_cfg: dict, user_col: str) -> dict:
    """
    User-level DP mean using per-user sum clipping (index-i method).
    """

    attr = dp_cfg["attribute"]
    epsilon = dp_cfg["epsilon"]

    if attr not in df.columns:
        raise ValueError(f"Attribute '{attr}' not found in data")

    # --------------------------------------------------
    # Group by user
    # --------------------------------------------------
    working_df = df.copy()
    try:
        working_df[attr], min_clip, max_clip = clip_series_from_cfg(working_df[attr], dp_cfg)
    except Exception as exc:
        raise ValueError(f"Attribute '{attr}' must be numeric for mean query") from exc
    groups = working_df.groupby(user_col)

    # Per-user record counts and sums
    user_stats = []
    for _, g in groups:
        values = g[attr].astype(float).values
        user_stats.append({
            "m": len(values),               # number of records
            "sum": float(values.sum())      # sum of values
        })

    if not user_stats:
        raise ValueError("No users found after grouping")

    # --------------------------------------------------
    # Global bounds
    # Prefer explicit bounds from config; fallback to data-derived.
    # --------------------------------------------------
    all_values = working_df[attr].astype(float).values
    if min_clip is not None and max_clip is not None:
        V = float(min_clip)
        U = float(max_clip)
    elif "U" in dp_cfg and "V" in dp_cfg:
        U = float(dp_cfg["U"])
        V = float(dp_cfg["V"])
    else:
        U = float(all_values.max())
        V = float(all_values.min())
    if U <= V:
        raise ValueError("For user-level mean, require U > V")

    # --------------------------------------------------
    # Sort users by contribution (descending m)
    # --------------------------------------------------
    user_stats.sort(key=lambda x: x["m"], reverse=True)

    n_users = len(user_stats)

    # --------------------------------------------------
    # index-i and T_epsilon
    # --------------------------------------------------
    index_i = min(math.ceil(2 / epsilon), n_users)
    m_i = user_stats[index_i - 1]["m"]
    T_eps = m_i * (U - V)

    # --------------------------------------------------
    # Clip per-user sums
    # --------------------------------------------------
    clipped_sums = []
    total_contributions = 0

    for idx, u in enumerate(user_stats):
        m_l = u["m"]
        user_sum = u["sum"]

        # transformed sum
        y_star = user_sum - m_l * V

        if idx + 1 >= index_i:
            A = y_star
            B = y_star
        else:
            d = m_l * (U - V) - T_eps
            A = d / 2
            B = m_l * (U - V) - d / 2

        clipped_star = max(A, min(y_star, B))
        clipped_sum = clipped_star + m_l * V

        clipped_sums.append(clipped_sum)
        total_contributions += m_l

    clipped_mean = sum(clipped_sums) / total_contributions
    true_mean = float(all_values.mean())

    # --------------------------------------------------
    # Sensitivity and noise
    # --------------------------------------------------
    sensitivity = T_eps / total_contributions
    scale = sensitivity / epsilon

    dp_mean = clipped_mean + laplace_noise(scale)

    return {
        "query": "mean",
        "privacy_level": "user",
        "attribute": attr,
        "user_column": user_col,
        "epsilon": epsilon,
        "dp_mean": dp_mean,
        "true_mean": true_mean,
        "sensitivity": sensitivity,
        "clipping": {"min_value": V, "max_value": U},
    }


# ---------------- ITEM LEVEL ---------------- #

def item_level_mean(df, dp_cfg):
    attr = dp_cfg["attribute"]
    epsilon = dp_cfg["epsilon"]
    min_val, max_val = resolve_clip_bounds(dp_cfg)
    if min_val is None or max_val is None:
        max_val = float(dp_cfg["max_value"])
        min_val = float(dp_cfg.get("min_value", 0.0))
        if max_val <= min_val:
            raise ValueError("For item-level mean, require max_value > min_value")

    try:
        clipped_values = df[attr].astype(float).clip(lower=min_val, upper=max_val)
    except Exception as exc:
        raise ValueError(f"Attribute '{attr}' must be numeric for mean query") from exc
    true_mean = float(clipped_values.mean())
    sensitivity_sum = max_val - min_val

    true_sum = clipped_values.sum()
    true_count = len(df)
    
    noise_sum = np.random.laplace(0, sensitivity_sum / (epsilon / 2))
    noise_count = np.random.laplace(0, 1 / (epsilon / 2))

    noisy_mean = (true_sum + noise_sum) / (true_count + noise_count)

    sensitivity_mean = sensitivity_sum / max(1, true_count)

    return {
        "query": "mean",
        "privacy_level": "item",
        "attribute": attr,
        "epsilon": epsilon,
        "dp_mean": noisy_mean,
        "true_mean": true_mean,
        "sensitivity": sensitivity_mean,
        "sensitivity_sum": sensitivity_sum,
        "sensitivity_count": 1.0,
        "clipping": {"min_value": min_val, "max_value": max_val},
    }
