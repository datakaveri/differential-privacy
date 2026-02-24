import numpy as np
from DP.utils.clipping import clip_series_from_cfg


def _apply_count_filter(df, dp_cfg: dict):
    """
    Optional pre-filter for count queries.
    If no count_attribute/count_operator/count_value is provided, returns df unchanged.
    """
    attr = dp_cfg.get("count_attribute")
    op = dp_cfg.get("count_operator")
    value = dp_cfg.get("count_value")

    # Backward compatible: no filter fields => count all rows.
    if attr is None and op is None and value is None:
        return df, {
            "mode": "all_records",
            "attribute": None,
            "operator": None,
            "value": None,
        }

    # If user selected a value but not an operator, default to equality.
    if value is not None and op is None:
        op = "=="

    if attr not in df.columns:
        raise ValueError(f"count_attribute '{attr}' not found in data")

    valid_ops = {">", ">=", "<", "<=", "==", "!="}
    if op not in valid_ops:
        raise ValueError(f"Invalid count_operator '{op}'. Use one of {sorted(valid_ops)}")

    series = df[attr]
    # Optional clipping before numeric comparisons.
    if op in {">", ">=", "<", "<="}:
        series, min_clip, max_clip = clip_series_from_cfg(series, dp_cfg)
    else:
        min_clip, max_clip = None, None

    if op == ">":
        try:
            mask = series > float(value)
        except Exception as exc:
            raise ValueError(
                f"Operator '>' requires numeric column/value for '{attr}'"
            ) from exc
    elif op == ">=":
        try:
            mask = series >= float(value)
        except Exception as exc:
            raise ValueError(
                f"Operator '>=' requires numeric column/value for '{attr}'"
            ) from exc
    elif op == "<":
        try:
            mask = series < float(value)
        except Exception as exc:
            raise ValueError(
                f"Operator '<' requires numeric column/value for '{attr}'"
            ) from exc
    elif op == "<=":
        try:
            mask = series <= float(value)
        except Exception as exc:
            raise ValueError(
                f"Operator '<=' requires numeric column/value for '{attr}'"
            ) from exc
    elif op == "==":
        mask = series == value
    else:  # "!="
        mask = series != value

    filtered_df = df[mask]
    filter_meta = {
        "mode": "filtered_records",
        "attribute": attr,
        "operator": op,
        "value": value,
        "clipping": {"min_value": min_clip, "max_value": max_clip},
    }
    return filtered_df, filter_meta


def user_level_count(df, dp_cfg, user_col: str):
    """
    User-level predicate count:
    number of users with at least one matching record.
    """
    epsilon = dp_cfg["epsilon"]

    filtered_df, filter_meta = _apply_count_filter(df, dp_cfg)
    if user_col not in df.columns:
        raise ValueError(f"user_column '{user_col}' not found in data")

    true_count = int(filtered_df[user_col].nunique())
    sensitivity = 1.0
    noise = np.random.laplace(0, sensitivity / epsilon)
    dp_count = int(round(true_count + noise))

    return {
        "query": "count",
        "privacy_level": "user",
        "user_column": user_col,
        "count_unit": "users_with_at_least_one_match",
        "epsilon": epsilon,
        "true_count": true_count,
        "dp_count": dp_count,
        "sensitivity": sensitivity,
        "count_filter": filter_meta,
    }


def item_level_count(df, dp_cfg: dict):
    """
    Item-level predicate count:
    number of records matching the filter.
    """
    epsilon = dp_cfg["epsilon"]
    filtered_df, filter_meta = _apply_count_filter(df, dp_cfg)

    true_count = int(len(filtered_df))
    sensitivity = 1.0
    noise = np.random.laplace(0, sensitivity / epsilon)
    dp_count = int(round(true_count + noise))

    return {
        "query": "count",
        "privacy_level": "item",
        "count_unit": "matching_records",
        "epsilon": epsilon,
        "true_count": true_count,
        "dp_count": dp_count,
        "sensitivity": sensitivity,
        "count_filter": filter_meta,
    }
