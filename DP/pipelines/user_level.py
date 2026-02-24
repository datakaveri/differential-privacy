import pandas as pd
from DP.queries.mean import user_level_mean
from DP.queries.histogram import user_level_histogram
from DP.queries.count import user_level_count
from DP.DPError import DPError
from DP.utils.plotting import plot_user_level_histogram


def run_user_level_dp(config: dict, query: str) -> dict:
    data_cfg = config["data"]
    dp_cfg = config["differential_privacy"]

    df = pd.read_csv(data_cfg["csv"])

    user_col = dp_cfg.get("user_column")
    if not user_col:
        raise DPError(
            "INVALID_CONFIG",
            "Missing 'user_column' in differential_privacy config"
        )

    if user_col not in df.columns:
        raise DPError(
            "INVALID_CONFIG",
            f"user_column '{user_col}' not found in CSV"
        )

    # -------------------------------
    # Mean
    # -------------------------------
    if query == "mean":
        return user_level_mean(df, dp_cfg, user_col)

    # -------------------------------
    # Histogram + auto-plot
    # -------------------------------
    elif query == "histogram":
        result = user_level_histogram(df, dp_cfg, user_col)

        plot_path = plot_user_level_histogram(
            result["histogram"],
            output_dir=dp_cfg.get("output_dir", "output"),
            title=f"User-Level DP Histogram ({dp_cfg['attribute']})"
        )

        result["plot"] = plot_path
        return result
    elif query == "count":
        return user_level_count(df, dp_cfg, user_col)

    else:
        raise DPError(
            "INVALID_QUERY",
            f"Unsupported user-level query: {query}"
        )
