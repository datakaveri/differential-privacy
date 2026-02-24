import pandas as pd
from DP.queries.mean import item_level_mean
from DP.queries.histogram import item_level_histogram
from DP.queries.count import item_level_count
from DP.DPError import DPError

def run_item_level_dp(config: dict, query: str) -> dict:
    data_cfg = config["data"]
    dp_cfg = config["differential_privacy"]

    df = pd.read_csv(data_cfg["csv"])

    if query == "mean":
        return item_level_mean(df, dp_cfg)

    elif query == "histogram":
        return item_level_histogram(df, dp_cfg)
    elif query == "count":
        return item_level_count(df, dp_cfg)

    else:
        raise DPError("INVALID_QUERY", f"Unsupported item-level query: {query}")
