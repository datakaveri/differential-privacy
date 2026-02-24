from DP.pipelines.user_level import run_user_level_dp
from DP.pipelines.item_level import run_item_level_dp
from DP.pipelines.spatiotemporal import run_spatiotemporal_dp
from DP.DPError import DPError

def route_dp_request(config: dict) -> dict:
    if "differential_privacy" not in config:
        raise DPError("CONFIG_INVALID", "Missing 'differential_privacy' section")

    dp = config["differential_privacy"]

    query = dp.get("query")
    if not query:
        raise DPError("CONFIG_INVALID", "Missing 'query' in differential_privacy config")
    if "epsilon" not in dp:
        raise DPError("CONFIG_INVALID", "Missing 'epsilon' in differential_privacy config")
    try:
        epsilon = float(dp["epsilon"])
    except Exception as exc:
        raise DPError("CONFIG_INVALID", "epsilon must be numeric") from exc
    if epsilon <= 0:
        raise DPError("CONFIG_INVALID", "epsilon must be > 0")

    if bool(dp.get("spatio_temporal_analysis", False)):
        if query != "mean":
            raise DPError(
                "INVALID_QUERY",
                "spatio_temporal_analysis currently supports only query='mean'",
            )
        return run_spatiotemporal_dp(config)

    level = dp.get("level")
    if not level:
        raise DPError("CONFIG_INVALID", "Missing 'level' in differential_privacy config")

    if level == "user":
        return run_user_level_dp(config, query)

    elif level == "item":
        return run_item_level_dp(config, query)

    else:
        raise DPError("INVALID_DP_LEVEL", f"Unknown DP level: {level}")
