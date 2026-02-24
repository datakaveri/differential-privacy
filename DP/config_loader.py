import json
from DP.DPError import DPError


def load_config(path: str) -> dict:
    try:
        with open(path, "r") as f:
            config = json.load(f)
    except Exception as e:
        raise DPError("CONFIG_LOAD_FAILED", str(e))

    return config
