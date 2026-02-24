import os
from copy import deepcopy

import numpy as np

from DP.config_loader import load_config
from DP.router import route_dp_request
from DP.DPError import DPError
from DP.config_normalizer import normalize_config


EPSILON_SLIDER_VALUES = [
    0.001,
    0.002,
    0.005,
    0.01,
    0.02,
    0.05,
    0.1,
    0.2,
    0.5,
    1,
    2,
    5,
    10,
    20,
    50,
    100,
]


def _auto_detect_file(folder: str, ext: str) -> str:
    """
    Detect exactly one file with given extension inside a folder.
    """
    if not os.path.isdir(folder):
        raise DPError("PATH_NOT_FOUND", f"Directory not found: {folder}")

    files = [f for f in os.listdir(folder) if f.endswith(ext)]
    if not files:
        raise DPError("FILE_NOT_FOUND", f"No {ext} file found in {folder}")
    if len(files) > 1:
        raise DPError(
            "AMBIGUOUS_INPUT",
            f"Multiple {ext} files found in {folder}: {files}"
        )

    return os.path.join(folder, files[0])


def _try_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _to_float_list(values):
    if not isinstance(values, list):
        return None
    out = []
    for v in values:
        fv = _try_float(v)
        if fv is None:
            return None
        out.append(float(fv))
    return out


def _attach_precomputed_noise_curve(result: dict) -> dict:
    """
    Precompute noisy outputs for the fixed epsilon slider values.

    Noise is sampled independently for each epsilon from Laplace(0, sensitivity/epsilon),
    so values can be positive or negative and are not derived from a single base draw.
    """
    if not isinstance(result, dict):
        return result

    query = str(result.get("query", "")).strip().lower()
    if query not in {"mean", "count", "histogram"}:
        return result

    epsilon0 = _try_float(result.get("epsilon"))
    sensitivity = _try_float(result.get("sensitivity"))
    if epsilon0 is None or sensitivity is None or epsilon0 <= 0 or sensitivity < 0:
        return result

    if query == "mean":
        true_value = _try_float(result.get("true_mean"))
        noisy_value = _try_float(result.get("dp_mean"))
        if true_value is None or noisy_value is None:
            return result

        precomputed = []
        for eps in EPSILON_SLIDER_VALUES:
            scale = max(1e-12, sensitivity / float(eps))
            noise = float(np.random.laplace(0.0, scale))
            dp_mean = true_value + noise
            precomputed.append(
                {
                    "epsilon": float(eps),
                    "noise": noise,
                    "dp_mean": dp_mean,
                }
            )

        result["epsilon_slider_precomputed"] = {
            "metric": "mean",
            "base_epsilon": epsilon0,
            "sensitivity": sensitivity,
            "true_mean": true_value,
            "base_dp_mean": noisy_value,
            "values": precomputed,
        }
        return result

    if query == "count":
        true_value = _try_float(result.get("true_count"))
        noisy_value = _try_float(result.get("dp_count"))
        if true_value is None or noisy_value is None:
            return result

        precomputed = []
        for eps in EPSILON_SLIDER_VALUES:
            scale = max(1e-12, sensitivity / float(eps))
            noise = float(np.random.laplace(0.0, scale))
            dp_count_raw = true_value + noise
            precomputed.append(
                {
                    "epsilon": float(eps),
                    "noise": noise,
                    "dp_count_raw": dp_count_raw,
                    "dp_count": int(round(dp_count_raw)),
                }
            )

        result["epsilon_slider_precomputed"] = {
            "metric": "count",
            "base_epsilon": epsilon0,
            "sensitivity": sensitivity,
            "true_count": int(round(true_value)),
            "base_dp_count": int(round(noisy_value)),
            "values": precomputed,
        }
        return result

    true_hist = _to_float_list(result.get("true_histogram"))
    base_noisy_hist = _to_float_list(result.get("histogram"))
    if true_hist is None or base_noisy_hist is None:
        return result

    precomputed = []
    for eps in EPSILON_SLIDER_VALUES:
        scale = max(1e-12, sensitivity / float(eps))
        noise_vector = [float(np.random.laplace(0.0, scale)) for _ in true_hist]
        noisy_hist = [max(0.0, float(t + n)) for t, n in zip(true_hist, noise_vector)]
        precomputed.append(
            {
                "epsilon": float(eps),
                "noise": noise_vector,
                "noisy_histogram": noisy_hist,
            }
        )

    result["epsilon_slider_precomputed"] = {
        "metric": "histogram",
        "base_epsilon": epsilon0,
        "sensitivity": sensitivity,
        "true_histogram": true_hist,
        "base_noisy_histogram": base_noisy_hist,
        "values": precomputed,
    }
    return result


def run_dp_pipeline() -> dict:
    try:
        # -----------------------------------
        # Auto-detect config and data
        # -----------------------------------
        config_path = _auto_detect_file("config", ".json")
        csv_path = _auto_detect_file("data", ".csv")

        # -----------------------------------
        # Load config
        # -----------------------------------
        raw_config = load_config(config_path)
        config = normalize_config(raw_config, data_dir="data")
        # For local single-run mode, keep explicit auto-detected CSV.
        config.setdefault("data", {})
        config["data"]["csv"] = csv_path

        result_payload = _execute_normalized_config(config)
        result_payload["config_used"] = config_path
        result_payload["csv_used"] = csv_path
        return result_payload

    except DPError as e:
        return {
            "status": "error",
            "error": e.to_dict()
        }

    except Exception as e:
        return {
            "status": "error",
            "error": {
                "code": "INTERNAL_ERROR",
                "message": str(e)
            }
        }


def run_dp_from_config(config: dict) -> dict:
    """
    Run DP pipeline from an already loaded config dict.
    If data.csv is missing, auto-detect a single CSV under data/.
    """
    try:
        normalized = normalize_config(config, data_dir="data")
        normalized.setdefault("data", {})
        if not normalized["data"].get("csv"):
            normalized["data"]["csv"] = _auto_detect_file("data", ".csv")
        return _execute_normalized_config(normalized)
    except DPError as e:
        return {
            "status": "error",
            "error": e.to_dict(),
        }
    except Exception as e:
        return {
            "status": "error",
            "error": {
                "code": "INTERNAL_ERROR",
                "message": str(e),
            },
        }


def _execute_normalized_config(normalized: dict) -> dict:
    """
    Execute either:
    1) single query config (legacy)
    2) multi-query config via differential_privacy.queries: [ ... ]
    """
    dp_cfg = normalized.get("differential_privacy", {})
    queries = dp_cfg.get("queries")
    if not isinstance(queries, list):
        result = route_dp_request(normalized)
        result = _attach_precomputed_noise_curve(result)
        return {
            "status": "success",
            "result": result,
        }

    if not queries:
        raise DPError("CONFIG_INVALID", "differential_privacy.queries must be non-empty")

    common_dp = {k: v for k, v in dp_cfg.items() if k not in {"queries", "query", "epsilon"}}
    query_results = []
    cumulative_epsilon_budget = 0.0
    any_error = False

    for idx, query_cfg in enumerate(queries, start=1):
        entry = {"index": idx}
        if not isinstance(query_cfg, dict):
            entry.update(
                {
                    "status": "error",
                    "error": {
                        "code": "CONFIG_INVALID",
                        "message": "Each queries[] entry must be an object",
                    },
                }
            )
            query_results.append(entry)
            any_error = True
            continue

        merged_dp = deepcopy(common_dp)
        merged_dp.update(query_cfg)

        query_name = str(merged_dp.get("name", merged_dp.get("query", f"query_{idx}")))
        entry["name"] = query_name
        entry["query"] = merged_dp.get("query")

        try:
            eps = float(merged_dp["epsilon"])
            if eps <= 0:
                raise ValueError("epsilon must be > 0")
            cumulative_epsilon_budget += eps
            entry["epsilon"] = eps
        except Exception:
            entry.update(
                {
                    "status": "error",
                    "error": {
                        "code": "CONFIG_INVALID",
                        "message": "Each queries[] entry must include numeric epsilon > 0",
                    },
                }
            )
            query_results.append(entry)
            any_error = True
            continue

        per_query_config = {
            "data": deepcopy(normalized.get("data", {})),
            "differential_privacy": merged_dp,
        }
        if "operations" in normalized:
            per_query_config["operations"] = deepcopy(normalized["operations"])
        if "data_type" in normalized:
            per_query_config["data_type"] = normalized["data_type"]

        try:
            result = route_dp_request(per_query_config)
            result = _attach_precomputed_noise_curve(result)
            entry.update({"status": "success", "result": result})
        except DPError as e:
            any_error = True
            entry.update({"status": "error", "error": e.to_dict()})
        except Exception as e:
            any_error = True
            entry.update(
                {
                    "status": "error",
                    "error": {"code": "INTERNAL_ERROR", "message": str(e)},
                }
            )
        query_results.append(entry)

    payload = {
        "multi_query": True,
        "query_results": query_results,
        "cumulative_epsilon_budget": cumulative_epsilon_budget,
    }
    if any_error:
        return {"status": "partial_error", "result": payload}
    return {"status": "success", "result": payload}


if __name__ == "__main__":
    output = run_dp_pipeline()
    print(output)
