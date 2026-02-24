from __future__ import annotations

import os
import re
from copy import deepcopy

from DP.DPError import DPError


def _norm_token(value: str) -> str:
    token = re.sub(r"\s*\(\d+\)\s*$", "", str(value).strip().lower())
    token = re.sub(r"[^a-z0-9]+", "", token)
    return token


def _infer_csv_from_data_type(data_type: str, data_dir: str = "data") -> str:
    if not os.path.isdir(data_dir):
        raise DPError("PATH_NOT_FOUND", f"Directory not found: {data_dir}")

    csv_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".csv")]
    if not csv_files:
        raise DPError("FILE_NOT_FOUND", f"No .csv file found in {data_dir}")

    token = _norm_token(data_type)
    if token:
        exact = []
        partial = []
        for fname in csv_files:
            stem = os.path.splitext(fname)[0]
            stem_token = _norm_token(stem)
            if stem_token == token:
                exact.append(fname)
            elif token in stem_token or stem_token in token:
                partial.append(fname)

        if len(exact) == 1:
            return os.path.join(data_dir, exact[0])
        if len(exact) > 1:
            raise DPError(
                "AMBIGUOUS_INPUT",
                f"Multiple CSVs match data_type '{data_type}': {exact}",
            )
        if len(partial) == 1:
            return os.path.join(data_dir, partial[0])
        if len(partial) > 1:
            raise DPError(
                "AMBIGUOUS_INPUT",
                f"Multiple CSVs partially match data_type '{data_type}': {partial}",
            )

    if len(csv_files) == 1:
        return os.path.join(data_dir, csv_files[0])

    raise DPError(
        "AMBIGUOUS_INPUT",
        f"Unable to infer CSV for data_type '{data_type}'. Found: {csv_files}",
    )


def normalize_config(config: dict, data_dir: str = "data") -> dict:
    """
    Accept both config shapes:
    1) Native:
       {"data": {...}, "differential_privacy": {...}}
    2) UI payload:
       {"operations": [...], "data_type": "<name>", "<name>": {...}}
    Returns native shape for the DP router.
    """
    cfg = deepcopy(config)

    # Already native format.
    if "differential_privacy" in cfg:
        cfg.setdefault("data", {})
        if not cfg["data"].get("csv"):
            cfg["data"]["csv"] = _infer_csv_from_data_type(
                str(cfg.get("data_type", "")), data_dir=data_dir
            )
        return cfg

    data_type = cfg.get("data_type")
    if not data_type:
        raise DPError(
            "CONFIG_INVALID",
            "Missing 'differential_privacy' or UI fields 'data_type' + '<data_type>' block",
        )

    dp_payload = cfg.get(data_type)
    if not isinstance(dp_payload, dict):
        raise DPError(
            "CONFIG_INVALID",
            f"Missing dataset block '{data_type}'",
        )

    dp_cfg = deepcopy(dp_payload)
    # UI metadata not needed by pipeline.
    dp_cfg.pop("insensitive_columns", None)

    data_csv = None
    if isinstance(cfg.get("data"), dict):
        data_csv = cfg["data"].get("csv")
    if not data_csv:
        data_csv = dp_payload.get("csv") or dp_payload.get("csv_path")
    if not data_csv:
        data_csv = _infer_csv_from_data_type(str(data_type), data_dir=data_dir)

    return {
        "operations": cfg.get("operations", ["dp"]),
        "data_type": data_type,
        "data": {"csv": data_csv},
        "differential_privacy": dp_cfg,
    }

