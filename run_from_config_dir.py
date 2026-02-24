import json
from pathlib import Path

from iudx_dp_main import main_process


def main():
    config_dir = Path("config")
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"

    config_files = sorted(config_dir.glob("*.json"))
    if not config_files:
        raise FileNotFoundError("No config JSON files found in config/")

    status_payload = {"status": "success", "results": {}, "cumulative_epsilon_budget": 0.0}

    for cfg_path in config_files:
        with cfg_path.open("r") as f:
            cfg = json.load(f)

        result = main_process(cfg)
        status_payload["results"][cfg_path.name] = result
        if result.get("status") != "success":
            status_payload["status"] = "partial_error"
        per_result = result.get("result", {})
        if isinstance(per_result, dict):
            if "cumulative_epsilon_budget" in per_result:
                try:
                    status_payload["cumulative_epsilon_budget"] += float(
                        per_result["cumulative_epsilon_budget"]
                    )
                except Exception:
                    pass
            elif "epsilon" in per_result:
                try:
                    status_payload["cumulative_epsilon_budget"] += float(per_result["epsilon"])
                except Exception:
                    pass
        print(f"{cfg_path.name} -> recorded in {status_path}")

    with status_path.open("w") as f:
        json.dump(status_payload, f, indent=2)


if __name__ == "__main__":
    main()
