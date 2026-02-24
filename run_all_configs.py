import json
from pathlib import Path

from iudx_dp_main import main_process


def main() -> None:
    config_dir = Path("all_config")
    output_dir = Path("output/results")
    # Active configs for now: only mean + histogram (item/user).
    config_files = [
        config_dir / "item_level_mean.json",
        config_dir / "item_level_histogram.json",
        config_dir / "user_level_mean.json",
        config_dir / "user_level_histogram.json",
        # Temporarily disabled:
        # config_dir / "user_level_count.json",
        # config_dir / "user_level_count_distinct_users.json",
        # config_dir / "spatio_temporal_mean.json",
    ]
    config_files = [p for p in config_files if p.exists()]

    if not config_files:
        print("No config files found under all_config/")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    # Clear previous run artifacts so output folder reflects active configs only.
    for old in output_dir.glob("*_output.json"):
        old.unlink()

    print("Running DP for all configs:\n")
    for path in config_files:
        with open(path, "r") as f:
            cfg = json.load(f)

        result = main_process(cfg)
        status = result.get("status")
        payload = result.get("result", {})
        level = payload.get("privacy_level")
        query = payload.get("query")
        out_file = output_dir / f"{path.stem}_output.json"
        with open(out_file, "w") as f:
            json.dump(result, f, indent=2)

        print(
            f"{path.name}: status={status}, level={level}, query={query}, "
            f"saved={out_file}"
        )


if __name__ == "__main__":
    main()
