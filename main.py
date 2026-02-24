import json
import sys
from pathlib import Path

from iudx_dp_main import main_process

if __name__ == "__main__":
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("all_config/user_level_mean.json")
    with config_path.open("r") as f:
        cfg = json.load(f)
    output = main_process(cfg)
    print(json.dumps(output, indent=2))
