# Differential Privacy Engine

This repository runs Differential Privacy (DP) queries from JSON configs over CSV data.

Supported dimensions:
- DP level: `item`, `user`
- Queries: `mean`, `histogram`, `count`
- Config modes: single-query and multi-query

## Project Layout
- `DP/`: core DP routing, pipelines, queries, and utilities
- `config/`: runtime configs consumed by `run_from_config_dir.py`
- `all_config/`: sample configs for manual testing
- `data/`: CSV datasets
- `output/`: runtime outputs (`status.json`, plots, JSON artifacts)

## Setup
```bash
cd differential-privacy
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Modes

### 1. Run all configs from `config/`
```bash
python3 run_from_config_dir.py
```

Output is written to:
- `output/status.json`

### 2. Run sample matrix from `all_config/`
```bash
python3 run_all_configs.py
```

Outputs are written to:
- `output/results/*_output.json`

### 3. Run a single config programmatically
```bash
python3 - <<'PY'
import json
from iudx_dp_main import main_process

with open("config/multi_query.json", "r") as f:
    cfg = json.load(f)

print(main_process(cfg))
PY
```

## Config Format (UI-Wrapped)
The preferred config format is:

```json
{
  "operations": ["dp"],
  "data_type": "dp_test_dataset",
  "dp_test_dataset": {
    "level": "user",
    "query": "mean",
    "attribute": "age",
    "epsilon": 1.0,
    "user_column": "user_id"
  }
}
```

Notes:
- `data_type` is used to resolve a CSV in `data/` when `data.csv` is not provided.
- The dataset key must match `data_type` exactly.
- `insensitive_columns` is optional metadata.

## Multi-Query Config
Use `queries` for multiple query executions in one request, each with its own epsilon:

```json
{
  "operations": ["dp"],
  "data_type": "dp_test_dataset",
  "dp_test_dataset": {
    "level": "user",
    "user_column": "user_id",
    "queries": [
      {
        "query": "mean",
        "attribute": "age",
        "epsilon": 0.7,
        "min_value": 0,
        "max_value": 100
      },
      {
        "query": "count",
        "count_attribute": "age",
        "count_operator": ">",
        "count_value": 25,
        "epsilon": 0.3
      }
    ]
  }
}
```

Result includes:
- `query_results` (per query)
- `cumulative_epsilon_budget` (sum of selected query epsilons)

Recommended: keep this JSON as a local runtime file under `config/` (for example `config/multi_query.json`) and do not commit it.

## Query Semantics

### Mean
- Item level: noisy mean over records (with clipping bounds).
- User level: user-contribution-aware mean with clipping.

### Histogram
- Categorical mode: bins from categories or unique values.
- Numeric mode: bins from `U/V` and `bin_width` or `bins`.

### Count
- Item level: count of records that satisfy predicate filter.
- User level: count of distinct users with at least one matching contribution.
- `dp_count` is rounded to an integer.

Predicate fields:
- `count_attribute`
- `count_operator`: `>`, `>=`, `<`, `<=`, `==`, `!=`
- `count_value`

## Output Contract
Top-level result shape:
- Success:
  - `{"status":"success","result":{...}}`
- Partial multi-query failure:
  - `{"status":"partial_error","result":{"query_results":[...], ...}}`
- Error:
  - `{"status":"error","error":{"code":"...","message":"..."}}`

`run_from_config_dir.py` aggregates all config runs into `output/status.json` and adds top-level `cumulative_epsilon_budget`.

## Tests
```bash
python3 -m unittest discover -s tests -v
```

## Docker
```bash
docker build -t dp-app .
docker run --rm \
  -v "$(pwd)/config:/app/config" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  dp-app
```
