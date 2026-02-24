import argparse
import json
from pathlib import Path

import pandas as pd


def _infer_config_path_from_output(output_json_path: Path) -> Path:
    stem = output_json_path.stem
    if stem.endswith("_output"):
        stem = stem[: -len("_output")]
    return Path("config") / f"{stem}.json"


def _compute_true_mean_and_sensitivity(config: dict, result: dict):
    dataset = config["data_type"]
    dp = config[dataset]
    attr = dp["attribute"]
    csv_path = "data/" +config["data_type"] + ".csv"
    df = pd.read_csv(csv_path)

    true_mean = float(df[attr].astype(float).mean())
    epsilon = float(dp["epsilon"])

    # User-level mean returns sensitivity directly.
    if "sensitivity" in result:
        sensitivity = float(result["sensitivity"])
        note = "user-level sensitivity from output"
        return true_mean, sensitivity, epsilon, note

    # Item-level mean in current code uses noisy sum/noisy count.
    # For Laplace visualization, use mean sensitivity approximation:
    # (max_value - min_value) / n
    max_v = float(dp["max_value"])
    min_v = float(dp.get("min_value", 0.0))
    n = max(1, len(df))
    sensitivity = (max_v - min_v) / n
    note = "item-level mean sensitivity approximated as (max-min)/n for visualization"
    return true_mean, sensitivity, epsilon, note


def generate_mean_laplace_slider_html(output_json_path: str):
    output_json = Path(output_json_path)
    payload = json.loads(output_json.read_text())
    if payload.get("status") != "success":
        raise ValueError(f"Output JSON status is not success: {output_json_path}")

    result = payload["result"]
    dp_mean = float(result["dp_mean"])

    cfg_path = _infer_config_path_from_output(output_json)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Inferred config not found: {cfg_path}")
    config = json.loads(cfg_path.read_text())

    true_mean, sensitivity, epsilon0, note = _compute_true_mean_and_sensitivity(config, result)
    b0 = max(1e-12, sensitivity / epsilon0)
    z0 = (dp_mean - true_mean) / b0

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / f"{output_json.stem}_laplace_slider.html"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Mean Laplace Slider</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    #panel {{ margin-bottom: 10px; }}
    #eps {{ width: 420px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
  </style>
</head>
<body>
  <h3>Laplace Distribution Around Mean</h3>
  <div id="panel">
    <div><b>Source:</b> <span class="mono">{output_json_path}</span></div>
    <div><b>True mean:</b> <span id="trueMean"></span></div>
    <div><b>Noisy mean (from output JSON):</b> <span id="fixedNoisy"></span></div>
    <div><b>Live noisy mean (slider):</b> <span id="liveNoisy"></span></div>
    <div><b>Sensitivity:</b> <span id="sens"></span></div>
    <div><b>Note:</b> {note}</div>
    <label><b>Epsilon:</b> <span id="epsVal"></span></label><br/>
    <input id="eps" type="range" min="0.1" max="10" value="{epsilon0}" step="0.1" />
  </div>
  <div id="plot" style="width:100%;height:520px;"></div>

  <script>
    const trueMean = {true_mean};
    const fixedNoisyMean = {dp_mean};
    const sensitivity = {sensitivity};
    const z0 = {z0};
    const eps0 = {epsilon0};

    const trueEl = document.getElementById("trueMean");
    const fixedEl = document.getElementById("fixedNoisy");
    const liveEl = document.getElementById("liveNoisy");
    const sensEl = document.getElementById("sens");
    const epsValEl = document.getElementById("epsVal");
    const slider = document.getElementById("eps");

    trueEl.textContent = trueMean.toFixed(6);
    fixedEl.textContent = fixedNoisyMean.toFixed(6);
    sensEl.textContent = sensitivity.toFixed(8);

    function laplacePdf(x, mu, b) {{
      return Math.exp(-Math.abs(x - mu) / b) / (2 * b);
    }}

    function posToEpsilon(pos) {{
      // Piecewise slider:
      // pos 0..10    => epsilon 0..0.1 in 0.01 steps
      // pos 11..19   => epsilon 0.2..1.0 in 0.1 steps
      // pos 20..119  => epsilon 1..100 in 1.0 steps
      if (pos <= 10) return pos / 100.0;
      if (pos <= 19) return (pos - 9) / 10.0;
      return pos - 19;
    }}

    function epsilonToPos(epsilon) {{
      if (epsilon <= 0.1) return Math.round(epsilon * 100);
      if (epsilon <= 1.0) return Math.round(9 + epsilon * 10);
      return Math.round(19 + epsilon);
    }}

    function buildSeries(epsilon) {{
      const epsEff = Math.max(1e-6, epsilon);
      const b = Math.max(1e-12, sensitivity / epsEff);
      const span = Math.max(1, 8 * b);
      const x0 = trueMean - span;
      const x1 = trueMean + span;
      const n = 600;
      const xs = [];
      const ys = [];
      for (let i = 0; i < n; i++) {{
        const x = x0 + (x1 - x0) * i / (n - 1);
        xs.push(x);
        ys.push(laplacePdf(x, trueMean, b));
      }}
      const liveNoisy = trueMean + z0 * b;
      return {{ xs, ys, b, liveNoisy }};
    }}

    function render(epsilon) {{
      const s = buildSeries(epsilon);
      epsValEl.textContent = Number(epsilon).toFixed(2);
      liveEl.textContent = s.liveNoisy.toFixed(6);
      const yTop = laplacePdf(trueMean, trueMean, s.b);

      const tracePdf = {{
        x: s.xs,
        y: s.ys,
        mode: "lines",
        name: "Laplace PDF",
        line: {{ width: 2 }}
      }};

      const traceTrue = {{
        x: [trueMean, trueMean],
        y: [0, yTop],
        mode: "lines",
        name: "True mean",
        line: {{ width: 3, color: "green" }}
      }};

      const traceFixed = {{
        x: [fixedNoisyMean, fixedNoisyMean],
        y: [0, yTop],
        mode: "lines",
        name: "Noisy mean (output)",
        line: {{ width: 3, color: "red" }}
      }};

      const traceLive = {{
        x: [s.liveNoisy, s.liveNoisy],
        y: [0, yTop],
        mode: "lines",
        name: "Noisy mean (live)",
        line: {{ width: 2, color: "orange", dash: "dot" }}
      }};

      const layout = {{
        title: "Laplace PDF with True and Noisy Means",
        xaxis: {{ title: "Mean value" }},
        yaxis: {{ title: "Density" }},
        margin: {{ t: 40, r: 20, b: 50, l: 60 }},
        legend: {{ orientation: "h" }}
      }};

      Plotly.newPlot("plot", [tracePdf, traceTrue, traceFixed, traceLive], layout, {{
        responsive: true
      }});
    }}

    slider.min = "0";
    slider.max = "119";
    slider.step = "1";
    slider.value = String(epsilonToPos(eps0));

    slider.addEventListener("input", (e) => {{
      const pos = parseInt(e.target.value, 10);
      const eps = posToEpsilon(pos);
      render(eps);
    }});
    render(posToEpsilon(parseInt(slider.value, 10)));
  </script>
</body>
</html>
"""
    out_html.write_text(html)
    return str(out_html)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-json",
        default="output/user_level_mean_output.json",
        help="Path to mean output JSON file",
    )
    args = parser.parse_args()

    out = generate_mean_laplace_slider_html(args.output_json)
    print(f"Laplace slider plot written to: {out}")
