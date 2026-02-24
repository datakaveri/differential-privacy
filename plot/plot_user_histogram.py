import json

from DP.utils.plotting import plot_user_level_histogram


if __name__ == "__main__":
    with open("output/results/user_level_histogram_output.json", "r") as f:
        payload = json.load(f)

    result = payload.get("result", {})
    values = result.get("histogram")
    labels = result.get("bin_labels")
    if not values:
        raise ValueError("Missing histogram values in output JSON")
    out = plot_user_level_histogram(
        values,
        bin_labels=labels,
        title=f"User-Level DP Histogram ({result.get('attribute', 'attribute')})",
    )
    print(f"Histogram plot written to: {out}")
