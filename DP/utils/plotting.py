import os
import json
import matplotlib.pyplot as plt


def plot_user_level_histogram(
    histogram,
    bin_labels=None,
    output_dir="output",
    title="User-Level DP Histogram",
    xlabel="Bin",
    ylabel="Noisy Count"
):
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 5))
    x = list(range(len(histogram)))
    plt.bar(x, histogram)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    if bin_labels:
        # Normalize labels for consistent rendering.
        labels = [str(lbl) for lbl in bin_labels]
        if len(labels) == len(x):
            plt.xticks(x, labels, rotation=45, ha="right")
        else:
            plt.xticks(x)
    else:
        plt.xticks(x)

    output_path = os.path.join(output_dir, "user_level_histogram.png")
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()

    return output_path


def plot_item_level_histogram(
    bin_labels,
    histogram,
    output_dir="output",
    title="Item-Level DP Histogram",
    xlabel="Category",
    ylabel="Noisy Count",
):
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(10, 5))
    x = list(range(len(histogram)))
    plt.bar(x, histogram)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(x, bin_labels, rotation=45, ha="right")

    output_path = os.path.join(output_dir, "item_level_histogram.png")
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()

    return output_path


def plot_item_histogram_from_output_json(
    output_json_path="output/results/item_level_histogram_output.json",
    output_dir="output",
):
    with open(output_json_path, "r") as f:
        payload = json.load(f)

    result = payload.get("result", {})
    labels = result.get("bin_labels")
    values = result.get("histogram")
    if not labels or not values:
        raise ValueError("Missing bin_labels/histogram in output JSON")

    return plot_item_level_histogram(
        labels,
        values,
        output_dir=output_dir,
        title=f"Item-Level DP Histogram ({result.get('attribute', 'attribute')})",
    )
