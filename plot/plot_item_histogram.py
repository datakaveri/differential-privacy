from DP.utils.plotting import plot_item_histogram_from_output_json


if __name__ == "__main__":
    out = plot_item_histogram_from_output_json()
    print(f"Histogram plot written to: {out}")
