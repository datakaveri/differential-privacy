from user_level_histogram_src.userDataHistogram import UserDataHistogram
from user_level_histogram_src.datasetHistogram import DatasetHistogram
from user_level_histogram_src.unclippedMechanismHistogram import UnclippedMechanismHistogram
from user_level_histogram_src.clippedMechanismHistogram import ClippedMechanismHistogram
import matplotlib.pyplot as plt
import pandas as pd

epsilons = [5]
num_runs = 1

output_file = "mse_results.txt"

open(output_file, "w").close()

# -------------------------------------------------------------------
# Load dataset ONCE
# -------------------------------------------------------------------

# Reading and Sorting Dataset as per User Contributions
df = pd.read_csv("synthetic_user_level_data.csv")
dataset = DatasetHistogram.from_dataframe(df)
dataset.sort_by_contribution()

# Defining parameters
attr = "systolic_bp"
U = 100
V = 150
k = 10
bin_width = 5

# Compute bins ONCE
bins = dataset.create_bins(k, bin_width, U, V)
sorted_bins = dataset.sort_to_bins(attr, bins, k, bin_width, U, V)
true_hist = dataset.count_bin(sorted_bins, k)   # Unclipped true histogram
m_star = dataset.compute_m_star()

# -------------------------------------------------------------------
# Loop over epsilon values
# -------------------------------------------------------------------
with open(output_file, "a") as file:

    file.write("=== MSE RESULTS FOR USER-LEVEL HISTOGRAM DP ===\n\n")

    for epsilon in epsilons:

        file.write(f"Running epsilon = {epsilon}\n")
        print(f"\n=== Running epsilon = {epsilon} ===")

        # Create fresh histogram objects for this epsilon
        unclipped_histogram = UnclippedMechanismHistogram(epsilon)
        clipped_histogram = ClippedMechanismHistogram(epsilon)

        # Compute C for this epsilon
        index = dataset.compute_idx(k, epsilon)
        print(f"index: {index}")

        C = dataset.compute_C(dataset, index)
        print(f"C: {C}")


        # -------------------------------------------------------------------
        # Build per-user histograms + clip first C users
        # -------------------------------------------------------------------
        user_histograms = []

        for idx, user in enumerate(dataset.users, start=1):
            m_i = len(user.records)
            #print(f"m_{idx}: {m_i}")
            user_counts = clipped_histogram.build_histogram_per_user(
                user, bins, attr, k, bin_width, U, V
            )

            if idx <= index:
                clipped = clipped_histogram.clip_histogram(C, m_i, user_counts, epsilon, k)
            else:
                clipped = user_counts

            user_histograms.append(clipped)

        # Merge into final clipped histogram
        clipped_hist = clipped_histogram.merge_clipped_user_histograms(user_histograms, k)

        unclipped_sens = unclipped_histogram.compute_unclipped_sensitivity_histogram(m_star)
        clipped_sens = clipped_histogram.compute_clipped_sensitivity_histogram(C)

        # Compute b parameters (Laplace scale)
        b_unclipped = unclipped_histogram.compute_unclipped_b_histogram(unclipped_sens)
        b_clipped = clipped_histogram.compute_clipped_b_histogram(clipped_sens)

        # Variance of Laplace = 2 * b^2
        var_unclipped = 2 * (b_unclipped ** 2)
        var_clipped = 2 * (b_clipped ** 2)

        # Log to file
        file.write(f"  2b^2 (unclipped): {var_unclipped}\n")
        file.write(f"  2b^2 (clipped):   {var_clipped}\n")

        print(f"2b^2 (unclipped variance): {var_unclipped}")
        print(f"2b^2 (clipped variance):   {var_clipped}")

        # -------------------------------------------------------------------
        # Now run DP noise addition 100,000 times
        # -------------------------------------------------------------------
        mse_values_unclipped = []
        mse_values_clipped = []

        for _ in range(num_runs):

            # Add noise
            noisy_hist_unclipped = unclipped_histogram.compute_unclipped_histogram(
                true_hist, k, unclipped_sens
            )
            noisy_hist_clipped = clipped_histogram.compute_clipped_histogram(
                clipped_hist, k, clipped_sens
            )

            # Compute MSE
            squared_errors_unclipped = [
                (true_hist[i] - noisy_hist_unclipped[i]) ** 2 for i in range(k)
            ]
            mse_unclipped = sum(squared_errors_unclipped) / k

            squared_errors_clipped = [
                (true_hist[i] - noisy_hist_clipped[i]) ** 2 for i in range(k)
            ]
            mse_clipped = sum(squared_errors_clipped) / k

            mse_values_unclipped.append(mse_unclipped)
            mse_values_clipped.append(mse_clipped)

        # Final MSE averages
        avg_unclipped = sum(mse_values_unclipped) / num_runs
        avg_clipped = sum(mse_values_clipped) / num_runs

        # Log results
        file.write(f"Epsilon: {epsilon}\n")
        file.write(f"  Unclipped Avg MSE: {avg_unclipped}\n")
        file.write(f"  Clipped Avg MSE:   {avg_clipped}\n\n")

        print(f"Unclipped Avg MSE: {avg_unclipped}")
        print(f"Clipped Avg MSE:   {avg_clipped}")

print("\nResults saved to mse_results.txt")


bin_centers = [(lower + upper) / 2 for (lower, upper, _) in bins]

plt.figure(figsize=(12, 6))

# Original histogram (solid)
plt.plot(bin_centers, true_hist, linestyle='-', linewidth=2, label='Original Histogram')

# Noisy histogram (dashed)
plt.plot(bin_centers, noisy_hist_unclipped, linestyle='--', linewidth=2, label='Unclipped Noisy Histogram')
plt.plot(bin_centers, noisy_hist_clipped, linestyle='--', linewidth=2, label='Clipped Noisy Histogram')

plt.xlabel("Attribute Value (systolic_bp)")
plt.ylabel("Count")
plt.title("Original vs DP-Noisy Histogram")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.5)

plt.tight_layout()
plt.show()