import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

from user_level_utils.data_generator import (
    generate_synthetic_data_variable_contrib,
    generate_synthetic_data_with_fixed_contrib,
    generate_binarized_dataset,
    generate_max_dataset,
    generate_min_dataset,
    generate_synthetic_data_one_contribution
)
from user_level_src.Dataset import Dataset
from user_level_src.Clipper import Clipper
from user_level_src.ClippedDPMechanism import ClippedDPMechanism
from user_level_src.UnclippedDPMechanism import UnclippedDPMechanism

# -------------------------------
# STEP 1: Generate base dataset
# -------------------------------
df, contribs = generate_synthetic_data_variable_contrib(total_users=200, total_records=10000, min_contrib=1, max_contrib=100,random_seed=42)

base_dataset = Dataset.from_dataframe(df)
base_dataset.sort_by_contribution()
contrib_counts = [user.num_records() for user in base_dataset.users]
user_ids = [user.user_id for user in base_dataset.users]
fixed_contrib_map = dict(zip(user_ids, contrib_counts))
print("Fixed contribution counts stored.")

#df, contribs = generate_synthetic_data_one_contribution(
    #total_users=200,
    #random_seed=42
#)

#single_contribution_dataset = Dataset.from_dataframe(df)
#single_contribution_dataset.sort_by_contribution()
#contrib_counts = [user.num_records() for user in single_contribution_dataset.users]
#user_ids = [user.user_id for user in single_contribution_dataset.users]
#fixed_contrib_map = dict(zip(user_ids, contrib_counts))


#binarized_df = generate_binarized_dataset(user_contributions=contrib_counts,base_attr_name="systolic_bp",random_seed=42)
#min_df = generate_min_dataset(user_contributions=contrib_counts, base_attr_name="systolic_bp", min_value=100, random_seed=None)
#max_df = generate_max_dataset(user_contributions=contrib_counts, base_attr_name="systolic_bp", max_value=150, random_seed=None)

#binarized_df = Dataset.from_dataframe(binarized_df)
#binarized_df.sort_by_contribution()

#min_df = Dataset.from_dataframe(min_df)
#min_df.sort_by_contribution()

#max_df = Dataset.from_dataframe(max_df)
#max_df.sort_by_contribution()

# Save to file
#binarized_df.to_csv("synthetic_user_level_data_binarized.csv")
#print("Binarized dataset saved as synthetic_user_level_data_binarized.csv")

#min_df.to_csv("min_synthetic_dataset.csv")
#print("Min Dataset saved")

#max_df.to_csv("max_synthetic_dataset.csv")
#print("Max Dataset saved")

min_contributors = min(contrib_counts)
max_contributors = max(contrib_counts)

print(f"Minimum contri: {min_contributors}")
print(f"Maximum contri: {max_contributors}")


# Compute once for terminal summary
attr_name = "systolic_bp"
all_values = [float(v) for user in base_dataset.users for v in user.get_attribute_values(attr_name)]
true_mean = np.mean(all_values)
U = 150
V = 100
num_users = len(base_dataset.users)
num_records = sum(user.num_records() for user in base_dataset.users)

#epsilon_value = 0.01

# Clipper and DP mechanisms
#clipper = Clipper()
#clipped_dp = ClippedDPMechanism(epsilon=epsilon_value)
#unclipped_dp = UnclippedDPMechanism(epsilon=epsilon_value)

# Compute T_epsilons
#index_i = base_dataset.compute_index_i(epsilon_value)
#T_eps_clipped = base_dataset.compute_T_epsilon_clipped(index_i, U, V, attr_name)
#T_eps_unclipped = base_dataset.compute_T_epsilon_unclipped(U, V, attr_name)

# Compute sensitivities
#sens_clipped = clipped_dp.compute_clipped_sensitivity(T_eps_clipped, base_dataset)
#sens_unclipped = unclipped_dp.compute_unclipped_sensitivity(T_eps_unclipped, base_dataset)
#b_clipped = clipped_dp.compute_clipped_b(sens_clipped)
#b_unclipped = unclipped_dp.compute_unclipped_b(sens_unclipped)

# Print terminal summary once
#print("\n=== Dataset Summary ===")
print(f"Number of users: {num_users}")
print(f"Number of records: {num_records}")
print(f"U (max value): {U}, V (min value): {V}")

#print(f"Sensitivity (clipped): {sens_clipped}")
#print(f"Sensitivity (unclipped): {sens_unclipped}")
#print(f"b (clipped): {b_clipped}")
#print(f"2b^2 (clipped): ", b_clipped * b_clipped * 2)
#print(f"b (unclipped): {b_unclipped}")
#print(f"2b^2 (unclipped): ", b_unclipped * b_unclipped * 2)


# -------------------------------
# STEP 2: Worker batch function
# -------------------------------
def run_single_epsilon_batch(epsilon, batch_index, contrib_counts, U, V, attr_name, runs_for_this_worker):
    results = []
    all_improvements = []

    # Generate dataset with same contributions
    df_new = generate_synthetic_data_with_fixed_contrib(contrib_counts, random_seed=42 + batch_index)
    #df_new = generate_binarized_dataset(user_contributions=contrib_counts, base_attr_name="systolic_bp", random_seed=42 + batch_index)
    #df_new = generate_min_dataset(user_contributions=contrib_counts, base_attr_name="systolic_bp", min_value=100, random_seed=None)
    #df_new = generate_max_dataset(user_contributions=contrib_counts, base_attr_name="systolic_bp", max_value=150, random_seed=None)
    #df_new, _ = generate_synthetic_data_one_contribution()

    dataset = Dataset.from_dataframe(df_new)
    dataset.sort_by_contribution()

    # DP mechanisms
    clipper = Clipper()
    clipped_dp = ClippedDPMechanism(epsilon=epsilon)
    unclipped_dp = UnclippedDPMechanism(epsilon=epsilon)

    # Compute T_epsilons
    index_i = dataset.compute_index_i(epsilon)
    T_eps_clipped = dataset.compute_T_epsilon_clipped(index_i, U, V, attr_name)
    T_eps_unclipped = dataset.compute_T_epsilon_unclipped(U, V, attr_name)

    # Compute sensitivities
    sens_clipped = clipped_dp.compute_clipped_sensitivity(T_eps_clipped, dataset)
    #print(f"Sensitivity clipped: {sens_clipped}")
    sens_unclipped = unclipped_dp.compute_unclipped_sensitivity(T_eps_unclipped, dataset)
    print(f"Sensitivity unclipped: {sens_unclipped}")
    b_clipped = clipped_dp.compute_clipped_b(sens_clipped)
    b_unclipped = unclipped_dp.compute_unclipped_b(sens_unclipped)

    two_b_sq_clipped = 2 * (b_clipped ** 2)
    two_b_sq_unclipped = 2 * (b_unclipped ** 2)

    # True mean
    all_values = [float(v) for user in dataset.users for v in user.get_attribute_values(attr_name)]
    true_mean = np.mean(all_values)

    # Computing N

    N = int(min((5*(10**4) * (two_b_sq_unclipped**2)), 10**6))
    print(f"Batch {batch_index}: N = {N}")

    # # === Compute epsilon for N = 1 ===
    # epsilon_for_one_run = (
    #     (5*(10**4) * (two_b_sq_clipped**2)))

    # print(f"Epsilon for N=1 (batch {batch_index}): {epsilon_for_one_run}")

    # epsilon_for_N1M = (
    #     (5*(10**4) * (two_b_sq_clipped**2)))

    # print(f"[Batch {batch_index}] epsilon for N=1e6   = {epsilon_for_N1M}")

    for run_num in range(1, runs_for_this_worker + 1):
        clipped_mean, _ = clipped_dp.release_clipped_dp_mean(dataset, attr_name, index_i, U, V, T_eps_clipped, clipper)
        unclipped_mean = unclipped_dp.release_dp_mean_unclipped(dataset, attr_name, T_eps_unclipped)

        error_c = true_mean - clipped_mean
        #print("clipped error: ", error_c)
        error_u = true_mean - unclipped_mean
        improvement = (error_u ** 2) - (error_c ** 2)

        Zi_clipped = unclipped_mean - true_mean

        results.append({
            "epsilon": epsilon,
            "batch": batch_index,
            "run_num": run_num,
            "true_mean": true_mean,
            "clipped_mean": clipped_mean,
            "unclipped_mean": unclipped_mean,
            "clipped_sens": sens_clipped,
            "unclipped_sens": sens_unclipped,
            "b_clipped": b_clipped,
            "two_b_sq_clipped": two_b_sq_clipped,
            "two_b_sq_unclipped": two_b_sq_unclipped,
            "error_sq_c": error_c ** 2,
            "error_sq_u": error_u ** 2,
            "improvement": improvement,
            "Zi_clipped": Zi_clipped,
        })
        all_improvements.append(improvement)

    return pd.DataFrame(results)

# -------------------------------
# STEP 3: Parallel execution (per ε)
# -------------------------------
if __name__ == "__main__":
    output_file = "dp_Zi_unclipped_results.txt"
    epsilons = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

    max_workers = 8

    # Init output
    with open(output_file, "w") as f:
        f.write(f"{'epsilon':>8} {'batch':>5} {'run_num':>7} "
                f"{'clipped_sens':>14} {'unclipped_sens':>14} {'b_clipped':>10} {'two_b_sq_clipped':>10} {'two_b_sq_unclipped':>10}"
                f"{'error_sq_c':>12} {'error_sq_u':>12} {'improvement':>12} {'Zi_clipped':>12}\n")
        f.write("-" * 65 + "\n")

    for eps_index, epsilon_value in enumerate(epsilons, start=1):
        print(f"\n=== Running for ε = {epsilon_value} ===")

        # ----------------------------------------
        # STEP 1: Run a probe batch to compute N
        # ----------------------------------------
        probe_df = run_single_epsilon_batch(
            epsilon_value, 
            batch_index=0, 
            contrib_counts=contrib_counts,
            U=U, V=V,
            attr_name=attr_name,
            runs_for_this_worker=1  # just a dummy value
        )

        # Read N from probe (every row has N_per_worker = 1, but printed value shows N)
        # So we manually recompute N using the same formula
        true_mean = probe_df["true_mean"].iloc[0]
        sens_unclipped = probe_df["unclipped_sens"].iloc[0]  # or whichever field holds sens_unclipped
        #two_b_sq_clipped = probe_df["two_b_sq_clipped"].iloc[0]
        two_b_sq_unclipped = probe_df["two_b_sq_unclipped"].iloc[0]

        N = int(min(
            (5*(10**4) * (two_b_sq_unclipped**2)), 10**6
        ))

        print(f"Global N for epsilon {epsilon_value}: {N}")

        # distribute N among workers
        if N == 0:
            print("N = 0, skipping this epsilon.")
            continue

        base_runs = N // max_workers
        remainder = N % max_workers

        runs_for_worker = [
            base_runs + (1 if i < remainder else 0)
            for i in range(max_workers)
        ]

        print("Runs per worker:", runs_for_worker)

        # ----------------------------------------
        # STEP 2: Run real batches in parallel
        # ----------------------------------------
        all_batches = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    run_single_epsilon_batch,
                    epsilon_value,
                    i + 1,
                    contrib_counts, U, V, attr_name,
                    runs_for_worker[i]
                )
                for i in range(max_workers)
                if runs_for_worker[i] > 0
            ]

            for future in as_completed(futures):
                all_batches.append(future.result())

        df_results = pd.concat(all_batches, ignore_index=True)

        # Summary
        df_runs = df_results[df_results["run_num"].apply(lambda x: isinstance(x, int))]
        avg_error_sq_c = df_runs["error_sq_c"].mean()
        avg_error_sq_u = df_runs["error_sq_u"].mean()
        avg_improvement = df_runs["improvement"].mean()

        # Write output
        with open(output_file, "a") as f:
            for _, row in df_runs.iterrows():
                f.write(f"{row['epsilon']:8.5f} {row['batch']:5} {row['run_num']:7} "
                        f"{row['clipped_sens']:>14} {row['unclipped_sens']:>14} {row['b_clipped']:>10} {row['two_b_sq_clipped']:>10} {row['two_b_sq_unclipped']:>10}"
                        f"{row['error_sq_c']:12.6f} {row['error_sq_u']:12.6f} {row['improvement']:12.6f} {row['Zi_clipped']:12.6f}\n")

            f.write(f"{epsilon_value:8.5f} {'ALL':>5} {'AVG':>7} "
                    f"{avg_error_sq_c:12.6f} {avg_error_sq_u:12.6f} {avg_improvement:12.6f}\n")
            f.write("-" * 65 + "\n")

        print(f"Finished epsilon {epsilon_value}!")
