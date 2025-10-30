import numpy as np
import pandas as pd
from datetime import datetime, timedelta


# ==============================================================
# Generate random user contribution distribution
# ==============================================================
def generate_random_user_contributions(
    num_users: int,
    total_records: int,
    min_records: int = 1,
    max_records: int = 100,
    random_seed: int = 42
) -> list:
    """
    Randomly assign each user a number of records, ensuring that:
    - Every user has at least 'min_records'
    - No user exceeds 'max_records'
    - Total sum of contributions = total_records
    """
    rng = np.random.default_rng(random_seed)
    contribs = np.full(num_users, min_records)
    remaining_records = total_records - contribs.sum()

    while remaining_records > 0:
        eligible = np.where(contribs < max_records)[0]
        if len(eligible) == 0:
            break
        i = rng.choice(eligible)
        contribs[i] += 1
        remaining_records -= 1

    return contribs.tolist()

def generate_user_contributions_single(num_users: int) -> list:
    """
    Each user contributes exactly one record.
    """
    return [1 for _ in range(num_users)]

def generate_synthetic_data_one_contribution(
    total_users: int = 1000,
    income_mean_log: float = 13.1,
    income_sigma_log: float = 0.8,
    income_min: int = 50000,
    income_max: int = 2000000,
    bp_mean: float = 120,
    bp_std: float = 15,
    bp_min: int = 100,
    bp_max: int = 150,
    start_date: str = "2023-01-01",
    end_date: str = "2025-09-29",
    random_seed: int = 42
):
    """
    Generates a synthetic dataset with one contribution per user.
    Returns:
        df (pd.DataFrame): synthetic dataset
        user_contributions (list[int]): per-user record counts
    """
    rng = np.random.default_rng(random_seed)

    # Step 0: generate contribution counts
    user_contributions = generate_user_contributions_single(
        total_users
    )

    # Step 1: create per-user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # Step 2: expand by contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)
    total_records = len(repeated_users)

    # Step 3: generate sensitive columns
    df = _generate_random_records(
        repeated_users,
        income_mean_log,
        income_sigma_log,
        income_min,
        income_max,
        bp_mean,
        bp_std,
        bp_min,
        bp_max,
        start_date,
        end_date,
        rng
    )

    # Merge user attributes
    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df, user_contributions


# ==============================================================
# Generate synthetic dataset (with variable contributions)
# ==============================================================
def generate_synthetic_data_variable_contrib(
    total_users: int = 1000,
    total_records: int = 100000,
    min_contrib: int = 1,
    max_contrib: int = 100,
    income_mean_log: float = 13.1,
    income_sigma_log: float = 0.8,
    income_min: int = 50000,
    income_max: int = 2000000,
    bp_mean: float = 120,
    bp_std: float = 15,
    bp_min: int = 100,
    bp_max: int = 150,
    start_date: str = "2023-01-01",
    end_date: str = "2025-09-29",
    random_seed: int = 42
):
    """
    Generates a synthetic dataset with variable user contributions.
    Returns:
        df (pd.DataFrame): synthetic dataset
        user_contributions (list[int]): per-user record counts
    """
    rng = np.random.default_rng(random_seed)

    # Step 0: generate contribution counts
    user_contributions = generate_random_user_contributions(
        total_users, total_records, min_contrib, max_contrib, random_seed
    )

    # Step 1: create per-user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # Step 2: expand by contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)
    total_records = len(repeated_users)

    # Step 3: generate sensitive columns
    df = _generate_random_records(
        repeated_users,
        income_mean_log,
        income_sigma_log,
        income_min,
        income_max,
        bp_mean,
        bp_std,
        bp_min,
        bp_max,
        start_date,
        end_date,
        rng
    )

    # Merge user attributes
    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df, user_contributions


# ==============================================================
# Generate new dataset using fixed user contributions
# ==============================================================
def generate_synthetic_data_with_fixed_contrib(
    user_contributions: list,
    income_mean_log: float = 13.1,
    income_sigma_log: float = 0.8,
    income_min: int = 50000,
    income_max: int = 2000000,
    bp_mean: float = 120,
    bp_std: float = 15,
    bp_min: int = 100,
    bp_max: int = 150,
    start_date: str = "2023-01-01",
    end_date: str = "2025-09-29",
    random_seed: int = None
):
    """
    Generates a new synthetic dataset where each user has the same
    number of contributions as in `user_contributions`, but with
    freshly randomized values for income, BP, and timestamps.
    """
    rng = np.random.default_rng(random_seed)
    total_users = len(user_contributions)

    # per-user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # expand user contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)

    df = _generate_random_records(
        repeated_users,
        income_mean_log,
        income_sigma_log,
        income_min,
        income_max,
        bp_mean,
        bp_std,
        bp_min,
        bp_max,
        start_date,
        end_date,
        rng
    )

    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df

def generate_binarized_dataset(user_contributions, base_attr_name="systolic_bp", random_seed=None):
    """
    Generates a dataset with the same user contributions as `user_contributions`,
    but with the specified attribute binarized to either min or max value.
    Other attributes can remain random.
    
    Returns a pandas DataFrame with the same structure as the synthetic dataset.
    """
    rng = np.random.default_rng(random_seed)
    total_users = len(user_contributions)

    # Generate basic user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # Expand users by their contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)
    total_records = len(repeated_users)

    # Generate other columns randomly (income, timestamps, etc.)
    df = _generate_random_records(
        repeated_users,
        income_mean_log=13.1,
        income_sigma_log=0.8,
        income_min=50000,
        income_max=2000000,
        bp_mean=0,  # bp_mean not used because we overwrite
        bp_std=1,   # bp_std not used
        bp_min=100, # placeholder
        bp_max=150, # placeholder
        start_date="2023-01-01",
        end_date="2025-09-29",
        rng=rng
    )

    # Binarize the chosen attribute
    df[base_attr_name] = rng.choice([100, 150], size=total_records)

    # Merge user metadata
    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df

def generate_min_dataset(user_contributions, base_attr_name="systolic_bp", min_value=100, random_seed=None):
    """
    Generates a dataset with the same user contributions as `user_contributions`,
    but sets the specified attribute to the minimum value for all records.
    Other attributes can remain random.

    Returns a pandas DataFrame.
    """
    rng = np.random.default_rng(random_seed)
    total_users = len(user_contributions)

    # Generate basic user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # Expand users by their contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)
    total_records = len(repeated_users)

    # Generate other columns randomly (income, timestamps, etc.)
    df = _generate_random_records(
        repeated_users,
        income_mean_log=13.1,
        income_sigma_log=0.8,
        income_min=50000,
        income_max=2000000,
        bp_mean=0,  # placeholder
        bp_std=1,   # placeholder
        bp_min=min_value,
        bp_max=min_value,
        start_date="2023-01-01",
        end_date="2025-09-29",
        rng=rng
    )

    # Set the chosen attribute to the minimum value for all records
    df[base_attr_name] = min_value

    # Merge user metadata
    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df

def generate_max_dataset(user_contributions, base_attr_name="systolic_bp", max_value=150, random_seed=None):
    """
    Generates a dataset with the same user contributions as `user_contributions`,
    but sets the specified attribute to the maximum value for all records.
    Other attributes can remain random.

    Returns a pandas DataFrame.
    """
    rng = np.random.default_rng(random_seed)
    total_users = len(user_contributions)

    # Generate basic user metadata
    user_ids = np.arange(1, total_users + 1)
    names = [f"User{i}" for i in user_ids]
    ages = rng.normal(40, 12, size=total_users).clip(18, 70).astype(int)
    user_df = pd.DataFrame({"user_id": user_ids, "name": names, "age": ages})

    # Expand users by their contributions
    repeated_users = np.repeat(user_df["user_id"].values, user_contributions)
    total_records = len(repeated_users)

    # Generate other columns randomly (income, timestamps, etc.)
    df = _generate_random_records(
        repeated_users,
        income_mean_log=13.1,
        income_sigma_log=0.8,
        income_min=50000,
        income_max=2000000,
        bp_mean=0,  # placeholder
        bp_std=1,   # placeholder
        bp_min=max_value,
        bp_max=max_value,
        start_date="2023-01-01",
        end_date="2025-09-29",
        rng=rng
    )

    # Set the chosen attribute to the maximum value for all records
    df[base_attr_name] = max_value

    # Merge user metadata
    df = df.merge(user_df, on="user_id", how="left")
    df = df[["user_id", "name", "age", "annual_income", "systolic_bp", "timestamp"]]

    return df


# ==============================================================
# Helper: Random record generator (used by both functions)
# ==============================================================
def _generate_random_records(
    repeated_users,
    income_mean_log,
    income_sigma_log,
    income_min,
    income_max,
    bp_mean,
    bp_std,
    bp_min,
    bp_max,
    start_date,
    end_date,
    rng
):
    """Helper for generating income, BP, and timestamps."""
    total_records = len(repeated_users)

    annual_income = rng.lognormal(mean=income_mean_log, sigma=income_sigma_log, size=total_records)
    annual_income = np.clip(annual_income, income_min, income_max).astype(int)

    systolic_bp = rng.normal(bp_mean, bp_std, size=total_records)
    systolic_bp = np.clip(systolic_bp, bp_min, bp_max).astype(int)

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    total_seconds = int((end_dt - start_dt).total_seconds())
    timestamps = [
        start_dt + timedelta(seconds=int(rng.integers(total_seconds)))
        for _ in range(total_records)
    ]

    return pd.DataFrame({
        "user_id": repeated_users,
        "annual_income": annual_income,
        "systolic_bp": systolic_bp,
        "timestamp": timestamps
    })


if __name__ == "__main__":
    # Step 1: Generate base dataset
    df, contribs = generate_synthetic_data_variable_contrib(
        total_users=200,
        total_records=10000,
        min_contrib=1,
        max_contrib=100
    )
    df.to_csv("synthetic_user_level_data.csv", index=False)
    print("✅ Base dataset saved as synthetic_user_level_data.csv")
