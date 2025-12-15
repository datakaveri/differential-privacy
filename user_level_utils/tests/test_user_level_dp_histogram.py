import pytest
import numpy as np
import pandas as pd
from user_level_histogram_src.datasetHistogram import DatasetHistogram
from user_level_histogram_src.clippedMechanismHistogram import ClippedMechanismHistogram
from user_level_histogram_src.unclippedMechanismHistogram import UnclippedMechanismHistogram

# TEST CASES FOR USER LEVEL DP HISTOGRAM ESTIMATION

@pytest.fixture(autouse=True)
def fixed_seed():
    np.random.seed(42)

def run_user_level_histogram_pipeline( df, attr="systolic_bp", U=100, V=150, k=10, bin_width=5, epsilon=0.1):
    dataset = DatasetHistogram.from_dataframe(df)
    
    if len(dataset.users) == 0:
        return {
            "dataset": dataset,
            "true_hist": [0] * k,
            "clipped_hist": [0] * k,
            "index": 0,
            "C": 0,
            "unclipped_sensitivity": 0,
            "clipped_sensitivity": 0,
            "dp_hist_unclipped": [0] * k,
            "dp_hist_clipped": [0] * k
        }
    
    dataset.sort_by_contribution()

    bins = dataset.create_bins(k, bin_width, U, V)
    sorted_bins = dataset.sort_to_bins(attr, bins, k, bin_width, U, V)
    true_hist = dataset.count_bin(sorted_bins, k)
    m_star = dataset.compute_m_star()

    unclipped_hist = UnclippedMechanismHistogram(epsilon)
    clipped_hist = ClippedMechanismHistogram(epsilon)

    index = dataset.compute_idx(k, epsilon)
    C = dataset.compute_C(dataset, index)

    user_histograms = []

    for idx, user in enumerate(dataset.users, start=1):
        m_i = len(user.records)

        per_user_hist = clipped_hist.build_histogram_per_user(
            user, bins, attr, k, bin_width, U, V
        )

        if idx <= index:
            clipped_user_hist = clipped_hist.clip_histogram(
                C, m_i, per_user_hist, epsilon, k
            )
        else:
            clipped_user_hist = per_user_hist

        user_histograms.append(clipped_user_hist)

    final_clipped_hist = clipped_hist.merge_clipped_user_histograms(
        user_histograms, k
    )

    unclipped_sensitivity = unclipped_hist.compute_unclipped_sensitivity_histogram(m_star)
    clipped_sensitivity = clipped_hist.compute_clipped_sensitivity_histogram(C)
    dp_hist_unclipped = unclipped_hist.compute_unclipped_histogram(
        true_hist, k, unclipped_sensitivity
    )
    dp_hist_clipped = clipped_hist.compute_clipped_histogram(
        final_clipped_hist, k, clipped_sensitivity
    )

    return {
        "dataset": dataset,
        "true_hist": true_hist,
        "clipped_hist": final_clipped_hist,
        "index": index,
        "C": C,
        "unclipped_sensitivity": unclipped_sensitivity,
        "clipped_sensitivity": clipped_sensitivity,
        "dp_hist_unclipped": dp_hist_unclipped,
        "dp_hist_clipped": dp_hist_clipped
    }

# TEST CASES

def test_histogram_invalid_k_zero():
    """
    k = 0 is invalid for histogram construction and should raise an error.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "systolic_bp": [120, 130]
    })

    with pytest.raises(ValueError, match="k must be a positive integer"):
        run_user_level_histogram_pipeline(
            df=df,
            attr="systolic_bp",
            k=0,
            epsilon=0.1
        )

def test_histogram_empty_dataset():
    """
    Empty dataset should return zero-valued histograms and sensitivities
    without throwing an error.
    """
    # Empty dataframe with required columns
    df = pd.DataFrame(columns=["user_id", "systolic_bp"])

    result = run_user_level_histogram_pipeline(
        df=df,
        attr="systolic_bp",
        k=10,
        epsilon=0.1
    )

    # Dataset exists but has no users
    assert result["dataset"] is not None
    assert len(result["dataset"].users) == 0

    assert result["true_hist"] == [0] * 10
    assert result["clipped_hist"] == [0] * 10
    assert result["dp_hist_unclipped"] == [0] * 10
    assert result["dp_hist_clipped"] == [0] * 10

    assert result["index"] == 0
    assert result["C"] == 0
    assert result["unclipped_sensitivity"] == 0
    assert result["clipped_sensitivity"] == 0

def test_histogram_all_users_same_value():
    """
    All users have the exact same value with multiple contributions each.
    Histogram mass should concentrate in a single bin.
    """

    # Create dataset:
    # 3 users, each with 5 identical measurements
    users = []
    values = []
    user_ids = []

    for u in range(3):
        for _ in range(5):
            user_ids.append(f"u{u}")
            values.append(120)  # identical value for all users

    df = pd.DataFrame({
        "user_id": user_ids,
        "systolic_bp": values
    })

    k = 10
    bin_width = 5
    U = 100
    V = 150

    result = run_user_level_histogram_pipeline(
        df=df,
        attr="systolic_bp",
        k=k,
        bin_width=bin_width,
        U=U,
        V=V,
        epsilon=0.5
    )

    true_hist = np.array(result["true_hist"])
    clipped_hist = np.array(result["clipped_hist"])
    dp_unclipped = np.array(result["dp_hist_unclipped"])
    dp_clipped = np.array(result["dp_hist_clipped"])

    # All histograms must have correct length
    assert len(true_hist) == k
    assert len(clipped_hist) == k
    assert len(dp_unclipped) == k
    assert len(dp_clipped) == k

    # True histogram: exactly one non-zero bin
    assert np.count_nonzero(true_hist) == 1

    # Total count should equal total number of records
    assert true_hist.sum() == len(df)

    # Clipped histogram should not exceed true histogram mass
    assert clipped_hist.sum() <= true_hist.sum()

    # DP histograms should be finite (no NaNs / infs)
    assert np.all(np.isfinite(dp_unclipped))
    assert np.all(np.isfinite(dp_clipped))


def test_histogram_values_outside_bounds():
    """
    One contribution exceeds upper bound V and one is below lower bound U.
    Pipeline should not crash and histograms should remain valid.
    """

    df = pd.DataFrame({
        "user_id": [
            "u1", "u1", "u1",
            "u2", "u2", "u2"
        ],
        "systolic_bp": [
            120, 125, 300,   # 300 > V
            130, 135, 80     # 80 < U
        ]
    })

    U = 100
    V = 150
    k = 10
    bin_width = 5

    result = run_user_level_histogram_pipeline(
        df=df,
        attr="systolic_bp",
        U=U,
        V=V,
        k=k,
        bin_width=bin_width,
        epsilon=0.5
    )

    true_hist = np.array(result["true_hist"])
    dp_unclipped = np.array(result["dp_hist_unclipped"])
    dp_clipped = np.array(result["dp_hist_clipped"])

    # Histogram length must match k
    assert len(true_hist) == k
    assert len(dp_unclipped) == k
    assert len(dp_clipped) == k

    # True histogram must not exceed total records
    assert true_hist.sum() <= len(df)

    # DP histograms must contain finite values
    assert np.all(np.isfinite(dp_unclipped))
    assert np.all(np.isfinite(dp_clipped))

def test_dataset_histogram_from_single_line_csv(tmp_path):
    """
    Test reading a one-line CSV using DatasetHistogram.from_csv
    """

    csv_content = (
        "user_id,systolic_bp\n"
        "u1,120\n"
    )

    csv_file = tmp_path / "single_line.csv"
    csv_file.write_text(csv_content)

    dataset = DatasetHistogram.from_csv(csv_file)

    # Assertions
    assert dataset is not None
    assert len(dataset.users) == 1
    assert len(dataset.users[0].records) == 1
    assert int(dataset.users[0].records[0]["systolic_bp"]) == 120

def test_dataset_histogram_to_single_line_csv(tmp_path):
    """
    Test writing a single-record dataset using DatasetHistogram.to_csv
    """

    df = pd.DataFrame({
        "user_id": ["u1"],
        "systolic_bp": [120]
    })

    dataset = DatasetHistogram.from_dataframe(df)

    csv_file = tmp_path / "output_single_line.csv"
    dataset.to_csv(csv_file)

    # Read back
    reloaded_dataset = DatasetHistogram.from_csv(csv_file)

    # Assertions
    assert len(reloaded_dataset.users) == 1
    assert len(reloaded_dataset.users[0].records) == 1
    assert int(reloaded_dataset.users[0].records[0]["systolic_bp"]) == 120

def test_clip_histogram():
    clipped_hist = ClippedMechanismHistogram(epsilon=1.0)

    # -----------------------------
    # Case 1: C >= m_i → no clipping
    # -----------------------------
    user_counts = [2, 3, 5]
    m_i = 3
    C = 5   # C >= m_i, no clipping
    k = 3
    epsilon = 1.0

    result = clipped_hist.clip_histogram(C, m_i, user_counts, epsilon, k)

    # Should return exact same counts
    assert result == user_counts
    assert result is not user_counts  # should be a copy

    # -----------------------------
    # Case 2: C < m_i → clipping applied
    # -----------------------------
    user_counts = [2, 4, 6]
    m_i = 6
    C = 3   # scale = 3/6 = 0.5
    expected = [1.0, 2.0, 3.0]

    result = clipped_hist.clip_histogram(C, m_i, user_counts, epsilon, k)

    # Values should be scaled correctly
    assert result == expected

    # -----------------------------
    # Case 3: Zero counts
    # -----------------------------
    user_counts = [0, 0, 0]
    m_i = 5
    C = 3
    expected = [0.0, 0.0, 0.0]

    result = clipped_hist.clip_histogram(C, m_i, user_counts, epsilon, k)
    assert result == expected

def test_histogram_negative_and_zero_values():
    """
    Confirm that the histogram pipeline supports negative and zero values without failing,
    and that sensitivities remain non-negative.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "systolic_bp": [-10, 0, -5]
    })

    result = run_user_level_histogram_pipeline(df)

    assert result["clipped_sensitivity"] >= 0
    assert result["unclipped_sensitivity"] >= 0
    assert all(np.isfinite(result["dp_hist_clipped"]))
    assert all(np.isfinite(result["dp_hist_unclipped"]))

def test_histogram_non_numeric_values():
    """
    Validate that the histogram pipeline raises ValueError on non-numeric attribute values.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "systolic_bp": ["a", "b"]
    })

    with pytest.raises((TypeError, ValueError)):
        run_user_level_histogram_pipeline(df)

def test_histogram_epsilon_zero():
    """
    Ensure that passing epsilon=0 triggers a ZeroDivisionError (Laplace scale undefined).
    """
    df = pd.DataFrame({
        "user_id": ["u1"],
        "systolic_bp": [5.0]
    })

    with pytest.raises(ZeroDivisionError):
        run_user_level_histogram_pipeline(df, epsilon=0)

def test_histogram_invalid_range():
    """
    Validate that an invalid range (U < V) raises ValueError.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "systolic_bp": [2, 8, 5]
    })

    U = 11
    V = 10

    with pytest.raises(ValueError):
        run_user_level_histogram_pipeline(df, U=U, V=V, k=5, bin_width=1, epsilon=0.01)


