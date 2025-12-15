from user_level_src.Dataset import Dataset
from user_level_src.Clipper import Clipper
from user_level_src.ClippedDPMechanism import ClippedDPMechanism
from user_level_src.UnclippedDPMechanism import UnclippedDPMechanism
import pytest
import numpy as np
import pandas as pd

# TEST CASES FOR USER LEVEL DP MEAN ESTIMATION

@pytest.fixture(autouse=True)
def fixed_seed():
    np.random.seed(42)

def run_user_level_dp_pipeline(df, attribute="attr", epsilon=0.1, U=10, V=0):
    dataset = Dataset.from_dataframe(df)
    if len(dataset.users) == 0:
        return {
            "sens_clipped": 0,
            "sens_unclipped": 0,
            "dp_mean_clipped": 0.0,
            "dp_mean_unclipped": 0.0,
            "dataset": dataset
        }
    dataset.sort_by_contribution()
   
    true_mean = float(dataset.users[0].records[0][attribute])

    # Compute T_epsilon clipped/unclipped
    index_i = dataset.compute_index_i(epsilon)
    T_clipped = dataset.compute_T_epsilon_clipped(index_i, U, V, attribute)
    T_unclipped = dataset.compute_T_epsilon_unclipped(U, V, attribute)

    # DP mechanisms
    clipper = Clipper()
    clipped_dp = ClippedDPMechanism(epsilon=epsilon)
    unclipped_dp = UnclippedDPMechanism(epsilon=epsilon)

    # Clipper Function

    clipped_mean = clipper.clipped_mean(
        dataset=dataset,
        attr_name=attribute,
        index_i=index_i,
        U=U,
        V=V,
        T_epsilon=T_clipped
    )

    # Sensitivity
    sens_clipped = clipped_dp.compute_clipped_sensitivity(T_clipped, dataset)
    sens_unclipped = unclipped_dp.compute_unclipped_sensitivity(T_unclipped, dataset)

    # Add noise
    dp_mean_clipped = true_mean + clipped_dp.add_laplace_noise_clipped(sens_clipped)
    dp_mean_unclipped = true_mean + unclipped_dp.add_laplace_noise_unclipped(sens_unclipped)

    return {
        "dataset": dataset,
        "true_mean": true_mean,
        "index_i": index_i,
        "clipped_mean": clipped_mean,
        "T_clipped": T_clipped,
        "T_unclipped": T_unclipped,
        "sens_clipped": sens_clipped,
        "sens_unclipped": sens_unclipped,
        "dp_mean_clipped": dp_mean_clipped,
        "dp_mean_unclipped": dp_mean_unclipped
    }


def test_single_record():
    """
    Verify that the DP pipeline works with a dataset containing only a single user and a single contribution, 
    and that all returned DP values are non-negative and of the correct type.
    """
    df = pd.DataFrame({"user_id": ["u1"], "attr": [5.0]})
    result = run_user_level_dp_pipeline(df)

    assert result["sens_clipped"] >= 0
    assert result["sens_unclipped"] >= 0
    assert isinstance(result["dp_mean_clipped"], float)
    assert isinstance(result["dp_mean_unclipped"], float)

def test_empty_dataset():
    """
    Ensure the pipeline correctly handles an empty dataset by returning zero sensitivities and zero DP means without errors.
    """
    df = pd.DataFrame(columns=["user_id", "attr"])  # no records
    result = run_user_level_dp_pipeline(df)

    # Sensitivities should be zero
    assert result["sens_clipped"] == 0
    assert result["sens_unclipped"] == 0

    assert result["dp_mean_clipped"] == 0
    assert result["dp_mean_unclipped"] == 0

def test_negative_and_zero_values():
    """
    Confirm that the pipeline supports datasets with negative and zero values without failing, 
    and that sensitivities remain non-negative.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "attr": [-10, 0, -5]
    })
    result = run_user_level_dp_pipeline(df)

    assert result["sens_clipped"] >= 0
    assert result["sens_unclipped"] >= 0
    assert isinstance(result["dp_mean_clipped"], float)

def test_identical_values():
    """
    Check that when all users have identical values, 
    the clipped and unclipped sensitivities must match (since the range is zero).
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "attr": [2, 2, 2]
    })
    result = run_user_level_dp_pipeline(df)

    # Sensitivity should be zero (all values identical)
    assert result["sens_clipped"] == result["sens_unclipped"]

def test_non_numeric_values():
    """
    Validate that the pipeline raises an appropriate error (ValueError) when encountering non-numeric attribute values.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "attr": ["a", "b"]  # invalid
    })
    with pytest.raises(ValueError):
        run_user_level_dp_pipeline(df)

def test_epsilon_zero():
    """
    Ensure that passing ε = 0 (undefined for Laplace noise) triggers a ZeroDivisionError rather than producing invalid output.
    """
    df = pd.DataFrame({"user_id": ["u1"], "attr": [5.0]})
    
    with pytest.raises(ZeroDivisionError): 
        run_user_level_dp_pipeline(df, epsilon=0)

def test_high_epsilon():
    """
    Test that with very large ε (low noise), 
    the DP mean stays very close to the true mean for both clipped and unclipped mechanisms.
    """
    df = pd.DataFrame({"user_id": ["u1", "u2", "u3"],
        "attr": [2, 8, 5]})
    
    epsilon = 100
    result = run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U=10, V=0)

    true_mean = result["true_mean"]
    dp_mean_clipped = result["dp_mean_clipped"]
    dp_mean_unclipped = result["dp_mean_unclipped"]

    assert abs(dp_mean_clipped - true_mean) < 0.1
    assert abs(dp_mean_unclipped - true_mean) < 0.1

def test_min_index_i():
    """
    Verify correct computation of index_i and ensure that for sufficiently large ε, 
    clipped and unclipped T-epsilon values coincide.
    """
    df = pd.DataFrame({"user_id": ["u1", "u2", "u3"],
        "attr": [2, 8, 5]})
    
    epsilon = 2
    result = run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U=10, V=0)

    dataset = result["dataset"]
    index_i = dataset.compute_index_i(epsilon)
    assert index_i == 1

    T_epsilon_clipped = dataset.compute_T_epsilon_clipped(index_i, 0, 10, "attr")
    T_epsilon_unclipped = dataset.compute_T_epsilon_unclipped(0, 10, "attr")

    assert T_epsilon_clipped == T_epsilon_unclipped

def test_equal_bounds():
    """
    Check the behavior when U = V, which forces the attribute range to zero.
    T-epsilon and sensitivity should both be zero.
    """
    df = pd.DataFrame({"user_id": ["u1", "u2", "u3"],
        "attr": [2, 8, 5]})
    
    epsilon = 0.01
    result = run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U = 10, V=10)

    assert result["T_clipped"] == 0
    assert result["T_unclipped"] == 0
    assert result["sens_clipped"] == 0
    assert result["sens_unclipped"] == 0

def test_negative_range():
    """
    Validate that passing an invalid range (U < V) results in a ValueError rather than silent incorrect behavior.
    """
    df = pd.DataFrame({"user_id": ["u1", "u2", "u3"],
        "attr": [2, 8, 5]})
    
    epsilon = 0.01
    U = 0
    V = 10  

    with pytest.raises(ValueError):
        run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U=U, V=V)

def test_equal_contributions_all_users():
    """
    Ensure that when all users have equal number of records, 
    clipped and unclipped mechanisms compute identical T-epsilon and sensitivity values.
    """
    df = pd.DataFrame({
        "user_id": ["u1", "u1", "u2", "u2", "u3", "u3"],
        "attr": [5, 7, 4, 6, 9, 3]
    })

    result = run_user_level_dp_pipeline(df)

    assert result["T_clipped"] == result["T_unclipped"]
    assert result["sens_clipped"] == result["sens_unclipped"]

def test_all_users_clipped():
    """
    Test a scenario where all users fall before index_i, forcing clipping for every user, 
    and verify that clipped and unclipped sensitivities differ.
    """
    df = pd.DataFrame({
        "user_id": [
            "u1", "u1",                 # 2 records
            "u2", "u2", "u2", "u2",     # 4 records
            "u3", "u3", "u3",           # 3 records
        ],
        "attr": [
            0, 10,      # values between 0–10
            3, 5, 7, 9,
            2, 4, 6
        ]
    })

    epsilon = 0.5
    U = 10
    V = 0
    result = run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U=U, V=V)

    num_users = df["user_id"].nunique()
    dataset = result["dataset"]
    index_i = dataset.compute_index_i(epsilon)
    assert index_i >= num_users

    assert result["T_clipped"] != result["T_unclipped"]

    assert result["sens_clipped"] != result["sens_unclipped"]

def test_one_user_with_max_contributions():
    """
    Validate correct handling when one user has significantly more contributions.
    """
    df = pd.DataFrame({
        "user_id": ["u1"] * 5 + ["u2"] * 2 + ["u3"] * 2 + ["u4"] * 2,
        "attr": [10, 12, 11, 9, 13, 5, 6, 7, 8, 7, 9]
    })

    epsilon = 0.1
    U = 15
    V = 0

    result = run_user_level_dp_pipeline(df, attribute="attr", epsilon=epsilon, U=U, V=V)
    assert result["T_unclipped"] == 5 * (U-V)

    assert result["T_unclipped"] != result["T_clipped"]

    assert result["sens_clipped"] != result["sens_unclipped"]

def test_compute_bounds_before_index_i():
    """
    Explicitly test the compute_bounds method for a user who lies before index_i, 
    verifying A and B are computed exactly following the formula.
    """
    clipper = Clipper()
    df = pd.DataFrame({
        "user_id": ["u1"] * 5 + ["u2"] * 2 + ["u3"] * 2 + ["u4"] * 2,
        "attr": [10, 12, 11, 9, 13, 5, 6, 7, 8, 7, 9]
    })

    # Run pipeline
    result = run_user_level_dp_pipeline(
        df,
        attribute="attr",
        epsilon=0.1,
        U=10,
        V=0
    )

    dataset = result["dataset"]
    index_i = result["index_i"]
    T_clipped = result["T_clipped"]

    # Test compute_bounds for first user (u1)
    user0 = dataset.users[0]   # u1
    user_index = 0

    A, B = clipper.compute_bounds(
        user_l=user0,
        user_index=user_index,
        index_i=index_i,
        U=10,
        V=0,
        T_epsilon=T_clipped,
        attr_name="attr"
    )

    # expected values
    m = user0.num_records()           # 5
    Y = user0.sum_attribute("attr")   # 55

    d_expected = (m * 10) - T_clipped

    expected_A = d_expected / 2
    expected_B = (m * 10) - (d_expected / 2)

    assert A == expected_A
    assert B == expected_B

def test_release_dp_mean_returns_float():
    """
    Check that both clipped and unclipped DP mean release functions return floating-point outputs irrespective of noise randomness.
    """
    clipper = Clipper()
    clipped_dp = ClippedDPMechanism(epsilon=0.5)
    unclipped_dp = UnclippedDPMechanism(epsilon=0.5)

    df = pd.DataFrame({
        "user_id": ["u1"] * 3 + ["u2"] * 3,
        "attr": [1,2,3,4,5,6],
    })
    dataset = Dataset.from_dataframe(df)
    dataset.sort_by_contribution()

    index_i = dataset.compute_index_i(0.5)
    T_clipped = dataset.compute_T_epsilon_clipped(index_i, 10, 0, "attr")
    T_unclipped = dataset.compute_T_epsilon_unclipped(10, 0, "attr")

    clipped_dp_mean, err = clipped_dp.release_clipped_dp_mean(dataset, "attr", index_i, 10, 0, T_clipped, clipper)
    unclipped_dp_mean = unclipped_dp.release_dp_mean_unclipped(dataset, "attr", T_unclipped)

    assert isinstance(clipped_dp_mean, float)
    assert isinstance(unclipped_dp_mean, float)

def test_compute_U_and_V_normal_dataset():
    """
    Verify correct computation of global attribute bounds U (max) and V (min) for a typical dataset with varying values.
    """
    df = pd.DataFrame({
        "user_id": ["u1"] * 3 + ["u2"] * 3,
        "attr": [5, 7, 9, 2, 4, 6]
    })

    dataset = Dataset.from_dataframe(df)

    U = dataset.compute_U("attr")
    V = dataset.compute_V("attr")

    assert U == 9      # max of all contributions
    assert V == 2      # min of all contributions

def test_dataset_from_single_line_csv(tmp_path):
    """
    Test Dataset.from_csv with a one-line CSV.
    Each row should become one record under one user.
    """

    csv_content = (
        "user_id,attr\n"
        "u1,120\n"
    )

    csv_file = tmp_path / "single_line.csv"
    csv_file.write_text(csv_content)

    dataset = Dataset.from_csv(csv_file)

    # Assertions
    assert dataset is not None
    assert len(dataset.users) == 1

    user = dataset.users[0]
    assert user.user_id == "u1"
    assert len(user.records) == 1
    assert user.records[0]["user_id"] == "u1"
    assert user.records[0]["attr"] == "120" 

def test_dataset_to_single_line_csv(tmp_path):
    """
    Test Dataset.to_csv by writing a dataset to CSV and reading it back.
    """

    df = pd.DataFrame({
        "user_id": ["u1"],
        "attr": [120]
    })

    # Build dataset from DataFrame
    dataset = Dataset.from_dataframe(df)

    csv_file = tmp_path / "output_single_line.csv"
    dataset.to_csv(csv_file)

    # Reload using from_csv
    reloaded_dataset = Dataset.from_csv(csv_file)

    # Assertions
    assert len(reloaded_dataset.users) == 1

    user = reloaded_dataset.users[0]
    assert user.user_id == "u1"
    assert len(user.records) == 1

    record = user.records[0]
    assert record["user_id"] == "u1"
    assert record["attr"] == "120"         
    assert "user_rank" in record             
    assert record["user_rank"] == "1"

