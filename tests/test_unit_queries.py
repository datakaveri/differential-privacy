import unittest
from unittest.mock import patch

import pandas as pd

from DP.queries.count import _apply_count_filter, user_level_count, item_level_count
from DP.queries.histogram import item_level_histogram, user_level_histogram
from DP.queries.mean import item_level_mean
from DP.router import route_dp_request
from DP.DPError import DPError
from DP.pipelines.spatiotemporal import _build_hats


class TestCountFilters(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2", "u3", "u3"],
                "AGE": [35, 61, 70, 42, 18],
                "GENDER": ["Male", "Male", "Female", "Female", "Male"],
            }
        )

    def test_filter_defaults_to_equals(self):
        filtered, meta = _apply_count_filter(
            self.df, {"count_attribute": "GENDER", "count_value": "Female"}
        )
        self.assertEqual(len(filtered), 2)
        self.assertEqual(meta["operator"], "==")

    def test_filter_numeric_gt(self):
        filtered, meta = _apply_count_filter(
            self.df,
            {"count_attribute": "AGE", "count_operator": ">", "count_value": 40},
        )
        self.assertEqual(len(filtered), 3)
        self.assertEqual(meta["operator"], ">")

    def test_filter_numeric_operator_on_non_numeric_raises(self):
        with self.assertRaises(ValueError):
            _apply_count_filter(
                self.df,
                {"count_attribute": "GENDER", "count_operator": ">", "count_value": 40},
            )

    def test_filter_records_clipping_metadata_present(self):
        _, meta = _apply_count_filter(
            self.df,
            {
                "count_attribute": "AGE",
                "count_operator": ">",
                "count_value": 30,
                "min_value": 0,
                "max_value": 60,
            },
        )
        self.assertEqual(meta["clipping"]["min_value"], 0.0)
        self.assertEqual(meta["clipping"]["max_value"], 60.0)


class TestCountQueries(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2", "u2", "u3"],
                "AGE": [65, 30, 70, 71, 20],
                "GENDER": ["Male", "Male", "Female", "Female", "Male"],
            }
        )

    @patch("numpy.random.laplace", return_value=0.0)
    def test_user_level_count_users_with_match(self, _):
        out = user_level_count(
            self.df,
            {
                "epsilon": 1.0,
                "count_attribute": "AGE",
                "count_operator": ">",
                "count_value": 60,
            },
            user_col="user_id",
        )
        # AGE > 60 rows belong to users u1 and u2
        self.assertEqual(out["true_count"], 2)
        self.assertEqual(out["dp_count"], 2)
        self.assertEqual(out["sensitivity"], 1.0)
        self.assertEqual(out["count_unit"], "users_with_at_least_one_match")
        self.assertIsInstance(out["dp_count"], int)

    @patch("numpy.random.laplace", return_value=0.0)
    def test_item_level_count_matching_records(self, _):
        out = item_level_count(
            self.df,
            {
                "epsilon": 1.0,
                "count_attribute": "AGE",
                "count_operator": ">",
                "count_value": 20,
            },
        )
        # Records > 20: 4 rows.
        self.assertEqual(out["true_count"], 4)
        self.assertEqual(out["sensitivity"], 1.0)
        self.assertEqual(out["dp_count"], 4)
        self.assertEqual(out["count_unit"], "matching_records")
        self.assertIsInstance(out["dp_count"], int)

    @patch("numpy.random.laplace", return_value=0.0)
    def test_user_level_count_no_filter_counts_users(self, _):
        out = user_level_count(
            self.df,
            {
                "epsilon": 1.0,
            },
            user_col="user_id",
        )
        self.assertEqual(out["true_count"], 3)
        self.assertEqual(out["sensitivity"], 1.0)
        self.assertEqual(out["dp_count"], 3)

    @patch("numpy.random.laplace", return_value=0.49)
    def test_count_rounding_behavior(self, _):
        out = item_level_count(
            self.df,
            {
                "epsilon": 1.0,
                "count_attribute": "AGE",
                "count_operator": ">",
                "count_value": 20,
            },
        )
        self.assertEqual(out["true_count"], 4)
        self.assertEqual(out["dp_count"], 4)  # round(4.49) -> 4


class TestHistogramQueries(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2", "u2"],
                "AGE": [5, 25, 45, 85],
            }
        )

    @patch("DP.queries.histogram.laplace_noise", return_value=0.0)
    def test_item_histogram_no_noise(self, _):
        out = item_level_histogram(
            self.df, {"attribute": "AGE", "epsilon": 1.0, "bins": 5, "U": 0, "V": 100}
        )
        self.assertEqual(out["histogram"], [1, 1, 1, 0, 1])
        self.assertEqual(out["sensitivity"], 1.0)
        self.assertEqual(out["histogram_mode"], "numeric")
        self.assertEqual(len(out["bin_labels"]), 5)

    @patch("DP.queries.histogram.laplace_noise", return_value=0.0)
    def test_item_histogram_categorical_no_noise(self, _):
        df = pd.DataFrame(
            {
                "GENDER": ["Male", "Female", "Male", "Others", "Female", "Male"]
            }
        )
        out = item_level_histogram(
            df,
            {
                "attribute": "GENDER",
                "epsilon": 1.0,
                "categories": ["Male", "Female", "Others"],
            },
        )
        self.assertEqual(out["bin_labels"], ["Male", "Female", "Others"])
        self.assertEqual(out["histogram"], [3.0, 2.0, 1.0])
        self.assertEqual(out["sensitivity"], 1.0)
        self.assertEqual(out["histogram_mode"], "categorical")

    @patch("DP.queries.histogram.laplace_noise", return_value=0.0)
    def test_item_histogram_numeric_with_bin_width(self, _):
        out = item_level_histogram(
            self.df,
            {
                "attribute": "AGE",
                "epsilon": 1.0,
                "U": 0,
                "V": 100,
                "bin_width": 20,
            },
        )
        self.assertEqual(out["bins"], 5)
        self.assertEqual(out["histogram"], [1, 1, 1, 0, 1])

    def test_item_histogram_numeric_invalid_bounds_raises(self):
        with self.assertRaises(ValueError):
            item_level_histogram(
                self.df,
                {
                    "attribute": "AGE",
                    "epsilon": 1.0,
                    "U": 100,
                    "V": 50,
                    "bin_width": 10,
                },
            )

    def test_item_histogram_empty_categories_raises(self):
        with self.assertRaises(ValueError):
            item_level_histogram(
                self.df,
                {"attribute": "AGE", "epsilon": 1.0, "categories": []},
            )

    @patch("DP.queries.histogram.laplace_noise", return_value=0.0)
    def test_user_histogram_no_noise(self, _):
        out = user_level_histogram(
            self.df,
            {"attribute": "AGE", "epsilon": 10.0, "bins": 5, "U": 0, "V": 100},
            user_col="user_id",
        )
        self.assertEqual(len(out["histogram"]), 5)
        self.assertEqual(sum(out["histogram"]), 4.0)
        self.assertGreaterEqual(out["sensitivity"], 0.0)
        self.assertEqual(out["histogram_mode"], "numeric")
        self.assertEqual(len(out["bin_labels"]), 5)

    @patch("DP.queries.histogram.laplace_noise", return_value=0.0)
    def test_user_histogram_categorical_no_noise(self, _):
        df = pd.DataFrame(
            {
                "user_id": ["u1", "u1", "u2", "u3", "u3"],
                "GENDER": ["Male", "Female", "Female", "Others", "Male"],
            }
        )
        out = user_level_histogram(
            df,
            {
                "attribute": "GENDER",
                "epsilon": 10.0,
                "categories": ["Male", "Female", "Others"],
            },
            user_col="user_id",
        )
        self.assertEqual(out["histogram_mode"], "categorical")
        self.assertEqual(out["bin_labels"], ["Male", "Female", "Others"])
        self.assertEqual(len(out["histogram"]), 3)


class TestMeanQueries(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({"AGE": [10.0, 20.0, 30.0, 40.0]})

    @patch("numpy.random.laplace", side_effect=[0.0, 0.0])
    def test_item_mean_no_noise(self, _):
        out = item_level_mean(
            self.df, {"attribute": "AGE", "epsilon": 1.0, "max_value": 100.0}
        )
        self.assertEqual(out["dp_mean"], 25.0)
        self.assertEqual(out["sensitivity_sum"], 100.0)

    @patch("numpy.random.laplace", side_effect=[0.0, 0.0])
    def test_item_mean_uses_clipping_bounds(self, _):
        df = pd.DataFrame({"AGE": [-10.0, 20.0, 200.0]})
        out = item_level_mean(
            df,
            {
                "attribute": "AGE",
                "epsilon": 1.0,
                "min_value": 0.0,
                "max_value": 100.0,
            },
        )
        # clipped values are [0, 20, 100], mean 40
        self.assertAlmostEqual(out["dp_mean"], 40.0, places=7)

    def test_item_mean_non_numeric_attribute_raises(self):
        df = pd.DataFrame({"CITY": ["A", "B"]})
        with self.assertRaises(ValueError):
            item_level_mean(
                df,
                {
                    "attribute": "CITY",
                    "epsilon": 1.0,
                    "min_value": 0.0,
                    "max_value": 100.0,
                },
            )


class TestRouterValidation(unittest.TestCase):
    def test_router_rejects_non_positive_epsilon(self):
        with self.assertRaises(DPError):
            route_dp_request(
                {
                    "data": {"csv": "data/BeneficiaryData.csv"},
                    "differential_privacy": {
                        "level": "item",
                        "query": "mean",
                        "epsilon": 0,
                    },
                }
            )

    def test_router_rejects_spatiotemporal_non_mean(self):
        with self.assertRaises(DPError):
            route_dp_request(
                {
                    "data": {"csv": "data/BeneficiaryData.csv"},
                    "differential_privacy": {
                        "spatio_temporal_analysis": True,
                        "query": "count",
                        "epsilon": 1.0,
                    },
                }
            )


class TestSpatioTemporalHelpers(unittest.TestCase):
    def test_build_hats_with_lat_lon(self):
        df = pd.DataFrame(
            {
                "lat": [12.9716, 12.9720],
                "lon": [77.5946, 77.5950],
                "ts": ["2025-01-01 10:15:00", "2025-01-01 10:35:00"],
                "speed": [40, 60],
            }
        )
        dp_cfg = {
            "spatial": {"lat_column": "lat", "lon_column": "lon", "h3_resolution": 8},
            "temporal": {
                "timestamp_column": "ts",
                "timeslot_minutes": 30,
                "start_hour": 9,
                "end_hour": 11,
            },
        }
        out = _build_hats(df, dp_cfg)
        self.assertIn("HAT", out.columns)
        self.assertEqual(len(out), 2)
        self.assertTrue(all(isinstance(x, str) for x in out["HAT"]))


if __name__ == "__main__":
    unittest.main()
