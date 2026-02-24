import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from DP.DPError import DPError
from DP.config_normalizer import _infer_csv_from_data_type, normalize_config
from DP.core import run_dp_from_config


class TestConfigNormalizer(unittest.TestCase):
    def test_normalize_ui_payload_uses_csv_in_payload(self):
        cfg = {
            "operations": ["dp"],
            "data_type": "datasetA",
            "datasetA": {
                "level": "item",
                "query": "histogram",
                "attribute": "city",
                "epsilon": 1.0,
                "categories": ["A", "B"],
                "csv": "/tmp/custom.csv",
                "insensitive_columns": ["x"],
            },
        }
        out = normalize_config(cfg, data_dir="data")
        self.assertEqual(out["data"]["csv"], "/tmp/custom.csv")
        self.assertIn("differential_privacy", out)
        self.assertNotIn("insensitive_columns", out["differential_privacy"])

    def test_normalize_ui_payload_missing_dataset_block_raises(self):
        cfg = {"operations": ["dp"], "data_type": "datasetA"}
        with self.assertRaises(DPError):
            normalize_config(cfg, data_dir="data")

    def test_infer_csv_single_file(self):
        with TemporaryDirectory() as td:
            p = os.path.join(td, "abc.csv")
            with open(p, "w") as f:
                f.write("x\n1\n")
            out = _infer_csv_from_data_type("anything", data_dir=td)
            self.assertEqual(out, p)

    def test_infer_csv_ambiguous_without_match_raises(self):
        with TemporaryDirectory() as td:
            with open(os.path.join(td, "foo.csv"), "w") as f:
                f.write("x\n1\n")
            with open(os.path.join(td, "bar.csv"), "w") as f:
                f.write("x\n2\n")
            with self.assertRaises(DPError):
                _infer_csv_from_data_type("baz", data_dir=td)


class TestCoreMultiQuery(unittest.TestCase):
    @patch("numpy.random.laplace", return_value=0.0)
    def test_multi_query_partial_error_and_budget(self, _):
        with TemporaryDirectory() as td:
            csv_path = os.path.join(td, "simple.csv")
            with open(csv_path, "w") as f:
                f.write("user_id,age,city\nu1,20,A\nu1,40,B\nu2,30,A\n")

            cfg = {
                "data": {"csv": csv_path},
                "differential_privacy": {
                    "level": "user",
                    "user_column": "user_id",
                    "queries": [
                        {"query": "mean", "attribute": "age", "epsilon": 0.7, "min_value": 0, "max_value": 100},
                        {"query": "count", "count_attribute": "age", "count_operator": ">", "count_value": 20},
                        {"query": "histogram", "attribute": "city", "epsilon": 0.3},
                    ],
                },
            }
            out = run_dp_from_config(cfg)
            self.assertEqual(out["status"], "partial_error")
            result = out["result"]
            self.assertTrue(result["multi_query"])
            self.assertEqual(len(result["query_results"]), 3)
            self.assertAlmostEqual(result["cumulative_epsilon_budget"], 1.0, places=9)
            statuses = [q["status"] for q in result["query_results"]]
            self.assertEqual(statuses, ["success", "error", "success"])

    def test_multi_query_empty_list_errors(self):
        cfg = {"differential_privacy": {"queries": []}, "data": {"csv": "dummy.csv"}}
        out = run_dp_from_config(cfg)
        self.assertEqual(out["status"], "error")
        self.assertEqual(out["error"]["code"], "CONFIG_INVALID")

    @patch("numpy.random.laplace", return_value=0.0)
    def test_single_query_has_precomputed_curve(self, _):
        with TemporaryDirectory() as td:
            csv_path = os.path.join(td, "simple.csv")
            with open(csv_path, "w") as f:
                f.write("age\n10\n20\n30\n")
            cfg = {
                "data": {"csv": csv_path},
                "differential_privacy": {
                    "level": "item",
                    "query": "mean",
                    "attribute": "age",
                    "epsilon": 1.0,
                    "min_value": 0,
                    "max_value": 100,
                },
            }
            out = run_dp_from_config(cfg)
            self.assertEqual(out["status"], "success")
            slider = out["result"].get("epsilon_slider_precomputed")
            self.assertIsInstance(slider, dict)
            self.assertEqual(slider.get("metric"), "mean")
            self.assertEqual(len(slider.get("values", [])), 16)


if __name__ == "__main__":
    unittest.main()
