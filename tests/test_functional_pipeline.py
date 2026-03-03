import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from iudx_dp_main import main_process
from run_all_configs import main as run_all_configs_main


class TestFunctionalPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure plotting works in headless environments.
        os.environ.setdefault("MPLBACKEND", "Agg")

    def test_all_configs_main_process_success(self):
        config_dir = Path("all_config")
        for cfg_file in sorted(config_dir.glob("*.json")):
            with cfg_file.open("r") as f:
                cfg = json.load(f)
            out = main_process(cfg)
            self.assertEqual(out.get("status"), "success", msg=cfg_file.name)
            self.assertIn("result", out, msg=cfg_file.name)
            self.assertIn("query", out["result"], msg=cfg_file.name)
            self.assertIn("privacy_level", out["result"], msg=cfg_file.name)

    def test_run_all_configs_writes_result_files(self):
        run_all_configs_main()
        output_dir = Path("output/results")
        self.assertTrue(output_dir.exists())

        active = [
            "item_level_mean",
            "item_level_histogram",
            "user_level_mean",
            "user_level_histogram",
        ]
        for stem in active:
            out_file = output_dir / f"{stem}_output.json"
            self.assertTrue(out_file.exists(), msg=f"Missing: {out_file}")
            with out_file.open("r") as f:
                payload = json.load(f)
            self.assertIn("status", payload)
            self.assertIn(payload["status"], {"success", "error"})

    def test_spatiotemporal_mean_success(self):
        with TemporaryDirectory() as td:
            csv_path = Path(td) / "spatio.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "lat,lon,timestamp,speed",
                        "12.9716,77.5946,2025-01-01 10:15:00,40",
                        "12.9720,77.5950,2025-01-01 10:35:00,60",
                        "12.9800,77.6000,2025-01-01 11:05:00,55",
                    ]
                )
            )

            cfg = {
                "data": {"csv": str(csv_path)},
                "differential_privacy": {
                    "spatio_temporal_analysis": True,
                    "query": "mean",
                    "attribute": "speed",
                    "epsilon": 1.0,
                    "U": 120,
                    "V": 0,
                    "spatial": {
                        "lat_column": "lat",
                        "lon_column": "lon",
                        "h3_resolution": 8,
                    },
                    "temporal": {
                        "timestamp_column": "timestamp",
                        "timeslot_minutes": 30,
                        "start_hour": 10,
                        "end_hour": 11,
                    },
                },
            }

            out = main_process(cfg)
            self.assertEqual(out.get("status"), "success")
            result = out["result"]
            self.assertTrue(result.get("spatio_temporal_analysis"))
            self.assertEqual(result.get("query"), "mean")
            self.assertGreater(result.get("num_hats", 0), 0)

    def test_ui_payload_config_shape_success(self):
        cfg = {
            "operations": ["dp"],
            "data_type": "DP_TestDataset",
            "DP_TestDataset": {
                "insensitive_columns": ["user_id", "city", "age"],
                "level": "item",
                "query": "histogram",
                "attribute": "city",
                "epsilon": 1.0,
                "categories": ["Bengaluru", "Mysuru", "Mangaluru"],
            },
        }
        out = main_process(cfg)
        self.assertEqual(out.get("status"), "success")
        self.assertEqual(out["result"].get("query"), "histogram")
        self.assertEqual(out["result"].get("privacy_level"), "item")

    def test_multi_query_cumulative_epsilon(self):
        with TemporaryDirectory() as td:
            csv_path = Path(td) / "simple.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "user_id,age,city",
                        "u1,20,Bengaluru",
                        "u1,40,Mysuru",
                        "u2,30,Bengaluru",
                    ]
                )
            )

            cfg = {
                "data": {"csv": str(csv_path)},
                "differential_privacy": {
                    "level": "user",
                    "user_column": "user_id",
                    "queries": [
                        {
                            "query": "mean",
                            "attribute": "age",
                            "epsilon": 0.7,
                            "min_value": 0,
                            "max_value": 100,
                        },
                        {
                            "query": "count",
                            "count_attribute": "age",
                            "count_operator": ">",
                            "count_value": 25,
                            "epsilon": 0.3,
                        },
                    ],
                },
            }

            out = main_process(cfg)
            self.assertEqual(out.get("status"), "success")
            result = out["result"]
            self.assertTrue(result.get("multi_query"))
            self.assertEqual(len(result.get("query_results", [])), 2)
            self.assertAlmostEqual(result.get("cumulative_epsilon_budget"), 1.0, places=9)


if __name__ == "__main__":
    unittest.main()
