import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from run_from_config_dir import main as run_from_config_dir_main


class TestRunnerStatusJson(unittest.TestCase):
    def test_runner_writes_status_and_cumulative_budget(self):
        with TemporaryDirectory() as td:
            prev = os.getcwd()
            try:
                os.chdir(td)
                Path("config").mkdir(parents=True, exist_ok=True)
                Path("data").mkdir(parents=True, exist_ok=True)

                csv_path = Path("data/simple.csv")
                csv_path.write_text(
                    "\n".join(
                        [
                            "user_id,age,city",
                            "u1,20,A",
                            "u1,40,B",
                            "u2,30,A",
                        ]
                    )
                )

                # Single query config (epsilon=0.5)
                Path("config/single.json").write_text(
                    json.dumps(
                        {
                            "data": {"csv": str(csv_path)},
                            "differential_privacy": {
                                "level": "item",
                                "query": "mean",
                                "attribute": "age",
                                "epsilon": 0.5,
                                "min_value": 0,
                                "max_value": 100,
                            },
                        }
                    )
                )

                # Multi query config (eps=0.7 + 0.3)
                Path("config/multi.json").write_text(
                    json.dumps(
                        {
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
                    )
                )

                run_from_config_dir_main()

                status_path = Path("output/status.json")
                self.assertTrue(status_path.exists())
                payload = json.loads(status_path.read_text())
                self.assertEqual(payload.get("status"), "success")
                self.assertIn("single.json", payload["results"])
                self.assertIn("multi.json", payload["results"])
                self.assertAlmostEqual(payload["cumulative_epsilon_budget"], 1.5, places=9)
            finally:
                os.chdir(prev)

    def test_runner_marks_partial_error(self):
        with TemporaryDirectory() as td:
            prev = os.getcwd()
            try:
                os.chdir(td)
                Path("config").mkdir(parents=True, exist_ok=True)
                Path("data").mkdir(parents=True, exist_ok=True)

                csv_path = Path("data/simple.csv")
                csv_path.write_text("age\n10\n20\n")

                # valid
                Path("config/good.json").write_text(
                    json.dumps(
                        {
                            "data": {"csv": str(csv_path)},
                            "differential_privacy": {
                                "level": "item",
                                "query": "mean",
                                "attribute": "age",
                                "epsilon": 1.0,
                                "min_value": 0,
                                "max_value": 100,
                            },
                        }
                    )
                )
                # invalid
                Path("config/bad.json").write_text(
                    json.dumps(
                        {
                            "data": {"csv": str(csv_path)},
                            "differential_privacy": {"level": "item", "query": "mean", "attribute": "age"},
                        }
                    )
                )

                run_from_config_dir_main()
                payload = json.loads(Path("output/status.json").read_text())
                self.assertEqual(payload.get("status"), "partial_error")
            finally:
                os.chdir(prev)


if __name__ == "__main__":
    unittest.main()
