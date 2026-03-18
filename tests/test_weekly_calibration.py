import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.weekly_calibration import WeeklyCalibrationBuilder  # noqa: E402


class TestWeeklyCalibration(unittest.TestCase):
	def test_calibration_computes_metrics_from_projection_and_ytd_deltas(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			history_root = temp_root / "history"
			(history_root / "2026-03-16").mkdir(parents=True, exist_ok=True)
			projection_history_path = history_root / "2026-03-16" / "player_projection_weekly_latest.json"
			projection_history_path.write_text(
				json.dumps(
					{
						"window": {"start_date": "2026-03-16", "end_date": "2026-03-22"},
						"players": [
							{"player_id": "1", "player_name": "A", "player_role": "batters", "projected_points_window": 10.0},
							{"player_id": "2", "player_name": "B", "player_role": "sp", "projected_points_window": 20.0},
						],
					}
				)
			)
			index_path = temp_root / "artifact_history_latest.json"
			index_path.write_text("{}")
			output_path = temp_root / "weekly_calibration_latest.json"

			raw_root = temp_root / "raw"
			(raw_root / "2026-03-22").mkdir(parents=True, exist_ok=True)
			(raw_root / "2026-03-15").mkdir(parents=True, exist_ok=True)
			(raw_root / "2026-03-22" / "rosters_2026-03-22.json").write_text(
				json.dumps(
					{
						"body": {
							"rosters": {
								"teams": [
									{"players": [{"id": 1, "ytd_points": 35.0}, {"id": 2, "ytd_points": 80.0}]}
								]
							}
						}
					}
				)
			)
			(raw_root / "2026-03-15" / "rosters_2026-03-15.json").write_text(
				json.dumps(
					{
						"body": {
							"rosters": {
								"teams": [
									{"players": [{"id": 1, "ytd_points": 20.0}, {"id": 2, "ytd_points": 55.0}]}
								]
							}
						}
					}
				)
			)

			builder = WeeklyCalibrationBuilder()
			with patch("analytics.weekly_calibration.get_weekly_calibration_latest_path", return_value=output_path), patch(
				"analytics.weekly_calibration.get_artifact_history_latest_path",
				return_value=index_path,
			), patch(
				"analytics.weekly_calibration.get_ingestion_raw_dir",
				side_effect=lambda date_value=None: (raw_root / date_value.strftime("%Y-%m-%d")) if date_value else raw_root,
			):
				result = builder.build(datetime.datetime(2026, 3, 23), dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(output_path.read_text())
				self.assertEqual(payload["summary"]["players_calibrated"], 2)
				self.assertEqual(payload["metrics"]["overall"]["count"], 2)
				self.assertIn("mae_points", payload["metrics"]["overall"])
				self.assertEqual(payload["calibration_week"]["start_date"], "2026-03-16")
				self.assertEqual(payload["calibration_week"]["end_date"], "2026-03-22")


if __name__ == "__main__":
	unittest.main()
