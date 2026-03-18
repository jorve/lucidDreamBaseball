import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.projection_horizons import ProjectionHorizonBuilder  # noqa: E402


class TestProjectionHorizons(unittest.TestCase):
	def test_weekly_window_is_day_aware_for_entire_week(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			priors_path = temp_root / "preseason_player_priors.json"
			blend_path = temp_root / "player_projection_deltas_latest.json"
			daily_path = temp_root / "player_projection_daily_latest.json"
			weekly_path = temp_root / "player_projection_weekly_latest.json"
			priors_path.write_text(
				json.dumps(
					{
						"players": [
							{
								"player_id": "1",
								"player_name": "Test Batter",
								"player_role": "batters",
								"prior_projection": 183.0,
								"projected_appearances": 183.0,
								"aRBI_per_app": 0.5,
								"aSB_per_app": 0.2,
							}
						]
					}
				)
			)
			blend_path.write_text(json.dumps({"players": [{"player_id": "1", "blended_projection": 183.0, "performance_delta": 0.0}]}))

			builder = ProjectionHorizonBuilder()
			with patch("analytics.projection_horizons.get_preseason_player_priors_path", return_value=priors_path), patch(
				"analytics.projection_horizons.get_player_projection_deltas_latest_path",
				return_value=blend_path,
			), patch(
				"analytics.projection_horizons.get_player_projection_daily_latest_path",
				return_value=daily_path,
			), patch(
				"analytics.projection_horizons.get_player_projection_weekly_latest_path",
				return_value=weekly_path,
			):
				test_days = [
					("2026-03-16", 7),  # Monday
					("2026-03-17", 6),  # Tuesday
					("2026-03-18", 5),  # Wednesday
					("2026-03-19", 4),  # Thursday
					("2026-03-20", 3),  # Friday
					("2026-03-21", 2),  # Saturday
					("2026-03-22", 1),  # Sunday
				]
				for day_value, expected_days in test_days:
					target_date = datetime.datetime.strptime(day_value, "%Y-%m-%d")
					result = builder.build(target_date, dry_run=False)
					self.assertEqual(result["status"], "ok")
					weekly_payload = json.loads(weekly_path.read_text())
					self.assertEqual(weekly_payload["window"]["days"], expected_days)
					self.assertEqual(weekly_payload["window"]["start_date"], day_value)
					self.assertEqual(weekly_payload["window"]["end_date"], "2026-03-22")
					self.assertEqual(weekly_payload["window"]["week_end_weekday"], 6)


if __name__ == "__main__":
	unittest.main()
