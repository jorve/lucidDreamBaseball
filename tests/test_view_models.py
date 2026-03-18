import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.view_models import ViewModelBuilder  # noqa: E402


class TestViewModels(unittest.TestCase):
	def test_build_generates_league_and_gm_views(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			daily_projection_path = temp_root / "player_projection_daily_latest.json"
			weekly_projection_path = temp_root / "player_projection_weekly_latest.json"
			league_daily_path = temp_root / "view_league_daily_latest.json"
			league_weekly_path = temp_root / "view_league_weekly_latest.json"
			gm_daily_path = temp_root / "view_gm_daily_latest.json"
			gm_weekly_path = temp_root / "view_gm_weekly_latest.json"

			daily_projection_path.write_text(
				json.dumps(
					{
						"window": {"start_date": "2026-03-20", "end_date": "2026-03-20", "days": 1},
						"players": [
							{
								"player_id": "1",
								"player_name": "A",
								"player_role": "batters",
								"projected_points_window": 8.0,
								"performance_delta": 1.5,
								"performance_flag": "overperforming",
								"category_delta_pct": {"aRBI": 18.0, "aSB": 22.0, "MGS": None, "VIJAY": None},
								"category_performance_flags": {"aRBI": "overperforming", "aSB": "overperforming", "MGS": "insufficient_data", "VIJAY": "insufficient_data"},
							},
							{
								"player_id": "2",
								"player_name": "B",
								"player_role": "sp",
								"projected_points_window": 6.0,
								"performance_delta": -1.0,
								"performance_flag": "underperforming",
								"category_delta_pct": {"aRBI": None, "aSB": None, "MGS": -15.0, "VIJAY": -25.0},
								"category_performance_flags": {"aRBI": "insufficient_data", "aSB": "insufficient_data", "MGS": "underperforming", "VIJAY": "underperforming"},
							},
						],
					}
				)
			)
			weekly_projection_path.write_text(
				json.dumps(
					{
						"window": {"start_date": "2026-03-20", "end_date": "2026-03-22", "days": 3},
						"players": [
							{
								"player_id": "1",
								"player_name": "A",
								"player_role": "batters",
								"projected_points_window": 24.0,
								"performance_delta": 1.5,
								"performance_flag": "overperforming",
								"category_delta_pct": {"aRBI": 18.0, "aSB": 22.0, "MGS": None, "VIJAY": None},
								"category_performance_flags": {"aRBI": "overperforming", "aSB": "overperforming", "MGS": "insufficient_data", "VIJAY": "insufficient_data"},
							},
							{
								"player_id": "2",
								"player_name": "B",
								"player_role": "sp",
								"projected_points_window": 18.0,
								"performance_delta": -1.0,
								"performance_flag": "underperforming",
								"category_delta_pct": {"aRBI": None, "aSB": None, "MGS": -15.0, "VIJAY": -25.0},
								"category_performance_flags": {"aRBI": "insufficient_data", "aSB": "insufficient_data", "MGS": "underperforming", "VIJAY": "underperforming"},
							},
						],
					}
				)
			)

			builder = ViewModelBuilder()
			with patch("analytics.view_models.get_player_projection_daily_latest_path", return_value=daily_projection_path), patch(
				"analytics.view_models.get_player_projection_weekly_latest_path",
				return_value=weekly_projection_path,
			), patch(
				"analytics.view_models.get_view_league_daily_latest_path",
				return_value=league_daily_path,
			), patch(
				"analytics.view_models.get_view_league_weekly_latest_path",
				return_value=league_weekly_path,
			), patch(
				"analytics.view_models.get_view_gm_daily_latest_path",
				return_value=gm_daily_path,
			), patch(
				"analytics.view_models.get_view_gm_weekly_latest_path",
				return_value=gm_weekly_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 20), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertTrue(league_daily_path.exists())
				self.assertTrue(league_weekly_path.exists())
				self.assertTrue(gm_daily_path.exists())
				self.assertTrue(gm_weekly_path.exists())

				league_daily_payload = json.loads(league_daily_path.read_text())
				self.assertEqual(league_daily_payload["view_type"], "league")
				self.assertEqual(league_daily_payload["horizon"], "daily")
				self.assertEqual(league_daily_payload["leaders"]["projected_points"][0]["player_id"], "1")
				league_weekly_payload = json.loads(league_weekly_path.read_text())
				self.assertIn("weekly_summary", league_weekly_payload)
				self.assertEqual(league_weekly_payload["weekly_summary"]["overall_performance_counts"]["overperforming"], 1)
				self.assertEqual(league_weekly_payload["weekly_summary"]["overall_performance_counts"]["underperforming"], 1)
				self.assertEqual(
					league_weekly_payload["weekly_summary"]["category_summary"]["aRBI"]["top_overperformers"][0]["player_id"],
					"1",
				)

				gm_weekly_payload = json.loads(gm_weekly_path.read_text())
				self.assertEqual(gm_weekly_payload["view_type"], "gm")
				self.assertEqual(gm_weekly_payload["horizon"], "weekly")
				self.assertEqual(gm_weekly_payload["summary"]["player_count"], 2)


if __name__ == "__main__":
	unittest.main()
