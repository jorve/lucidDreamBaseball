import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.weekly_digest import WeeklyDigestBuilder  # noqa: E402


class TestWeeklyDigest(unittest.TestCase):
	def test_generates_json_and_text_digest(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			league_path = temp_root / "view_league_weekly_latest.json"
			gm_path = temp_root / "view_gm_weekly_latest.json"
			free_agents_path = temp_root / "free_agent_candidates_latest.json"
			out_path = temp_root / "weekly_digest_latest.json"
			text_path = temp_root / "weekly_digest_latest.txt"

			league_path.write_text(
				json.dumps(
					{
						"window": {"start_date": "2026-03-20", "end_date": "2026-03-22", "days": 3},
						"summary": {"player_count": 2},
						"leaders": {
							"overperformers": [{"player_id": "1", "player_name": "A", "player_role": "batters", "performance_delta": 2.5, "performance_flag": "overperforming"}],
							"underperformers": [{"player_id": "2", "player_name": "B", "player_role": "sp", "performance_delta": -1.2, "performance_flag": "underperforming"}],
						},
						"weekly_summary": {
							"overall_performance_counts": {"overperforming": 1, "underperforming": 1, "on_track": 0, "insufficient_data": 0},
							"category_summary": {
								"aRBI": {"overperforming_count": 1, "underperforming_count": 0, "top_overperformers": [{"player_id": "1", "player_name": "A"}], "top_underperformers": []},
								"aSB": {"overperforming_count": 0, "underperforming_count": 0, "top_overperformers": [], "top_underperformers": []},
								"MGS": {"overperforming_count": 0, "underperforming_count": 1, "top_overperformers": [], "top_underperformers": [{"player_id": "2", "player_name": "B"}]},
								"VIJAY": {"overperforming_count": 0, "underperforming_count": 0, "top_overperformers": [], "top_underperformers": []},
							},
						},
					}
				)
			)
			gm_path.write_text(json.dumps({"summary": {"player_count": 2}}))
			free_agents_path.write_text(
				json.dumps(
					{
						"replacement_suggestions": {
							"suggestions": [
								{
									"team_name": "LDB",
									"add_player": {"player_name": "FA One"},
									"drop_player": {"player_name": "Drop One"},
									"net_points_weekly": 3.2,
								}
							]
						}
					}
				)
			)

			builder = WeeklyDigestBuilder()
			with patch("analytics.weekly_digest.get_view_league_weekly_latest_path", return_value=league_path), patch(
				"analytics.weekly_digest.get_view_gm_weekly_latest_path",
				return_value=gm_path,
			), patch(
				"analytics.weekly_digest.get_free_agent_candidates_latest_path",
				return_value=free_agents_path,
			), patch(
				"analytics.weekly_digest.get_weekly_digest_latest_path",
				return_value=out_path,
			), patch(
				"analytics.weekly_digest.get_weekly_digest_latest_text_path",
				return_value=text_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 20), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertTrue(out_path.exists())
				self.assertTrue(text_path.exists())
				payload = json.loads(out_path.read_text())
				self.assertEqual(payload["summary"]["overperforming_count"], 1)
				self.assertEqual(payload["summary"]["underperforming_count"], 1)
				self.assertEqual(payload["summary"]["replacement_candidates"], 1)
				self.assertIn("aRBI", payload["category_spotlight"])
				self.assertEqual(payload["recommended_swaps"][0]["team_name"], "LDB")
				text_digest = text_path.read_text()
				self.assertIn("Weekly Digest", text_digest)
				self.assertIn("Recommended Swaps", text_digest)


if __name__ == "__main__":
	unittest.main()
