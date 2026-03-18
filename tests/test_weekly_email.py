import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.weekly_email import WeeklyEmailBuilder  # noqa: E402


class TestWeeklyEmail(unittest.TestCase):
	def test_generates_payload_with_history_fallback(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			league_path = temp_root / "view_league_weekly_latest.json"
			proj_path = temp_root / "player_projection_weekly_latest.json"
			digest_latest = temp_root / "weekly_digest_latest.json"
			free_agents = temp_root / "free_agent_candidates_latest.json"
			out_path = temp_root / "weekly_email_payload_latest.json"
			text_path = temp_root / "weekly_email_latest.txt"
			history_dir = temp_root / "history"
			history_dir.mkdir(parents=True, exist_ok=True)
			prev_digest = history_dir / "weekly_digest_2026-03-15.json"

			league_path.write_text(json.dumps({"weekly_summary": {"category_summary": {}}, "window": {"days": 3}}))
			proj_path.write_text(
				json.dumps(
					{
						"window": {"days": 3},
						"players": [
							{"player_id": "1", "player_name": "A", "player_role": "batters", "projected_points_window": 9.0, "performance_flag": "overperforming"}
						],
					}
				)
			)
			digest_latest.write_text(json.dumps({"window": {"start_date": "2026-03-16", "end_date": "2026-03-22"}}))
			free_agents.write_text(
				json.dumps(
					{
						"replacement_suggestions": {
							"suggestions": [
								{"team_name": "LDB", "add_player": {"player_name": "FA"}, "drop_player": {"player_name": "DROP"}, "net_points_weekly": 2.0}
							]
						}
					}
				)
			)
			prev_digest.write_text(
				json.dumps(
					{
						"summary": {"overperforming_count": 4, "underperforming_count": 2},
						"top_overperformers": [{"player_name": "X", "player_role": "batters", "performance_delta": 2.0, "performance_flag": "overperforming"}],
						"top_underperformers": [{"player_name": "Y", "player_role": "sp", "performance_delta": -1.5, "performance_flag": "underperforming"}],
						"category_spotlight": {},
					}
				)
			)

			builder = WeeklyEmailBuilder()
			with patch("analytics.weekly_email.get_view_league_weekly_latest_path", return_value=league_path), patch(
				"analytics.weekly_email.get_player_projection_weekly_latest_path",
				return_value=proj_path,
			), patch(
				"analytics.weekly_email.get_weekly_digest_latest_path",
				return_value=digest_latest,
			), patch(
				"analytics.weekly_email.get_free_agent_candidates_latest_path",
				return_value=free_agents,
			), patch(
				"analytics.weekly_email.get_weekly_email_payload_latest_path",
				return_value=out_path,
			), patch(
				"analytics.weekly_email.get_weekly_email_text_latest_path",
				return_value=text_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(out_path.read_text())
				self.assertEqual(payload["lookback_week"]["status"], "ok")
				self.assertEqual(payload["lookahead_week"]["projected_top_players"][0]["player_name"], "A")
				self.assertIn("delivery_metadata", payload)
				self.assertIn("subject", payload["delivery_metadata"])
				self.assertIn("send_schedule", payload["delivery_metadata"])
				self.assertEqual(payload["delivery_metadata"]["delivery_mode"], "generation_only")
				self.assertTrue(text_path.exists())
				self.assertIn("Upcoming Week", text_path.read_text())


if __name__ == "__main__":
	unittest.main()
