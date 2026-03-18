import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.free_agent_candidates import FreeAgentCandidatesBuilder  # noqa: E402


class TestFreeAgentCandidates(unittest.TestCase):
	def test_build_filters_rostered_and_ranks_candidates(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			priors_path = temp_root / "preseason_player_priors.json"
			daily_path = temp_root / "player_projection_daily_latest.json"
			weekly_path = temp_root / "player_projection_weekly_latest.json"
			roster_state_path = temp_root / "roster_state_latest.json"
			eligibility_path = temp_root / "player_eligibility_latest.json"
			out_path = temp_root / "free_agent_candidates_latest.json"

			priors_path.write_text(
				json.dumps(
					{
						"players": [
							{"player_id": "1", "player_name": "Rostered", "player_role": "batters"},
							{"player_id": "2", "player_name": "Candidate A", "player_role": "batters"},
							{"player_id": "3", "player_name": "Candidate B", "player_role": "sp"},
						]
					}
				)
			)
			daily_path.write_text(
				json.dumps(
					{
						"players": [
							{"player_id": "2", "projected_points_window": 4.0, "performance_delta": 1.0},
							{"player_id": "3", "projected_points_window": 3.0, "performance_delta": -0.5},
						]
					}
				)
			)
			weekly_path.write_text(
				json.dumps(
					{
						"players": [
							{"player_id": "2", "projected_points_window": 12.0, "performance_delta": 1.0},
							{"player_id": "3", "projected_points_window": 8.0, "performance_delta": -0.5},
						]
					}
				)
			)
			roster_state_path.write_text(
				json.dumps(
					{
						"as_of_utc": "2026-03-20T00:00:00Z",
						"teams": [{"team_id": "10", "players": [{"player_id": "1"}]}],
					}
				)
			)
			eligibility_path.write_text(json.dumps({"players": [{"player_id": "2", "slot_positions": ["OF", "U"]}]}))

			builder = FreeAgentCandidatesBuilder()
			with patch("analytics.free_agent_candidates.get_preseason_player_priors_path", return_value=priors_path), patch(
				"analytics.free_agent_candidates.get_player_projection_daily_latest_path",
				return_value=daily_path,
			), patch(
				"analytics.free_agent_candidates.get_player_projection_weekly_latest_path",
				return_value=weekly_path,
			), patch(
				"analytics.free_agent_candidates.get_roster_state_latest_path",
				return_value=roster_state_path,
			), patch(
				"analytics.free_agent_candidates.get_player_eligibility_latest_path",
				return_value=eligibility_path,
			), patch(
				"analytics.free_agent_candidates.get_free_agent_candidates_latest_path",
				return_value=out_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 20), dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(out_path.read_text())
				self.assertEqual(payload["summary"]["candidate_count"], 2)
				self.assertEqual(payload["candidates"][0]["player_id"], "2")
				self.assertEqual(payload["candidates"][0]["slot_positions"], ["OF", "U"])
				self.assertEqual(payload["assignment_snapshot"]["source"], "roster_state_latest")
				self.assertGreaterEqual(payload["replacement_suggestions"]["summary"]["suggestions_count"], 1)
				first_suggestion = payload["replacement_suggestions"]["suggestions"][0]
				self.assertEqual(first_suggestion["team_id"], "10")
				self.assertEqual(first_suggestion["add_player"]["player_id"], "2")
				self.assertEqual(first_suggestion["drop_player"]["player_id"], "1")


if __name__ == "__main__":
	unittest.main()
