import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.artifact_history import ArtifactHistoryBuilder  # noqa: E402


class TestArtifactHistory(unittest.TestCase):
	def test_snapshots_available_artifacts(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			history_index_path = temp_root / "artifact_history_latest.json"
			paths = {
				"player_projection_daily": temp_root / "player_projection_daily_latest.json",
				"player_projection_weekly": temp_root / "player_projection_weekly_latest.json",
				"view_league_daily": temp_root / "view_league_daily_latest.json",
				"view_league_weekly": temp_root / "view_league_weekly_latest.json",
				"view_gm_daily": temp_root / "view_gm_daily_latest.json",
				"view_gm_weekly": temp_root / "view_gm_weekly_latest.json",
				"free_agent_candidates": temp_root / "free_agent_candidates_latest.json",
				"team_weekly_totals": temp_root / "team_weekly_totals_latest.json",
				"clap_v2": temp_root / "clap_v2_latest.json",
				"clap_player_history": temp_root / "clap_player_history_latest.json",
				"matchup_expectations": temp_root / "matchup_expectations_latest.json",
				"clap_calibration": temp_root / "clap_calibration_latest.json",
				"weekly_digest_json": temp_root / "weekly_digest_latest.json",
				"weekly_digest_text": temp_root / "weekly_digest_latest.txt",
				"weekly_email_payload": temp_root / "weekly_email_payload_latest.json",
				"weekly_email_text": temp_root / "weekly_email_latest.txt",
			}
			for path_value in paths.values():
				path_value.write_text("{}")

			builder = ArtifactHistoryBuilder()
			with patch("analytics.artifact_history.get_artifact_history_latest_path", return_value=history_index_path), patch(
				"analytics.artifact_history.get_player_projection_daily_latest_path",
				return_value=paths["player_projection_daily"],
			), patch(
				"analytics.artifact_history.get_player_projection_weekly_latest_path",
				return_value=paths["player_projection_weekly"],
			), patch(
				"analytics.artifact_history.get_view_league_daily_latest_path",
				return_value=paths["view_league_daily"],
			), patch(
				"analytics.artifact_history.get_view_league_weekly_latest_path",
				return_value=paths["view_league_weekly"],
			), patch(
				"analytics.artifact_history.get_view_gm_daily_latest_path",
				return_value=paths["view_gm_daily"],
			), patch(
				"analytics.artifact_history.get_view_gm_weekly_latest_path",
				return_value=paths["view_gm_weekly"],
			), patch(
				"analytics.artifact_history.get_free_agent_candidates_latest_path",
				return_value=paths["free_agent_candidates"],
			), patch(
				"analytics.artifact_history.get_team_weekly_totals_latest_path",
				return_value=paths["team_weekly_totals"],
			), patch(
				"analytics.artifact_history.get_clap_v2_latest_path",
				return_value=paths["clap_v2"],
			), patch(
				"analytics.artifact_history.get_clap_player_history_latest_path",
				return_value=paths["clap_player_history"],
			), patch(
				"analytics.artifact_history.get_matchup_expectations_latest_path",
				return_value=paths["matchup_expectations"],
			), patch(
				"analytics.artifact_history.get_clap_calibration_latest_path",
				return_value=paths["clap_calibration"],
			), patch(
				"analytics.artifact_history.get_weekly_digest_latest_path",
				return_value=paths["weekly_digest_json"],
			), patch(
				"analytics.artifact_history.get_weekly_digest_latest_text_path",
				return_value=paths["weekly_digest_text"],
			), patch(
				"analytics.artifact_history.get_weekly_email_payload_latest_path",
				return_value=paths["weekly_email_payload"],
			), patch(
				"analytics.artifact_history.get_weekly_email_text_latest_path",
				return_value=paths["weekly_email_text"],
			):
				result = builder.build(datetime.datetime(2026, 3, 20), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertTrue(history_index_path.exists())
				payload = json.loads(history_index_path.read_text())
				self.assertEqual(payload["target_date"], "2026-03-20")
				self.assertGreaterEqual(result["summary"]["snapshotted_count"], 15)


if __name__ == "__main__":
	unittest.main()
