import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.clap_v2 import ClapV2Builder  # noqa: E402


class TestMatchupExpectationsCalibration(unittest.TestCase):
	def test_calibration_recommends_better_engine_from_history(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			projection_path = temp_root / "player_projection_weekly_latest.json"
			roster_path = temp_root / "roster_state_latest.json"
			weekly_totals_path = temp_root / "team_weekly_totals_latest.json"
			weekly_state_path = temp_root / "team_weekly_totals_state.json"
			clap_path = temp_root / "clap_v2_latest.json"
			history_path = temp_root / "clap_player_history_latest.json"
			matchup_path = temp_root / "matchup_expectations_latest.json"
			calibration_path = temp_root / "clap_calibration_latest.json"
			history_index_path = temp_root / "artifact_history_latest.json"
			history_index_path.write_text("{}")

			projection_path.write_text(json.dumps({"players": [{"player_id": "p1", "player_role": "batters", "aRBI_window": 5.0}, {"player_id": "p2", "player_role": "batters", "aRBI_window": 1.0}]}))
			roster_path.write_text(json.dumps({"teams": [{"team_id": "1", "players": [{"player_id": "p1"}]}, {"team_id": "2", "players": [{"player_id": "p2"}]}]}))
			weekly_state_path.write_text(
				json.dumps(
					{
						"periods": {
							"period_1": {
								"teams": {
									"1": {
										"players": {
											"p1": {"categories": {"aRBI": {"weekly_total": 8.0, "daily_values": {"2026-03-16": 8.0}}}}
										}
									},
									"2": {
										"players": {
											"p2": {"categories": {"aRBI": {"weekly_total": 1.0, "daily_values": {"2026-03-16": 1.0}}}}
										}
									},
								}
							}
						}
					}
				)
			)
			weekly_totals_path.write_text(
				json.dumps(
					{
						"period_key": "period_1",
						"teams": [{"team_id": "1", "category_totals": {"aRBI": 8.0}}, {"team_id": "2", "category_totals": {"aRBI": 1.0}}],
						"matchups": [{"matchup_id": "m1", "away_team_id": "1", "home_team_id": "2"}],
					}
				)
			)

			# Historical snapshot where analytic is better calibrated than Monte Carlo.
			history_day = temp_root / "history" / "2026-03-20"
			history_day.mkdir(parents=True, exist_ok=True)
			history_day.joinpath("matchup_expectations_latest.json").write_text(
				json.dumps(
					{
						"matchups": [
							{
								"matchup_id": "m_hist",
								"engines": {
									"analytic_normal": {"categories": {"aRBI": {"away_win_prob": 0.95}}},
									"monte_carlo": {"categories": {"aRBI": {"away_win_prob": 0.05}}},
								},
							}
						]
					}
				)
			)
			history_day.joinpath("team_weekly_totals_latest.json").write_text(
				json.dumps(
					{
						"teams": [{"team_id": "1", "category_totals": {"aRBI": 10.0}}, {"team_id": "2", "category_totals": {"aRBI": 1.0}}],
						"matchups": [{"matchup_id": "m_hist", "away_team_id": "1", "home_team_id": "2"}],
					}
				)
			)

			builder = ClapV2Builder()
			builder.clap_cfg.update({"selected_engine": "auto", "calibration_lookback_days": 10})
			with patch("analytics.clap_v2.get_player_projection_weekly_latest_path", return_value=projection_path), patch(
				"analytics.clap_v2.get_roster_state_latest_path",
				return_value=roster_path,
			), patch(
				"analytics.clap_v2.get_team_weekly_totals_latest_path",
				return_value=weekly_totals_path,
			), patch(
				"analytics.clap_v2.get_team_weekly_totals_state_path",
				return_value=weekly_state_path,
			), patch(
				"analytics.clap_v2.get_clap_v2_latest_path",
				return_value=clap_path,
			), patch(
				"analytics.clap_v2.get_clap_player_history_latest_path",
				return_value=history_path,
			), patch(
				"analytics.clap_v2.get_matchup_expectations_latest_path",
				return_value=matchup_path,
			), patch(
				"analytics.clap_v2.get_clap_calibration_latest_path",
				return_value=calibration_path,
			), patch(
				"analytics.clap_v2.get_artifact_history_latest_path",
				return_value=history_index_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 23), dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(calibration_path.read_text())
				self.assertEqual(payload["engine_recommendation"]["recommended"], "analytic_normal")
				self.assertEqual(payload["engine_recommendation"]["selection_mode"], "auto")
				self.assertEqual(payload["engine_recommendation"]["selected"], "analytic_normal")
				self.assertIn("role_segments", payload["metrics"])
				self.assertIn("sp_start_buckets", payload["metrics"])
				self.assertIn("category_source_diagnostics", payload["metrics"])
				self.assertIn("VIJAY", payload["metrics"]["category_source_diagnostics"]["appearance_summed_categories"])


if __name__ == "__main__":
	unittest.main()
