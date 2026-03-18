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


class TestClapV2(unittest.TestCase):
	def _write_inputs(self, root_dir, roster_payload, state_payload):
		projection_path = root_dir / "player_projection_weekly_latest.json"
		roster_path = root_dir / "roster_state_latest.json"
		weekly_totals_path = root_dir / "team_weekly_totals_latest.json"
		weekly_state_path = root_dir / "team_weekly_totals_state.json"
		projection_path.write_text(
			json.dumps(
				{
					"players": [
						{"player_id": "p1", "player_name": "A", "player_role": "batters", "aRBI_window": 12.0, "aSB_window": 3.0, "MGS_window": 0.0, "VIJAY_window": 0.0},
						{"player_id": "p2", "player_name": "B", "player_role": "batters", "aRBI_window": 2.0, "aSB_window": 1.0, "MGS_window": 0.0, "VIJAY_window": 0.0},
						{"player_id": "p3", "player_name": "C", "player_role": "rp", "aRBI_window": 0.0, "aSB_window": 0.0, "MGS_window": 9.0, "VIJAY_window": 3.0, "projected_appearances_window": 3.0},
						{"player_id": "p4", "player_name": "D", "player_role": "sp", "aRBI_window": 0.0, "aSB_window": 0.0, "MGS_window": 20.0, "VIJAY_window": 0.0, "projected_appearances_window": 2.0},
					]
				}
			)
		)
		roster_path.write_text(json.dumps(roster_payload))
		weekly_state_path.write_text(json.dumps(state_payload))
		weekly_totals_path.write_text(
			json.dumps(
				{
					"period_key": "period_1",
					"teams": [
						{"team_id": "1", "category_totals": {"aRBI": 20.0, "aSB": 4.0, "MGS": 25.0}},
						{"team_id": "2", "category_totals": {"aRBI": 10.0, "aSB": 2.0, "MGS": 10.0}},
					],
					"matchups": [
						{
							"matchup_id": "m1",
							"away_team_id": "1",
							"home_team_id": "2",
							"away_team_abbr": "T1",
							"home_team_abbr": "T2",
						}
					],
				}
			)
		)
		return projection_path, roster_path, weekly_totals_path, weekly_state_path

	def _state_payload(self):
		return {
			"periods": {
				"period_1": {
					"teams": {
						"1": {
							"players": {
								"p1": {
									"player_name": "A",
									"categories": {"aRBI": {"weekly_total": 8.0, "daily_values": {"2026-03-16": 5.0, "2026-03-17": 3.0}}},
								},
								"p2": {
									"player_name": "B",
									"categories": {"aRBI": {"weekly_total": 0.0, "daily_values": {}}},
								},
								"p3": {
									"player_name": "C",
									"categories": {
										"MGS": {"weekly_total": 11.0, "daily_values": {"2026-03-16": 6.0, "2026-03-18": 5.0}},
										"VIJAY": {"weekly_total": 3.0, "daily_values": {"2026-03-16": 1.0, "2026-03-18": 2.0}},
									},
								},
								"p4": {
									"player_name": "D",
									"categories": {
										"MGS": {"weekly_total": 30.0, "daily_values": {"2026-03-16": 20.0, "2026-03-21": 10.0}},
										"VIJAY": {"weekly_total": 0.0, "daily_values": {"2026-03-16": 0.0, "2026-03-21": 0.0}},
									},
									"derived_inputs": {
										"daily": {
											"2026-03-16": {"IP_OUTS": 18},
											"2026-03-21": {"IP_OUTS": 15},
										}
									},
								},
							}
						}
					}
				}
			}
		}

	def test_generates_engines_and_probability_bounds(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			state_payload = self._state_payload()
			roster_payload = {
				"teams": [
					{"team_id": "1", "team_name": "Team One", "players": [{"player_id": "p1"}, {"player_id": "p4"}]},
					{"team_id": "2", "team_name": "Team Two", "players": [{"player_id": "p2"}, {"player_id": "p3"}]},
				]
			}
			projection_path, roster_path, weekly_totals_path, weekly_state_path = self._write_inputs(temp_root, roster_payload, state_payload)
			clap_path = temp_root / "clap_v2_latest.json"
			history_path = temp_root / "clap_player_history_latest.json"
			matchup_path = temp_root / "matchup_expectations_latest.json"
			calibration_path = temp_root / "clap_calibration_latest.json"
			history_index_path = temp_root / "artifact_history_latest.json"
			history_index_path.write_text("{}")

			builder = ClapV2Builder()
			builder.clap_cfg.update({"selected_engine": "analytic_normal", "monte_carlo_samples": 800, "calibration_lookback_days": 7, "stabilization_samples_weekly": 6, "stabilization_samples_starts": 6})
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
				self.assertTrue(clap_path.exists())
				self.assertTrue(history_path.exists())
				self.assertTrue(matchup_path.exists())
				history_payload = json.loads(history_path.read_text())
				sp_row = next(row for row in history_payload["players"] if row["player_id"] == "p4")
				self.assertEqual(sp_row["role"], "sp")
				self.assertEqual(sp_row["per_start_samples"]["MGS"]["n_starts"], 2)
				self.assertEqual(sp_row["weekly_start_count_signal"]["observed"]["mu"], 2.0)
				rp_row = next(row for row in history_payload["players"] if row["player_id"] == "p3")
				self.assertEqual(rp_row["vijay_weekly_sum_samples"]["mu"], 3.0)
				self.assertEqual(rp_row["vijay_appearance_count"], 2)
				payload = json.loads(matchup_path.read_text())
				self.assertEqual(payload["summary"]["matchups"], 1)
				row = payload["matchups"][0]
				for engine_name in ("analytic_normal", "monte_carlo"):
					for category, probs in row["engines"][engine_name]["categories"].items():
						self.assertGreaterEqual(probs["away_win_prob"], 0.0, msg=category)
						self.assertLessEqual(probs["away_win_prob"], 1.0, msg=category)
						self.assertAlmostEqual(probs["away_win_prob"] + probs["home_win_prob"], 1.0, places=5)
				self.assertEqual(row["engines"]["analytic_normal"]["categories"]["VIJAY"]["category_source"], "appearance_summed")
				self.assertEqual(row["engines"]["analytic_normal"]["categories"]["aRBI"]["category_source"], "component_derived")
				clap_payload = json.loads(clap_path.read_text())
				team_one = next(team for team in clap_payload["teams"] if team["team_id"] == "1")
				self.assertGreaterEqual(team_one["expected_sp_starts_week"], 1.5)

	def test_trade_sensitivity_changes_category_probability(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			state_payload = self._state_payload()
			clap_path = temp_root / "clap_v2_latest.json"
			history_path = temp_root / "clap_player_history_latest.json"
			matchup_path = temp_root / "matchup_expectations_latest.json"
			calibration_path = temp_root / "clap_calibration_latest.json"
			history_index_path = temp_root / "artifact_history_latest.json"
			history_index_path.write_text("{}")

			roster_before = {
				"teams": [
					{"team_id": "1", "team_name": "Team One", "players": [{"player_id": "p1"}, {"player_id": "p4"}]},
					{"team_id": "2", "team_name": "Team Two", "players": [{"player_id": "p2"}, {"player_id": "p3"}]},
				]
			}
			projection_path, roster_path, weekly_totals_path, weekly_state_path = self._write_inputs(temp_root, roster_before, state_payload)
			builder = ClapV2Builder()
			builder.clap_cfg.update({"selected_engine": "analytic_normal", "monte_carlo_samples": 800})

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
				builder.build(datetime.datetime(2026, 3, 23), dry_run=False)
				before_payload = json.loads(matchup_path.read_text())
				before_prob = before_payload["matchups"][0]["engines"]["analytic_normal"]["categories"]["aRBI"]["away_win_prob"]

				# Trade elite aRBI hitter from away to home.
				roster_after = {
					"teams": [
						{"team_id": "1", "team_name": "Team One", "players": [{"player_id": "p2"}, {"player_id": "p4"}]},
						{"team_id": "2", "team_name": "Team Two", "players": [{"player_id": "p1"}, {"player_id": "p3"}]},
					]
				}
				roster_path.write_text(json.dumps(roster_after))
				builder.build(datetime.datetime(2026, 3, 23), dry_run=False)
				after_payload = json.loads(matchup_path.read_text())
				after_prob = after_payload["matchups"][0]["engines"]["analytic_normal"]["categories"]["aRBI"]["away_win_prob"]
				self.assertGreater(before_prob, after_prob)

	def test_low_sample_blending_keeps_weekly_mean_between_prior_and_observed(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			state_payload = self._state_payload()
			roster_payload = {"teams": [{"team_id": "1", "team_name": "Team One", "players": [{"player_id": "p1"}]}, {"team_id": "2", "team_name": "Team Two", "players": [{"player_id": "p2"}]}]}
			projection_path, roster_path, weekly_totals_path, weekly_state_path = self._write_inputs(temp_root, roster_payload, state_payload)
			clap_path = temp_root / "clap_v2_latest.json"
			history_path = temp_root / "clap_player_history_latest.json"
			matchup_path = temp_root / "matchup_expectations_latest.json"
			calibration_path = temp_root / "clap_calibration_latest.json"
			history_index_path = temp_root / "artifact_history_latest.json"
			history_index_path.write_text("{}")
			builder = ClapV2Builder()
			builder.clap_cfg.update({"selected_engine": "analytic_normal", "stabilization_samples_weekly": 6})
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
				builder.build(datetime.datetime(2026, 3, 23), dry_run=False)
			clap_payload = json.loads(clap_path.read_text())
			team_one = next(team for team in clap_payload["teams"] if team["team_id"] == "1")
			blended = team_one["categories"]["aRBI"]["mu"]
			# p1 prior aRBI_window is 12.0, observed weekly sample in fixture is 8.0.
			self.assertGreater(blended, 8.0)
			self.assertLess(blended, 12.0)


if __name__ == "__main__":
	unittest.main()
