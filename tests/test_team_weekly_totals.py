import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.team_weekly_totals import TeamWeeklyTotalsBuilder  # noqa: E402


def _live_payload(team_a_values, team_b_values):
	def cats(values):
		return [
			{"name": "HR", "value": values["HR"], "is_bad": "false"},
			{"name": "R", "value": values["R"], "is_bad": "false"},
			{"name": "ERA", "value": values["ERA"], "is_bad": "true"},
			{"name": "MGS", "value": values["MGS"], "is_bad": "false"},
			{"name": "VIJAY", "value": values["VIJAY"], "is_bad": "false"},
		]

	return {
		"body": {
			"live_scoring": {
				"teams": [
					{"id": "1", "name": "Team One", "long_abbr": "ONE", "categories": cats(team_a_values)},
					{"id": "2", "name": "Team Two", "long_abbr": "TWO", "categories": cats(team_b_values)},
				]
			}
		}
	}


def _schedule_payload():
	return {
		"body": {
			"schedule": {
				"periods": [
					{
						"id": "1",
						"label": "Period 1",
						"start": "3/25/26",
						"end": "3/29/26",
						"matchups": [
							{
								"id": "1",
								"away_team": {"id": "1", "long_abbr": "ONE", "name": "Team One"},
								"home_team": {"id": "2", "long_abbr": "TWO", "name": "Team Two"},
							}
						],
					}
				]
			}
		}
	}


def _schedule_payload_with_period(period_id):
	return {
		"body": {
			"schedule": {
				"periods": [
					{
						"id": str(period_id),
						"label": f"Period {period_id}",
						"start": "3/25/26",
						"end": "3/29/26",
						"matchups": [
							{
								"id": "1",
								"away_team": {"id": "1", "long_abbr": "ONE", "name": "Team One"},
								"home_team": {"id": "2", "long_abbr": "TWO", "name": "Team Two"},
							}
						],
					}
				]
			}
		}
	}


class TestTeamWeeklyTotals(unittest.TestCase):
	def test_skips_when_target_date_outside_scoring_period(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 16)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(
				json.dumps(
					_live_payload(
						{"HR": 2, "R": 5, "ERA": 3.0, "MGS": 10, "VIJAY": 4},
						{"HR": 1, "R": 4, "ERA": 4.0, "MGS": 8, "VIJAY": 2},
					)
				)
			)
			(raw_root / target_day_str / f"schedule_{target_day_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				result = builder.build(target_date=target_day, dry_run=False)

			self.assertEqual(result["status"], "skipped")
			self.assertEqual(result["reason"], "OUTSIDE_SCORING_SEASON")
			self.assertTrue(state_path.exists())
			self.assertFalse(latest_path.exists())

	def test_tied_category_awards_home_team(self):
		builder = TeamWeeklyTotalsBuilder()
		score = builder._compute_matchup_score(
			away_row={"category_totals": {"HR": 2.0, "ERA": 3.5}},
			home_row={"category_totals": {"HR": 2.0, "ERA": 3.5}},
		)
		self.assertEqual(score["away"], 0.0)
		self.assertEqual(score["home"], 12.0)

	def test_accumulates_daily_values_and_is_idempotent(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			day_one = datetime.datetime(2026, 3, 25)
			day_two = datetime.datetime(2026, 3, 26)
			day_one_str = day_one.strftime("%Y-%m-%d")
			day_two_str = day_two.strftime("%Y-%m-%d")
			(raw_root / day_one_str).mkdir(parents=True, exist_ok=True)
			(raw_root / day_two_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			(raw_root / day_one_str / f"live_scoring_{day_one_str}.json").write_text(
				json.dumps(
					_live_payload(
						{"HR": 2, "R": 5, "ERA": 3.0, "MGS": 10, "VIJAY": 4},
						{"HR": 1, "R": 4, "ERA": 4.0, "MGS": 8, "VIJAY": 2},
					)
				)
			)
			(raw_root / day_two_str / f"live_scoring_{day_two_str}.json").write_text(
				json.dumps(
					_live_payload(
						{"HR": 1, "R": 3, "ERA": 2.0, "MGS": 7, "VIJAY": 1},
						{"HR": 0, "R": 1, "ERA": 3.0, "MGS": 5, "VIJAY": 1},
					)
				)
			)
			(raw_root / day_one_str / f"schedule_{day_one_str}.json").write_text(json.dumps(_schedule_payload()))
			(raw_root / day_two_str / f"schedule_{day_two_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=day_one, dry_run=False)
				result_day_two = builder.build(target_date=day_two, dry_run=False)
				builder.build(target_date=day_two, dry_run=False)

			self.assertEqual(result_day_two["status"], "ok")
			payload = json.loads(latest_path.read_text())
			self.assertEqual(payload["period"]["id"], "1")
			teams = {row["team_id"]: row for row in payload["teams"]}
			self.assertAlmostEqual(teams["1"]["category_totals"]["HR"], 3.0)
			self.assertAlmostEqual(teams["1"]["category_totals"]["MGS"], 17.0)
			self.assertAlmostEqual(teams["1"]["category_totals"]["VIJAY"], 5.0)
			self.assertAlmostEqual(teams["2"]["category_totals"]["HR"], 1.0)
			self.assertEqual(teams["1"]["days_captured"], 2)
			matchups = payload.get("matchups", [])
			self.assertEqual(len(matchups), 1)
			self.assertIn("away", matchups[0]["score"])
			self.assertIn("home", matchups[0]["score"])

	def test_team_totals_count_only_active_player_stats(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 25)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			live_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "101",
										"fullname": "Active Hitter",
										"status": "Active",
										"stats_today": "2 HR, 3 R, 1 aRBI",
										"stats_period": "2 HR, 3 R, 1 aRBI",
									},
									{
										"id": "102",
										"fullname": "Bench Hitter",
										"status": "Reserve",
										"stats_today": "5 HR, 8 R, 4 aRBI",
										"stats_period": "5 HR, 8 R, 4 aRBI",
									},
								],
								"categories": [],
							},
							{
								"id": "2",
								"name": "Team Two",
								"long_abbr": "TWO",
								"players": [],
								"categories": [],
							},
						]
					}
				}
			}

			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(json.dumps(live_payload))
			(raw_root / target_day_str / f"schedule_{target_day_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=target_day, dry_run=False)

			payload = json.loads(latest_path.read_text())
			team_one = next(row for row in payload["teams"] if row["team_id"] == "1")
			self.assertEqual(team_one["category_totals"].get("HR"), 2.0)
			self.assertEqual(team_one["category_totals"].get("R"), 3.0)
			self.assertEqual(team_one["category_totals"].get("aRBI"), 1.0)
			players = {p["player_id"]: p for p in team_one["players"]}
			self.assertTrue(players["101"]["counted_for_team_totals"])
			self.assertFalse(players["102"]["counted_for_team_totals"])

	def test_target_date_not_duplicated_across_periods(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 25)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(
				json.dumps(
					_live_payload(
						{"HR": 2, "R": 5, "ERA": 3.0, "MGS": 10, "VIJAY": 4},
						{"HR": 1, "R": 4, "ERA": 4.0, "MGS": 8, "VIJAY": 2},
					)
				)
			)
			schedule_path = raw_root / target_day_str / f"schedule_{target_day_str}.json"
			schedule_path.write_text(json.dumps(_schedule_payload_with_period(1)))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=target_day, dry_run=False)
				# Simulate schedule period remap for same date, then rerun.
				schedule_path.write_text(json.dumps(_schedule_payload_with_period(2)))
				builder.build(target_date=target_day, dry_run=False)

			state_payload = json.loads(state_path.read_text())
			date_occurrences = 0
			for period_state in state_payload.get("periods", {}).values():
				for team_state in period_state.get("teams", {}).values():
					for category_state in team_state.get("categories", {}).values():
						if target_day_str in category_state.get("daily_values", {}):
							date_occurrences += 1
			# For two teams, target date should appear exactly once per team/category
			# and should not double on period remap rerun.
			self.assertEqual(date_occurrences, 16)

	def test_rate_categories_recomputed_from_underlying_components(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			day_one = datetime.datetime(2026, 3, 25)
			day_two = datetime.datetime(2026, 3, 26)
			day_one_str = day_one.strftime("%Y-%m-%d")
			day_two_str = day_two.strftime("%Y-%m-%d")
			(raw_root / day_one_str).mkdir(parents=True, exist_ok=True)
			(raw_root / day_two_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			day_one_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "101",
										"fullname": "Active Hitter",
										"status": "Active",
										"stats_today": "1-2, BB, 0.6667 OBP, 1.1667 OPS",
										"stats_period": "1-2, BB, 0.6667 OBP, 1.1667 OPS",
									}
								],
								"categories": [],
							},
							{"id": "2", "name": "Team Two", "long_abbr": "TWO", "players": [], "categories": []},
						]
					}
				}
			}
			day_two_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "101",
										"fullname": "Active Hitter",
										"status": "Active",
										"stats_today": "0-2, 0.0000 OBP, 0.0000 OPS",
										"stats_period": "1-4, BB, 0.4000 OBP, 0.6500 OPS",
									}
								],
								"categories": [],
							},
							{"id": "2", "name": "Team Two", "long_abbr": "TWO", "players": [], "categories": []},
						]
					}
				}
			}

			(raw_root / day_one_str / f"live_scoring_{day_one_str}.json").write_text(json.dumps(day_one_payload))
			(raw_root / day_two_str / f"live_scoring_{day_two_str}.json").write_text(json.dumps(day_two_payload))
			(raw_root / day_one_str / f"schedule_{day_one_str}.json").write_text(json.dumps(_schedule_payload()))
			(raw_root / day_two_str / f"schedule_{day_two_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=day_one, dry_run=False)
				builder.build(target_date=day_two, dry_run=False)

			payload = json.loads(latest_path.read_text())
			team_one = next(row for row in payload["teams"] if row["team_id"] == "1")
			self.assertAlmostEqual(team_one["category_totals"]["OBP"], 0.4, places=6)

	def test_mgs_vijay_sum_across_appearances(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 25)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			live_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "201",
										"fullname": "Pitcher One",
										"status": "Active",
										"stats_today": "5 INN, 10 MGS, 2 VIJAY",
										"stats_period": "5 INN, 10 MGS, 2 VIJAY",
									},
									{
										"id": "202",
										"fullname": "Pitcher Two",
										"status": "Active",
										"stats_today": "4 INN, 20 MGS, 4 VIJAY",
										"stats_period": "4 INN, 20 MGS, 4 VIJAY",
									},
								],
								"categories": [],
							},
							{"id": "2", "name": "Team Two", "long_abbr": "TWO", "players": [], "categories": []},
						]
					}
				}
			}
			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(json.dumps(live_payload))
			(raw_root / target_day_str / f"schedule_{target_day_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=target_day, dry_run=False)

			payload = json.loads(latest_path.read_text())
			team_one = next(row for row in payload["teams"] if row["team_id"] == "1")
			self.assertAlmostEqual(team_one["category_totals"]["MGS"], 30.0, places=6)
			self.assertAlmostEqual(team_one["category_totals"]["VIJAY"], 6.0, places=6)

	def test_awhip_includes_hit_batters(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 25)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			live_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "301",
										"fullname": "Pitcher HB",
										"status": "Active",
										"stats_today": "3 INN, 3 HA, HB, 2 BBI, 6 K, ER, 3.000 ERA, 2 aWHIP",
										"stats_period": "3 INN, 3 HA, HB, 2 BBI, 6 K, ER, 3.000 ERA, 2 aWHIP",
									}
								],
								"categories": [],
							},
							{"id": "2", "name": "Team Two", "long_abbr": "TWO", "players": [], "categories": []},
						]
					}
				}
			}
			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(json.dumps(live_payload))
			(raw_root / target_day_str / f"schedule_{target_day_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=target_day, dry_run=False)

			payload = json.loads(latest_path.read_text())
			team_one = next(row for row in payload["teams"] if row["team_id"] == "1")
			self.assertAlmostEqual(team_one["category_totals"]["aWHIP"], 2.0, places=6)

	def test_season_roto_includes_all_players_and_recomputes_player_rates(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			raw_root = temp_root / "raw"
			json_root = temp_root / "json"
			target_day = datetime.datetime(2026, 3, 25)
			target_day_str = target_day.strftime("%Y-%m-%d")
			(raw_root / target_day_str).mkdir(parents=True, exist_ok=True)
			json_root.mkdir(parents=True, exist_ok=True)

			live_payload = {
				"body": {
					"live_scoring": {
						"teams": [
							{
								"id": "1",
								"name": "Team One",
								"long_abbr": "ONE",
								"players": [
									{
										"id": "401",
										"fullname": "Active Batter",
										"status": "Active",
										"stats_today": "1-2, BB, HR, 2 R, 1 aRBI",
										"stats_period": "1-2, BB, HR, 2 R, 1 aRBI",
									},
									{
										"id": "402",
										"fullname": "Reserve RP",
										"status": "Reserve",
										"stats_today": "4 INN, 14 MGS, 3 VIJAY, 2 HA, 1 BBI, 1 ER",
										"stats_period": "4 INN, 14 MGS, 3 VIJAY, 2 HA, 1 BBI, 1 ER",
									},
								],
								"categories": [],
							},
							{"id": "2", "name": "Team Two", "long_abbr": "TWO", "players": [], "categories": []},
						]
					}
				}
			}
			(raw_root / target_day_str / f"live_scoring_{target_day_str}.json").write_text(json.dumps(live_payload))
			(raw_root / target_day_str / f"schedule_{target_day_str}.json").write_text(json.dumps(_schedule_payload()))

			state_path = json_root / "team_weekly_totals_state.json"
			latest_path = json_root / "team_weekly_totals_latest.json"
			builder = TeamWeeklyTotalsBuilder()

			def fake_raw_dir(date_value=None):
				if date_value is None:
					return raw_root
				return raw_root / date_value.strftime("%Y-%m-%d")

			with patch("analytics.team_weekly_totals.get_ingestion_raw_dir", side_effect=fake_raw_dir), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_state_path",
				return_value=state_path,
			), patch(
				"analytics.team_weekly_totals.get_team_weekly_totals_latest_path",
				return_value=latest_path,
			):
				builder.build(target_date=target_day, dry_run=False)

			payload = json.loads(latest_path.read_text())
			season_team_one = next(row for row in payload["season_roto"]["teams"] if row["team_id"] == "1")
			players = {row["player_id"]: row for row in season_team_one["players"]}
			self.assertIn("401", players)
			self.assertIn("402", players)
			# Batter ratios from components: (H+BB)/(AB+BB) = (1+1)/(2+1) = 0.666667.
			self.assertAlmostEqual(players["401"]["category_totals"]["OBP"], 2.0 / 3.0, places=6)
			# RP per-appearance categories are season sums.
			self.assertAlmostEqual(players["402"]["category_totals"]["MGS"], 14.0, places=6)
			self.assertAlmostEqual(players["402"]["category_totals"]["VIJAY"], 3.0, places=6)


if __name__ == "__main__":
	unittest.main()
