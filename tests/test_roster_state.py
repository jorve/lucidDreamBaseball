import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.roster_state import RosterStateBuilder  # noqa: E402


class TestRosterState(unittest.TestCase):
	def setUp(self):
		self.builder = RosterStateBuilder()
		self.target_date = datetime.datetime(2026, 3, 16)

	def _write_test_inputs(self, temp_root, transactions_fixture_name):
		date_dir = temp_root / "raw" / "2026-03-16"
		date_dir.mkdir(parents=True, exist_ok=True)
		roster_base = json.loads((PROJECT_ROOT / "fixtures" / "roster_base.json").read_text())
		roster_payload = {"body": {"rosters": {"teams": []}}}
		for team in roster_base["teams"]:
			team_payload = {
				"id": int(team["team_id"]),
				"name": team["team_name"],
				"players": [],
			}
			for player in team["players"]:
				team_payload["players"].append(
					{
						"id": int(player["player_id"]),
						"fullname": player["player_name"],
						"eligible": player.get("positions", []),
						"roster_status": player.get("status", "active"),
					}
				)
			roster_payload["body"]["rosters"]["teams"].append(team_payload)
		(date_dir / "rosters_2026-03-16.json").write_text(json.dumps(roster_payload))

		transactions_fixture = json.loads((PROJECT_ROOT / "fixtures" / transactions_fixture_name).read_text())
		(temp_root / "transactions_latest.json").write_text(json.dumps(transactions_fixture))

	def test_trade_applies_atomically(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			self._write_test_inputs(temp_root, "transactions_trade.json")
			with patch(
				"analytics.roster_state.get_ingestion_raw_dir",
				side_effect=lambda date_value=None: (temp_root / "raw" / date_value.strftime("%Y-%m-%d")) if date_value else (temp_root / "raw"),
			), patch(
				"analytics.roster_state.get_transactions_latest_path",
				return_value=temp_root / "transactions_latest.json",
			), patch(
				"analytics.roster_state.get_roster_state_latest_path",
				return_value=temp_root / "roster_state_latest.json",
			), patch(
				"analytics.roster_state.get_roster_state_diagnostics_latest_path",
				return_value=temp_root / "roster_state_diagnostics_latest.json",
			):
				result = self.builder.build(self.target_date, dry_run=False)
				self.assertEqual(result["events_applied"], 1)
				payload = json.loads((temp_root / "roster_state_latest.json").read_text())
				team_players = {team["team_id"]: {p["player_id"] for p in team["players"]} for team in payload["teams"]}
				self.assertIn("12345", team_players["9"])
				self.assertIn("67890", team_players["17"])

	def test_bad_trade_is_quarantined(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			self._write_test_inputs(temp_root, "transactions_trade.json")
			bad_txn = json.loads((temp_root / "transactions_latest.json").read_text())
			bad_txn["events"][0]["players"][0]["player_id"] = "999999"
			(temp_root / "transactions_latest.json").write_text(json.dumps(bad_txn))
			with patch(
				"analytics.roster_state.get_ingestion_raw_dir",
				side_effect=lambda date_value=None: (temp_root / "raw" / date_value.strftime("%Y-%m-%d")) if date_value else (temp_root / "raw"),
			), patch(
				"analytics.roster_state.get_transactions_latest_path",
				return_value=temp_root / "transactions_latest.json",
			), patch(
				"analytics.roster_state.get_roster_state_latest_path",
				return_value=temp_root / "roster_state_latest.json",
			), patch(
				"analytics.roster_state.get_roster_state_diagnostics_latest_path",
				return_value=temp_root / "roster_state_diagnostics_latest.json",
			):
				result = self.builder.build(self.target_date, dry_run=False)
				self.assertEqual(result["events_applied"], 0)
				self.assertEqual(result["events_quarantined"], 1)


if __name__ == "__main__":
	unittest.main()
