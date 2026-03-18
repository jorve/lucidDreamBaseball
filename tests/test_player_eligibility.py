import datetime
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.player_eligibility import PlayerEligibilityBuilder  # noqa: E402


class TestPlayerEligibility(unittest.TestCase):
	def setUp(self):
		self.builder = PlayerEligibilityBuilder()

	def test_parse_player_field_extracts_name_positions_team(self):
		row = self.builder._parse_player_field("Shohei Ohtani U,SP | LAD")
		self.assertEqual(row["name"], "Shohei Ohtani")
		self.assertEqual(row["positions"], ["SP", "U"])
		self.assertEqual(row["team_abbr"], "LAD")

	def test_slot_mapping_for_outfield_and_utility(self):
		slots = self.builder._derive_slot_positions({"RF"}, {"batters"})
		self.assertEqual(sorted(slots), ["OF", "RF", "U"])
		slots_cf = self.builder._derive_slot_positions({"CF"}, {"batters"})
		self.assertEqual(sorted(slots_cf), ["CF", "OF", "U"])

	def test_build_writes_latest_and_changes(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			batters = temp_root / "batters.csv"
			sp = temp_root / "sp.csv"
			rp = temp_root / "rp.csv"
			batters.write_text(
				"All Players   Projections Standard Categories\n"
				"Avail,Player,Rank,\n"
				"FA,\"Aaron Judge RF,OF | NYY\",1\n"
			)
			sp.write_text(
				"All Starting Pitchers   Projections Standard Categories\n"
				"Avail,Player,Rank,\n"
				"FA,\"Tarik Skubal SP | DET\",1\n"
			)
			rp.write_text(
				"All Relief Pitchers   Projections Standard Categories\n"
				"Avail,Player,Rank,\n"
				"FA,\"Mason Miller RP | SD\",1\n"
			)

			out_latest = temp_root / "player_eligibility_latest.json"
			out_changes = temp_root / "player_eligibility_changes_latest.json"
			with patch.object(
				self.builder,
				"_resolve_csv_sources",
				return_value=[("batters", batters), ("sp", sp), ("rp", rp)],
			), patch(
				"analytics.player_eligibility.get_player_eligibility_latest_path",
				return_value=out_latest,
			), patch(
				"analytics.player_eligibility.get_player_eligibility_changes_latest_path",
				return_value=out_changes,
			):
				result = self.builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertTrue(out_latest.exists())
				self.assertTrue(out_changes.exists())


if __name__ == "__main__":
	unittest.main()
