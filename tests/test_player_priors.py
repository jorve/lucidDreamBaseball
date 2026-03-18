import datetime
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.player_priors import PlayerPriorBuilder  # noqa: E402


class TestPlayerPriors(unittest.TestCase):
	def test_resolve_path_expands_year_token(self):
		builder = PlayerPriorBuilder()
		resolved = builder._resolve_path("data/{year}/preseason/batter_priors.csv", 2026)
		self.assertTrue(str(resolved).endswith("data\\2026\\preseason\\batter_priors.csv"))

	def test_loads_priors_from_multiple_csvs(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			batters_path = temp_root / "batters.csv"
			sp_path = temp_root / "sp.csv"
			rp_path = temp_root / "rp.csv"
			batters_path.write_text("player_id,player_name,projection\n1,A,10\n")
			sp_path.write_text("player_id,player_name,projection\n2,B,20\n")
			rp_path.write_text("player_id,player_name,projection\n3,C,30\n")
			out_path = temp_root / "preseason_player_priors.json"

			builder = PlayerPriorBuilder()
			with patch.object(
				builder,
				"_resolve_csv_sources",
				return_value=[("batters", batters_path), ("sp", sp_path), ("rp", rp_path)],
			), patch(
				"analytics.player_priors.get_preseason_player_priors_path",
				return_value=out_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertEqual(result["summary"]["players_loaded"], 3)
				self.assertTrue(out_path.exists())
				payload = out_path.read_text()
				self.assertIn("source_csvs", payload)
				self.assertIn("player_role", payload)

	def test_skips_when_all_csvs_missing(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			out_path = temp_root / "preseason_player_priors.json"
			builder = PlayerPriorBuilder()
			with patch.object(
				builder,
				"_resolve_csv_sources",
				return_value=[("batters", temp_root / "missing_batters.csv"), ("sp", temp_root / "missing_sp.csv"), ("rp", temp_root / "missing_rp.csv")],
			), patch(
				"analytics.player_priors.get_preseason_player_priors_path",
				return_value=out_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "skipped")
				self.assertEqual(result["reason"], "PRESEASON_CSVS_MISSING")

	def test_batter_derived_asb_uses_half_caught_stealing_penalty(self):
		builder = PlayerPriorBuilder()
		derived = builder._derived_scoring_fields(
			source_key="batters",
			row={"RBI": "50", "GIDP": "10", "SB": "12", "CS": "4"},
			projected_appearances=100,
		)
		self.assertEqual(derived["aRBI_total"], 40.0)
		self.assertEqual(derived["aSB_total"], 10.0)  # 12 - 0.5*4
		self.assertEqual(derived["aSB_per_app"], 0.1)


if __name__ == "__main__":
	unittest.main()
