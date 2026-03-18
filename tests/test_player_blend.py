import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.player_blend import PlayerBlendBuilder  # noqa: E402


class TestPlayerBlend(unittest.TestCase):
	def test_blend_generates_output(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			priors_path = temp_root / "preseason_player_priors.json"
			priors_path.write_text(
				json.dumps(
					{
						"players": [
							{"player_id": "12345", "player_name": "Player One", "prior_projection": 40.0},
							{"player_id": "67890", "player_name": "Player Two", "prior_projection": 30.0},
						]
					}
				)
			)
			raw_dir = temp_root / "raw" / "2026-03-16"
			raw_dir.mkdir(parents=True, exist_ok=True)
			raw_dir.joinpath("rosters_2026-03-16.json").write_text(
				json.dumps(
					{
						"body": {
							"rosters": {
								"teams": [
									{"players": [{"id": 12345, "ytd_points": 55.0}, {"id": 67890, "ytd_points": 20.0}]}
								]
							}
						}
					}
				)
			)
			out_path = temp_root / "player_projection_deltas_latest.json"
			builder = PlayerBlendBuilder()
			with patch("analytics.player_blend.get_preseason_player_priors_path", return_value=priors_path), patch(
				"analytics.player_blend.get_ingestion_raw_dir",
				side_effect=lambda date_value=None: (temp_root / "raw" / date_value.strftime("%Y-%m-%d")) if date_value else (temp_root / "raw"),
			), patch(
				"analytics.player_blend.get_player_projection_deltas_latest_path",
				return_value=out_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "ok")
				self.assertTrue(out_path.exists())
				payload = json.loads(out_path.read_text())
				self.assertEqual(payload["summary"]["players_with_priors"], 2)
				self.assertEqual(payload["summary"]["players_with_observed"], 2)
				self.assertIn("overperforming_count", payload["summary"])
				self.assertIn("underperforming_count", payload["summary"])
				player_rows = payload["players"]
				self.assertIn("performance_flag", player_rows[0])
				self.assertIn("performance_delta_pct", player_rows[0])
				self.assertIn("category_performance_flags", player_rows[0])
				self.assertIn("aRBI", player_rows[0]["category_performance_flags"])
				flags = {row["performance_flag"] for row in player_rows}
				self.assertTrue(flags.issubset({"overperforming", "underperforming", "on_track", "insufficient_data"}))

	def test_skips_when_priors_missing(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			builder = PlayerBlendBuilder()
			with patch("analytics.player_blend.get_preseason_player_priors_path", return_value=temp_root / "missing.json"), patch(
				"analytics.player_blend.get_player_projection_deltas_latest_path",
				return_value=temp_root / "player_projection_deltas_latest.json",
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "skipped")
				self.assertEqual(result["reason"], "PRESEASON_PRIORS_MISSING")

	def test_percent_thresholds_override_absolute_thresholds(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			priors_path = temp_root / "preseason_player_priors.json"
			priors_path.write_text(json.dumps({"players": [{"player_id": "100", "player_name": "Player Pct", "prior_projection": 100.0}]}))
			raw_dir = temp_root / "raw" / "2026-03-16"
			raw_dir.mkdir(parents=True, exist_ok=True)
			raw_dir.joinpath("rosters_2026-03-16.json").write_text(
				json.dumps({"body": {"rosters": {"teams": [{"players": [{"id": 100, "ytd_points": 110.0}]}]}}})
			)
			out_path = temp_root / "player_projection_deltas_latest.json"
			builder = PlayerBlendBuilder()
			builder.player_blend_cfg["overperform_threshold"] = 0.1
			builder.player_blend_cfg["underperform_threshold"] = 0.1
			builder.player_blend_cfg["performance_thresholds_percent"] = {
				"overall": {"over": 50.0, "under": 50.0},
				"aRBI": {"over": 15.0, "under": 15.0},
				"aSB": {"over": 20.0, "under": 20.0},
				"MGS": {"over": 12.0, "under": 12.0},
				"VIJAY": {"over": 20.0, "under": 20.0},
			}
			with patch("analytics.player_blend.get_preseason_player_priors_path", return_value=priors_path), patch(
				"analytics.player_blend.get_ingestion_raw_dir",
				side_effect=lambda date_value=None: (temp_root / "raw" / date_value.strftime("%Y-%m-%d")) if date_value else (temp_root / "raw"),
			), patch(
				"analytics.player_blend.get_player_projection_deltas_latest_path",
				return_value=out_path,
			):
				result = builder.build(datetime.datetime(2026, 3, 16), dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(out_path.read_text())
				self.assertEqual(payload["players"][0]["performance_flag"], "on_track")

	def test_observed_category_asb_uses_half_caught_stealing_penalty(self):
		builder = PlayerBlendBuilder()
		rates = builder._extract_observed_category_rates(
			{
				"G": 10,
				"RBI": 15,
				"GIDP": 3,
				"SB": 6,
				"CS": 4,
			}
		)
		self.assertAlmostEqual(rates["aRBI_per_app"], 1.2, places=6)
		self.assertAlmostEqual(rates["aSB_per_app"], 0.4, places=6)  # (6 - 0.5*4) / 10


if __name__ == "__main__":
	unittest.main()
