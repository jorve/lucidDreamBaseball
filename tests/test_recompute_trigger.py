import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.recompute_trigger import RecomputeTriggerBuilder  # noqa: E402


class TestRecomputeTrigger(unittest.TestCase):
	def setUp(self):
		self.builder = RecomputeTriggerBuilder()
		self.target_date = datetime.datetime(2026, 3, 16)

	def test_triggered_when_new_events_present(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			(temp_root / "transactions_latest.json").write_text((PROJECT_ROOT / "fixtures" / "transactions_trade.json").read_text())
			(temp_root / "roster_state_diagnostics_latest.json").write_text(
				json.dumps({"integrity_checks": {"status": "ok", "duplicate_player_assignments": 0, "atomic_trade_failures": 0}})
			)
			with patch("analytics.recompute_trigger.get_transactions_latest_path", return_value=temp_root / "transactions_latest.json"), patch(
				"analytics.recompute_trigger.get_roster_state_diagnostics_latest_path",
				return_value=temp_root / "roster_state_diagnostics_latest.json",
			), patch(
				"analytics.recompute_trigger.get_recompute_request_latest_path",
				return_value=temp_root / "recompute_request_latest.json",
			):
				result = self.builder.build(self.target_date, dry_run=False)
				self.assertTrue(result["triggered"])
				self.assertEqual(result["recommended_scope"], "incremental")

	def test_fallback_full_when_integrity_error(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			(temp_root / "transactions_latest.json").write_text((PROJECT_ROOT / "fixtures" / "transactions_trade.json").read_text())
			(temp_root / "roster_state_diagnostics_latest.json").write_text(
				json.dumps({"integrity_checks": {"status": "error", "duplicate_player_assignments": 1}})
			)
			with patch("analytics.recompute_trigger.get_transactions_latest_path", return_value=temp_root / "transactions_latest.json"), patch(
				"analytics.recompute_trigger.get_roster_state_diagnostics_latest_path",
				return_value=temp_root / "roster_state_diagnostics_latest.json",
			), patch(
				"analytics.recompute_trigger.get_recompute_request_latest_path",
				return_value=temp_root / "recompute_request_latest.json",
			):
				result = self.builder.build(self.target_date, dry_run=False)
				self.assertTrue(result["fallback_full_recompute"])
				self.assertEqual(result["recommended_scope"], "full")


if __name__ == "__main__":
	unittest.main()
