import datetime
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.status_index import write_ingestion_status_index  # noqa: E402


class TestStatusIndex(unittest.TestCase):
	def test_emits_eligibility_codes_when_changes_present(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			index_path = temp_root / "ingestion_status_latest.json"
			transactions_path = temp_root / "transactions_latest.json"
			transactions_path.write_text(json.dumps({"events": [], "summary": {"events_new": 0}}))
			run_summary = {
				"status": "ok",
				"auth": {"status": "ok"},
				"fetch": {"status": "ok"},
				"normalize": {"status": "ok"},
				"transactions": {"status": "ok"},
				"roster_state": {"status": "ok", "integrity": {"status": "ok"}},
				"recompute_trigger": {"status": "ok"},
				"player_priors": {"status": "ok"},
				"player_eligibility": {
					"status": "ok",
					"changes_summary": {"added_count": 2, "removed_count": 1, "updated_count": 3},
				},
				"player_blend": {"status": "ok"},
			}
			with patch("analytics.status_index.get_ingestion_status_latest_path", return_value=index_path), patch(
				"analytics.status_index.get_transactions_latest_path",
				return_value=transactions_path,
			), patch(
				"analytics.status_index.get_ingestion_config",
				return_value={"health_max_age_hours": 30, "transaction_health_max_age_hours": 168},
			):
				result = write_ingestion_status_index(datetime.datetime(2026, 3, 16), run_summary, dry_run=False)
				self.assertEqual(result["status"], "ok")
				payload = json.loads(index_path.read_text())
				self.assertIn("ELIGIBILITY_UPDATED", payload["codes"])
				self.assertIn("ELIGIBILITY_ADDED", payload["codes"])
				self.assertIn("ELIGIBILITY_REMOVED", payload["codes"])
				self.assertTrue(payload["eligibility_changes"]["has_changes"])


if __name__ == "__main__":
	unittest.main()
