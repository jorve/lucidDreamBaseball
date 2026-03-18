import json
import pathlib
import subprocess
import sys
import unittest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.validators import (  # noqa: E402
	validate_roster_state_payload,
	validate_transactions_payload,
)


class TestIngestionPhase1Smoke(unittest.TestCase):
	def test_fixture_contracts_validate(self):
		fixtures_dir = PROJECT_ROOT / "fixtures"
		validate_transactions_payload(json.loads((fixtures_dir / "transactions_none.json").read_text()))
		validate_transactions_payload(json.loads((fixtures_dir / "transactions_add_drop.json").read_text()))
		validate_transactions_payload(json.loads((fixtures_dir / "transactions_trade.json").read_text()))
		validate_roster_state_payload(json.loads((fixtures_dir / "roster_base.json").read_text()))
		validate_roster_state_payload(json.loads((fixtures_dir / "expected_roster_after_trade.json").read_text()))

	def test_ingestion_dry_run_writes_phase1_summary_keys(self):
		cmd = [sys.executable, str(PROJECT_ROOT / "py" / "run_ingestion.py"), "--date", "2026-03-16", "--dry-run"]
		result = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False, capture_output=True, text=True)
		self.assertEqual(result.returncode, 0, msg=result.stdout + "\n" + result.stderr)

		log_path = PROJECT_ROOT / "logs" / "ingestion_2026-03-16.log"
		last_record = json.loads(log_path.read_text().splitlines()[-1])
		for key in ("transactions", "roster_state", "recompute_trigger", "status_index"):
			self.assertIn(key, last_record)

	def test_legacy_outputs_still_exist(self):
		required_paths = [
			PROJECT_ROOT / "data" / "2016" / "week12.json",
			PROJECT_ROOT / "data" / "2016" / "schedule.json",
			PROJECT_ROOT / "json" / "schedule.json",
		]
		for path_value in required_paths:
			self.assertTrue(path_value.exists(), msg=f"Missing legacy output: {path_value}")


if __name__ == "__main__":
	unittest.main()
