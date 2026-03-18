import json
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from run_ingestion import IngestionRunLockError, acquire_run_lock, release_run_lock  # noqa: E402


UTC = timezone.utc


class TestRunLock(unittest.TestCase):
	def test_acquire_and_release_lock(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			with patch("run_ingestion.get_ingestion_state_dir", return_value=temp_root), patch(
				"run_ingestion.get_ingestion_config",
				return_value={"run_lock": {"enabled": True, "stale_hours": 8}},
			):
				lock_info = acquire_run_lock(dry_run=False)
				self.assertTrue(lock_info["acquired"])
				lock_path = temp_root / "ingestion_run.lock"
				self.assertTrue(lock_path.exists())
				release_run_lock(lock_info)
				self.assertFalse(lock_path.exists())

	def test_existing_fresh_lock_blocks(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			lock_path = temp_root / "ingestion_run.lock"
			lock_path.write_text(
				json.dumps(
					{
						"pid": 1234,
						"acquired_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
					}
				)
			)
			with patch("run_ingestion.get_ingestion_state_dir", return_value=temp_root), patch(
				"run_ingestion.get_ingestion_config",
				return_value={"run_lock": {"enabled": True, "stale_hours": 8}},
			):
				with self.assertRaises(IngestionRunLockError):
					acquire_run_lock(dry_run=False)

	def test_stale_lock_is_replaced(self):
		with tempfile.TemporaryDirectory() as temp_dir:
			temp_root = pathlib.Path(temp_dir)
			lock_path = temp_root / "ingestion_run.lock"
			stale_time = (datetime.now(UTC) - timedelta(hours=12)).isoformat().replace("+00:00", "Z")
			lock_path.write_text(json.dumps({"pid": 9999, "acquired_at_utc": stale_time}))
			with patch("run_ingestion.get_ingestion_state_dir", return_value=temp_root), patch(
				"run_ingestion.get_ingestion_config",
				return_value={"run_lock": {"enabled": True, "stale_hours": 8}},
			):
				lock_info = acquire_run_lock(dry_run=False)
				self.assertTrue(lock_info["acquired"])
				payload = json.loads(lock_path.read_text())
				self.assertNotEqual(payload.get("pid"), 9999)
				release_run_lock(lock_info)


if __name__ == "__main__":
	unittest.main()
