import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys
from contextlib import closing

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from storage import StorageRecorder


class StorageDualWriteTests(unittest.TestCase):
	def setUp(self):
		self.temp_dir = tempfile.TemporaryDirectory()
		self.db_path = Path(self.temp_dir.name) / "storage_test.db"
		self.recorder = StorageRecorder(force_enabled=True)
		self.recorder.db_path = self.db_path

	def tearDown(self):
		self.temp_dir.cleanup()

	def test_team_weekly_totals_writes_player_season_rows(self):
		payload = {
			"target_date": "2026-03-16",
			"season_roto": {
				"teams": [
					{
						"team_id": "1",
						"team_name": "Alpha",
						"players": [
							{
								"player_id": "101",
								"player_name": "Player One",
								"status": "Active",
								"category_totals": {"HR": 3, "R": 8, "MGS": 12.5, "VIJAY": 4.0},
							},
							{
								"player_id": "102",
								"player_name": "Player Two",
								"status": "Reserve",
								"category_totals": {"HR": 1, "R": 2},
							},
						],
					}
				]
			},
		}

		result = self.recorder.record_json_artifact(
			path_value=Path("json/team_weekly_totals_latest.json"),
			payload=payload,
			artifact_kind="analytics",
			write_source="test",
		)
		self.assertEqual(result.get("status"), "ok")
		self.assertEqual(result.get("season_player_rows_written"), 2)

		with closing(sqlite3.connect(str(self.db_path))) as conn:
			artifact_count = conn.execute("SELECT COUNT(*) FROM artifact_writes").fetchone()[0]
			season_count = conn.execute("SELECT COUNT(*) FROM team_season_player_totals").fetchone()[0]
		self.assertEqual(artifact_count, 1)
		self.assertEqual(season_count, 2)

	def test_clap_artifact_writes_snapshot(self):
		payload = {
			"target_date": "2026-03-16",
			"schema_version": "1.0",
			"metrics": {"analytic_normal": {"brier_score": 0.21}},
		}
		result = self.recorder.record_json_artifact(
			path_value=Path("json/clap_calibration_latest.json"),
			payload=payload,
			artifact_kind="analytics",
			write_source="test",
		)
		self.assertEqual(result.get("status"), "ok")

		with closing(sqlite3.connect(str(self.db_path))) as conn:
			clap_count = conn.execute("SELECT COUNT(*) FROM clap_output_snapshots").fetchone()[0]
		self.assertEqual(clap_count, 1)


if __name__ == "__main__":
	unittest.main()
