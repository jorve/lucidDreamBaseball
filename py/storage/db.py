import json
import sqlite3
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from contextlib import closing

from project_config import get_storage_config, get_storage_db_path


UTC = timezone.utc


class StorageWriteError(RuntimeError):
	pass


def _now_utc():
	return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalize_target_date(payload):
	if isinstance(payload, dict):
		value = payload.get("target_date")
		if value is not None:
			return str(value)
	return None


def _payload_hash(payload):
	try:
		serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
	except TypeError:
		serialized = json.dumps(str(payload), separators=(",", ":"), ensure_ascii=False)
	return sha256(serialized.encode("utf-8")).hexdigest(), serialized


class StorageRecorder:
	def __init__(self, force_enabled=False):
		self.cfg = get_storage_config()
		mode = str(self.cfg.get("mode", "json_only")).strip().lower()
		self.mode = mode
		self.enabled = bool(force_enabled or self.cfg.get("enabled", False) or mode in {"dual_write", "db_primary"})
		self.strict_writes = bool(self.cfg.get("strict_writes", False))
		self.db_path = get_storage_db_path()

	def _connect(self):
		return sqlite3.connect(str(self.db_path))

	def _ensure_schema(self, conn):
		conn.executescript(
			"""
			CREATE TABLE IF NOT EXISTS artifact_writes (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				artifact_path TEXT NOT NULL,
				artifact_name TEXT NOT NULL,
				artifact_kind TEXT NOT NULL,
				target_date TEXT,
				write_source TEXT,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_artifact_writes_name ON artifact_writes(artifact_name, recorded_at_utc DESC);
			CREATE INDEX IF NOT EXISTS idx_artifact_writes_target ON artifact_writes(target_date, artifact_name);

			CREATE TABLE IF NOT EXISTS run_events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				event_type TEXT NOT NULL,
				status TEXT NOT NULL,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS transaction_ledger_snapshots (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS roster_state_snapshots (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS team_weekly_totals_state_snapshots (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);

			CREATE TABLE IF NOT EXISTS clap_output_snapshots (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				clap_artifact TEXT NOT NULL,
				payload_hash TEXT NOT NULL,
				payload_json TEXT NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_clap_output_snapshots_type ON clap_output_snapshots(clap_artifact, recorded_at_utc DESC);

			CREATE TABLE IF NOT EXISTS team_season_player_totals (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at_utc TEXT NOT NULL,
				target_date TEXT,
				team_id TEXT,
				team_name TEXT,
				player_id TEXT,
				player_name TEXT,
				player_status TEXT,
				category_totals_json TEXT NOT NULL,
				payload_hash TEXT NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_team_season_player_totals_lookup ON team_season_player_totals(target_date, team_id, player_id);
			"""
		)

	def _handle_error(self, error, context):
		if self.strict_writes:
			raise StorageWriteError(f"{context}: {error}") from error
		return {"status": "failed", "error": str(error), "context": context}

	def _extract_team_entries(self, teams_value):
		if isinstance(teams_value, list):
			return teams_value
		if isinstance(teams_value, dict):
			return list(teams_value.values())
		return []

	def _extract_player_entries(self, players_value):
		if isinstance(players_value, list):
			return players_value
		if isinstance(players_value, dict):
			return list(players_value.values())
		return []

	def _write_team_season_player_totals(self, conn, payload_hash, payload, target_date):
		season_roto = payload.get("season_roto", {}) if isinstance(payload, dict) else {}
		teams = self._extract_team_entries(season_roto.get("teams"))
		if not teams and isinstance(payload, dict):
			teams = self._extract_team_entries(payload.get("teams"))
		recorded_at = _now_utc()
		rows = []
		for team in teams:
			if not isinstance(team, dict):
				continue
			team_id = str(team.get("team_id", "") or "")
			team_name = str(team.get("team_name") or team.get("team_abbr") or team_id or "")
			for player in self._extract_player_entries(team.get("players")):
				if not isinstance(player, dict):
					continue
				rows.append(
					(
						recorded_at,
						target_date,
						team_id,
						team_name,
						str(player.get("player_id", "") or ""),
						str(player.get("player_name", "") or ""),
						str(player.get("status", "") or ""),
						json.dumps(player.get("category_totals", {}), ensure_ascii=False, sort_keys=True),
						payload_hash,
					)
				)
		if not rows:
			return 0
		conn.executemany(
			"""
			INSERT INTO team_season_player_totals
			(recorded_at_utc, target_date, team_id, team_name, player_id, player_name, player_status, category_totals_json, payload_hash)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			""",
			rows,
		)
		return len(rows)

	def _write_pilot_snapshots(self, conn, artifact_name, target_date, payload_hash, payload_json, payload):
		recorded_at = _now_utc()
		if artifact_name == "transactions_latest.json":
			conn.execute(
				"INSERT INTO transaction_ledger_snapshots (recorded_at_utc, target_date, payload_hash, payload_json) VALUES (?, ?, ?, ?)",
				(recorded_at, target_date, payload_hash, payload_json),
			)
		elif artifact_name == "roster_state_latest.json":
			conn.execute(
				"INSERT INTO roster_state_snapshots (recorded_at_utc, target_date, payload_hash, payload_json) VALUES (?, ?, ?, ?)",
				(recorded_at, target_date, payload_hash, payload_json),
			)
		elif artifact_name == "team_weekly_totals_state.json":
			conn.execute(
				"INSERT INTO team_weekly_totals_state_snapshots (recorded_at_utc, target_date, payload_hash, payload_json) VALUES (?, ?, ?, ?)",
				(recorded_at, target_date, payload_hash, payload_json),
			)
		elif artifact_name in {
			"clap_v2_latest.json",
			"matchup_expectations_latest.json",
			"clap_calibration_latest.json",
			"clap_player_history_latest.json",
		}:
			conn.execute(
				"INSERT INTO clap_output_snapshots (recorded_at_utc, target_date, clap_artifact, payload_hash, payload_json) VALUES (?, ?, ?, ?, ?)",
				(recorded_at, target_date, artifact_name, payload_hash, payload_json),
			)

		if artifact_name == "team_weekly_totals_latest.json":
			return self._write_team_season_player_totals(conn, payload_hash, payload, target_date)
		return 0

	def record_json_artifact(self, path_value, payload, artifact_kind="analytics", write_source="unknown", target_date=None):
		if not self.enabled:
			return {"status": "skipped", "reason": "storage_disabled"}
		try:
			path_obj = Path(path_value)
			artifact_path = str(path_obj)
			artifact_name = path_obj.name
			payload_hash, payload_json = _payload_hash(payload)
			target = str(target_date) if target_date is not None else _normalize_target_date(payload)
			recorded_at = _now_utc()
			with closing(self._connect()) as conn:
				self._ensure_schema(conn)
				conn.execute(
					"""
					INSERT INTO artifact_writes
					(recorded_at_utc, artifact_path, artifact_name, artifact_kind, target_date, write_source, payload_hash, payload_json)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					""",
					(recorded_at, artifact_path, artifact_name, artifact_kind, target, write_source, payload_hash, payload_json),
				)
				season_rows = self._write_pilot_snapshots(conn, artifact_name, target, payload_hash, payload_json, payload)
				conn.commit()
			return {
				"status": "ok",
				"artifact_name": artifact_name,
				"artifact_kind": artifact_kind,
				"payload_hash": payload_hash,
				"target_date": target,
				"season_player_rows_written": season_rows,
			}
		except Exception as error:
			return self._handle_error(error, "record_json_artifact")

	def record_run_event(self, event_type, payload, target_date=None, status="ok"):
		if not self.enabled:
			return {"status": "skipped", "reason": "storage_disabled"}
		try:
			payload_hash, payload_json = _payload_hash(payload)
			recorded_at = _now_utc()
			with closing(self._connect()) as conn:
				self._ensure_schema(conn)
				conn.execute(
					"""
					INSERT INTO run_events
					(recorded_at_utc, target_date, event_type, status, payload_hash, payload_json)
					VALUES (?, ?, ?, ?, ?, ?)
					""",
					(recorded_at, str(target_date) if target_date is not None else None, str(event_type), str(status), payload_hash, payload_json),
				)
				conn.commit()
			return {"status": "ok", "event_type": event_type, "payload_hash": payload_hash}
		except Exception as error:
			return self._handle_error(error, "record_run_event")

