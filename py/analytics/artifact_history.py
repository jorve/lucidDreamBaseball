import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analytics.io import write_json
from project_config import (
	get_artifact_history_latest_path,
	get_clap_calibration_latest_path,
	get_clap_player_history_latest_path,
	get_clap_v2_latest_path,
	get_free_agent_candidates_latest_path,
	get_ingestion_config,
	get_matchup_expectations_latest_path,
	get_player_projection_daily_latest_path,
	get_player_projection_weekly_latest_path,
	get_team_weekly_totals_latest_path,
	get_view_gm_daily_latest_path,
	get_view_gm_weekly_latest_path,
	get_view_league_daily_latest_path,
	get_view_league_weekly_latest_path,
	get_weekly_digest_latest_path,
	get_weekly_digest_latest_text_path,
	get_weekly_calibration_latest_path,
	get_weekly_email_payload_latest_path,
	get_weekly_email_text_latest_path,
)


UTC = timezone.utc


class ArtifactHistoryError(RuntimeError):
	pass


class ArtifactHistoryBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.history_cfg = self.ingestion_cfg.get("history", {})

	def build(self, target_date, dry_run=False):
		output_path = get_artifact_history_latest_path()
		if not self.history_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "ARTIFACT_HISTORY_DISABLED", "output_path": output_path}

		retention_days = int(self.history_cfg.get("retention_days", 180))
		history_root = output_path.parent / "history" / target_date.strftime("%Y-%m-%d")
		entries = []
		for name, source_path in self._artifact_sources().items():
			source = Path(source_path)
			if not source.exists():
				entries.append({"artifact": name, "status": "missing", "source": str(source), "snapshot": ""})
				continue
			dest = history_root / source.name
			entries.append({"artifact": name, "status": "snapshotted", "source": str(source), "snapshot": str(dest)})
			if not dry_run:
				dest.parent.mkdir(parents=True, exist_ok=True)
				shutil.copy2(source, dest)

		cleanup_result = self._cleanup_history(output_path.parent / "history", retention_days, dry_run=dry_run)
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"history_root": str(history_root),
			"retention_days": retention_days,
			"artifacts": entries,
			"cleanup": cleanup_result,
		}
		if not dry_run:
			write_json(output_path, payload)
		return {
			"status": "ok",
			"output_path": output_path,
			"summary": {
				"snapshotted_count": len([row for row in entries if row["status"] == "snapshotted"]),
				"missing_count": len([row for row in entries if row["status"] == "missing"]),
				"deleted_dirs": len(cleanup_result.get("deleted_dirs", [])),
			},
		}

	def _artifact_sources(self):
		return {
			"player_projection_daily": get_player_projection_daily_latest_path(),
			"player_projection_weekly": get_player_projection_weekly_latest_path(),
			"view_league_daily": get_view_league_daily_latest_path(),
			"view_league_weekly": get_view_league_weekly_latest_path(),
			"view_gm_daily": get_view_gm_daily_latest_path(),
			"view_gm_weekly": get_view_gm_weekly_latest_path(),
			"free_agent_candidates": get_free_agent_candidates_latest_path(),
			"team_weekly_totals": get_team_weekly_totals_latest_path(),
			"clap_v2": get_clap_v2_latest_path(),
			"clap_player_history": get_clap_player_history_latest_path(),
			"matchup_expectations": get_matchup_expectations_latest_path(),
			"clap_calibration": get_clap_calibration_latest_path(),
			"weekly_digest_json": get_weekly_digest_latest_path(),
			"weekly_digest_text": get_weekly_digest_latest_text_path(),
			"weekly_calibration": get_weekly_calibration_latest_path(),
			"weekly_email_payload": get_weekly_email_payload_latest_path(),
			"weekly_email_text": get_weekly_email_text_latest_path(),
		}

	def _cleanup_history(self, history_root, retention_days, dry_run=False):
		if not history_root.exists():
			return {"deleted_dirs": [], "skipped_dirs": []}
		now = datetime.now(UTC).date()
		deleted_dirs = []
		skipped_dirs = []
		for child in history_root.iterdir():
			if not child.is_dir():
				continue
			try:
				child_date = datetime.strptime(child.name, "%Y-%m-%d").date()
			except Exception:
				skipped_dirs.append(str(child))
				continue
			if (now - child_date).days > retention_days:
				deleted_dirs.append(str(child))
				if not dry_run:
					shutil.rmtree(child, ignore_errors=True)
		return {"deleted_dirs": deleted_dirs, "skipped_dirs": skipped_dirs}
