import argparse
import json
import os
import shutil
from datetime import datetime, timedelta, timezone

from analytics.artifact_history import ArtifactHistoryBuilder, ArtifactHistoryError
from analytics.clap_v2 import ClapV2Builder, ClapV2Error
from analytics.free_agent_candidates import FreeAgentCandidatesBuilder, FreeAgentCandidatesError
from analytics.player_eligibility import PlayerEligibilityBuilder, PlayerEligibilityError
from analytics.player_blend import PlayerBlendBuilder, PlayerBlendError
from analytics.projection_horizons import ProjectionHorizonBuilder, ProjectionHorizonError
from analytics.player_priors import PlayerPriorBuilder, PlayerPriorError
from analytics.recompute_trigger import RecomputeTriggerBuilder, RecomputeTriggerError
from analytics.roster_state import RosterStateBuilder, RosterStateError
from analytics.status_index import StatusIndexError, write_ingestion_status_index
from analytics.team_weekly_totals import TeamWeeklyTotalsBuilder, TeamWeeklyTotalsError
from analytics.transaction_ledger import TransactionLedgerBuilder, TransactionLedgerError
from analytics.view_models import ViewModelBuilder, ViewModelError
from analytics.weekly_digest import WeeklyDigestBuilder, WeeklyDigestError
from analytics.weekly_email import WeeklyEmailBuilder, WeeklyEmailError
from analytics.weekly_calibration import WeeklyCalibrationBuilder, WeeklyCalibrationError
from ingestion.auth import AuthError, AuthManager
from ingestion.fetch_cbs_data import CbsApiFetcher, FetchError
from ingestion.normalize import IngestionNormalizer, NormalizeError
from project_config import (
	get_ingestion_config,
	get_ingestion_raw_dir,
	get_ingestion_run_log_path,
	get_ingestion_state_dir,
	get_storage_config,
)
from storage import StorageRecorder, StorageWriteError


UTC = timezone.utc
STORAGE_RECORDER = StorageRecorder()


class IngestionRunLockError(RuntimeError):
	pass


def _parse_iso_utc(value):
	if not value:
		return None
	try:
		dt_value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
		if dt_value.tzinfo is None:
			dt_value = dt_value.replace(tzinfo=UTC)
		return dt_value.astimezone(UTC)
	except Exception:
		return None


def _run_lock_path():
	return get_ingestion_state_dir() / "ingestion_run.lock"


def acquire_run_lock(dry_run=False):
	ingestion_cfg = get_ingestion_config()
	lock_cfg = ingestion_cfg.get("run_lock", {})
	path_value = _run_lock_path()
	if dry_run or not lock_cfg.get("enabled", True):
		return {"enabled": False, "acquired": False, "path": path_value}
	stale_hours = float(lock_cfg.get("stale_hours", 8))
	now_utc = datetime.now(UTC)
	payload = {
		"pid": os.getpid(),
		"acquired_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
	}

	def _write_lock():
		with path_value.open("x") as outfile:
			json.dump(payload, outfile)

	try:
		_write_lock()
	except FileExistsError:
		existing = {}
		try:
			with path_value.open() as infile:
				existing = json.load(infile)
		except Exception:
			existing = {}
		acquired_at = _parse_iso_utc(existing.get("acquired_at_utc"))
		is_stale = acquired_at is not None and (now_utc - acquired_at) > timedelta(hours=stale_hours)
		if is_stale:
			try:
				path_value.unlink(missing_ok=True)
			except Exception:
				pass
			try:
				_write_lock()
			except FileExistsError as error:
				raise IngestionRunLockError("INGESTION_RUN_LOCKED") from error
		else:
			lock_pid = existing.get("pid")
			raise IngestionRunLockError(f"INGESTION_RUN_LOCKED: pid={lock_pid}")
	return {"enabled": True, "acquired": True, "path": path_value}


def release_run_lock(lock_info):
	if not lock_info or not lock_info.get("enabled") or not lock_info.get("acquired"):
		return
	path_value = lock_info.get("path")
	try:
		path_value.unlink(missing_ok=True)
	except Exception:
		pass


def parse_args():
	parser = argparse.ArgumentParser(description="Run nightly CBS ingestion workflow.")
	parser.add_argument("--date", help="Target date in YYYY-MM-DD format. Default is yesterday.")
	parser.add_argument("--dry-run", action="store_true", help="Plan ingestion actions without network or writes.")
	parser.add_argument("--skip-auth", action="store_true", help="Skip auth refresh and use cached session.")
	parser.add_argument("--skip-normalize", action="store_true", help="Fetch raw snapshots only.")
	parser.add_argument("--force-auth-refresh", action="store_true", help="Force auth refresh even if cache is valid.")
	return parser.parse_args()


def resolve_target_date(date_arg):
	if date_arg:
		return datetime.strptime(date_arg, "%Y-%m-%d")
	return datetime.now() - timedelta(days=1)


def append_run_log(target_date, payload):
	log_path = get_ingestion_run_log_path(target_date)
	with log_path.open("a") as logfile:
		logfile.write(json.dumps(payload) + "\n")
	STORAGE_RECORDER.record_run_event(
		event_type="ingestion_run_log",
		target_date=target_date.strftime("%Y-%m-%d"),
		status=payload.get("status", "unknown"),
		payload=payload,
	)


def cleanup_old_raw_snapshots(target_date, dry_run=False):
	ingestion_cfg = get_ingestion_config()
	retention_days = int(ingestion_cfg.get("retention_days", 45))
	raw_root = get_ingestion_raw_dir()
	cutoff_date = target_date - timedelta(days=retention_days)
	deleted_dirs = []
	skipped_dirs = []

	for child in raw_root.iterdir():
		if not child.is_dir():
			continue
		try:
			child_date = datetime.strptime(child.name, "%Y-%m-%d")
		except ValueError:
			skipped_dirs.append(str(child))
			continue
		if child_date < cutoff_date:
			deleted_dirs.append(str(child))
			if not dry_run:
				shutil.rmtree(child, ignore_errors=True)

	return {
		"retention_days": retention_days,
		"cutoff_date": cutoff_date.strftime("%Y-%m-%d"),
		"deleted_dirs": deleted_dirs,
		"skipped_dirs": skipped_dirs,
	}


def main():
	args = parse_args()
	target_date = resolve_target_date(args.date)
	lock_info = None
	run_summary = {
		"target_date": target_date.strftime("%Y-%m-%d"),
		"dry_run": args.dry_run,
		"skip_auth": args.skip_auth,
		"skip_normalize": args.skip_normalize,
	}
	storage_cfg = get_storage_config()
	storage_mode = str(storage_cfg.get("mode", "json_only"))
	storage_enabled = bool(storage_cfg.get("enabled", False) or storage_mode in {"dual_write", "db_primary"})
	run_summary["storage"] = {
		"status": "ok" if storage_enabled else "skipped",
		"mode": storage_mode,
		"db_path": str(STORAGE_RECORDER.db_path),
	}
	lock_info = acquire_run_lock(dry_run=args.dry_run)
	run_summary["run_lock"] = {"status": "ok" if lock_info.get("acquired") else "skipped"}
	STORAGE_RECORDER.record_run_event(
		event_type="run_lock",
		target_date=target_date.strftime("%Y-%m-%d"),
		status=run_summary["run_lock"]["status"],
		payload=run_summary["run_lock"],
	)

	try:
		auth_manager = AuthManager()
		auth_session = auth_manager.get_session(
			force_refresh=args.force_auth_refresh,
			dry_run=args.dry_run,
			skip_auth=args.skip_auth,
		)
		run_summary["auth"] = {"status": "ok", "updated_at": auth_session.updated_at.isoformat()}
		STORAGE_RECORDER.record_run_event(
			event_type="auth",
			target_date=target_date.strftime("%Y-%m-%d"),
			status="ok",
			payload=run_summary["auth"],
		)

		fetcher = CbsApiFetcher()
		fetch_result = fetcher.fetch_all(target_date=target_date, auth_session=auth_session, dry_run=args.dry_run)
		run_summary["fetch"] = {"status": "ok", "raw_dir": str(fetch_result["raw_dir"])}
		STORAGE_RECORDER.record_run_event(
			event_type="fetch",
			target_date=target_date.strftime("%Y-%m-%d"),
			status="ok",
			payload=run_summary["fetch"],
		)
		if args.dry_run:
			run_summary["team_weekly_totals"] = {"status": "dry_run"}
		else:
			team_weekly_totals_builder = TeamWeeklyTotalsBuilder()
			team_weekly_totals_result = team_weekly_totals_builder.build(target_date=target_date, dry_run=args.dry_run)
			run_summary["team_weekly_totals"] = {"status": team_weekly_totals_result.get("status", "unknown")}
			if "output_path" in team_weekly_totals_result:
				run_summary["team_weekly_totals"]["output"] = str(team_weekly_totals_result["output_path"])
			if "state_path" in team_weekly_totals_result:
				run_summary["team_weekly_totals"]["state_output"] = str(team_weekly_totals_result["state_path"])
			if "reason" in team_weekly_totals_result:
				run_summary["team_weekly_totals"]["reason"] = team_weekly_totals_result["reason"]
			if "summary" in team_weekly_totals_result:
				run_summary["team_weekly_totals"]["summary"] = team_weekly_totals_result["summary"]

		ingestion_cfg = get_ingestion_config()
		if args.skip_normalize:
			run_summary["normalize"] = {"status": "skipped"}
			run_summary["transactions"] = {"status": "skipped"}
			run_summary["roster_state"] = {"status": "skipped"}
			run_summary["recompute_trigger"] = {"status": "skipped"}
			run_summary["player_priors"] = {"status": "skipped"}
			run_summary["player_eligibility"] = {"status": "skipped"}
			run_summary["player_blend"] = {"status": "skipped"}
			run_summary["projection_horizons"] = {"status": "skipped"}
			run_summary["view_models"] = {"status": "skipped"}
			run_summary["clap_v2"] = {"status": "skipped"}
			run_summary["free_agent_candidates"] = {"status": "skipped"}
			run_summary["weekly_digest"] = {"status": "skipped"}
			run_summary["weekly_email"] = {"status": "skipped"}
			run_summary["weekly_calibration"] = {"status": "skipped"}
			run_summary["artifact_history"] = {"status": "skipped"}
		else:
			normalizer = IngestionNormalizer()
			normalize_result = normalizer.normalize(target_date=target_date, dry_run=args.dry_run)
			run_summary["normalize"] = {"status": "ok", "outputs": {k: str(v) for k, v in normalize_result["outputs"].items()}}
			STORAGE_RECORDER.record_run_event(
				event_type="normalize",
				target_date=target_date.strftime("%Y-%m-%d"),
				status="ok",
				payload=run_summary["normalize"],
			)

			transactions_cfg = ingestion_cfg.get("transactions", {})
			if args.dry_run:
				run_summary["transactions"] = {"status": "dry_run"}
				run_summary["roster_state"] = {"status": "dry_run"}
				run_summary["recompute_trigger"] = {"status": "dry_run"}
				run_summary["player_priors"] = {"status": "dry_run"}
				run_summary["player_eligibility"] = {"status": "dry_run"}
				run_summary["player_blend"] = {"status": "dry_run"}
				run_summary["projection_horizons"] = {"status": "dry_run"}
				run_summary["view_models"] = {"status": "dry_run"}
				run_summary["clap_v2"] = {"status": "dry_run"}
				run_summary["free_agent_candidates"] = {"status": "dry_run"}
				run_summary["weekly_digest"] = {"status": "dry_run"}
				run_summary["weekly_email"] = {"status": "dry_run"}
				run_summary["weekly_calibration"] = {"status": "dry_run"}
				run_summary["artifact_history"] = {"status": "dry_run"}
			elif transactions_cfg.get("enabled", True):
				ledger_builder = TransactionLedgerBuilder()
				ledger_result = ledger_builder.build(target_date=target_date, dry_run=args.dry_run)
				run_summary["transactions"] = {
					"status": "ok",
					"output": str(ledger_result["output_path"]),
					"summary": ledger_result["summary"],
				}
				roster_builder = RosterStateBuilder()
				roster_result = roster_builder.build(target_date=target_date, dry_run=args.dry_run)
				run_summary["roster_state"] = {
					"status": "ok",
					"output": str(roster_result["output_path"]),
					"diagnostics": str(roster_result["diagnostics_path"]),
					"events_applied": roster_result["events_applied"],
					"events_quarantined": roster_result["events_quarantined"],
					"integrity": roster_result["integrity"],
				}

				recompute_cfg = ingestion_cfg.get("recompute", {})
				if recompute_cfg.get("trigger_enabled", True):
					trigger_builder = RecomputeTriggerBuilder()
					trigger_result = trigger_builder.build(
						target_date=target_date,
						dry_run=args.dry_run,
						transaction_summary=ledger_result.get("summary"),
						roster_integrity=roster_result.get("integrity"),
					)
					run_summary["recompute_trigger"] = {
						"status": "ok",
						"output": str(trigger_result["output_path"]),
						"triggered": trigger_result["triggered"],
						"recommended_scope": trigger_result["recommended_scope"],
						"fallback_full_recompute": trigger_result["fallback_full_recompute"],
						"affected_team_count": trigger_result["affected_team_count"],
						"affected_player_count": trigger_result["affected_player_count"],
					}
				else:
					run_summary["recompute_trigger"] = {"status": "skipped"}

				player_prior_builder = PlayerPriorBuilder()
				priors_result = player_prior_builder.build(target_date=target_date, dry_run=args.dry_run)
				run_summary["player_priors"] = {
					"status": priors_result["status"],
					"output": str(priors_result["output_path"]),
				}
				if "reason" in priors_result:
					run_summary["player_priors"]["reason"] = priors_result["reason"]
				if "summary" in priors_result:
					run_summary["player_priors"]["summary"] = priors_result["summary"]

				eligibility_builder = PlayerEligibilityBuilder()
				eligibility_result = eligibility_builder.build(target_date=target_date, dry_run=args.dry_run)
				run_summary["player_eligibility"] = {
					"status": eligibility_result["status"],
					"output": str(eligibility_result["output_path"]),
					"changes_output": str(eligibility_result["changes_path"]),
				}
				if "reason" in eligibility_result:
					run_summary["player_eligibility"]["reason"] = eligibility_result["reason"]
				if "summary" in eligibility_result:
					run_summary["player_eligibility"]["summary"] = eligibility_result["summary"]
				if "changes_summary" in eligibility_result:
					run_summary["player_eligibility"]["changes_summary"] = eligibility_result["changes_summary"]

				if priors_result["status"] == "ok":
					player_blend_builder = PlayerBlendBuilder()
					blend_result = player_blend_builder.build(target_date=target_date, dry_run=args.dry_run)
					run_summary["player_blend"] = {
						"status": blend_result["status"],
						"output": str(blend_result["output_path"]),
					}
					if "reason" in blend_result:
						run_summary["player_blend"]["reason"] = blend_result["reason"]
					if "summary" in blend_result:
						run_summary["player_blend"]["summary"] = blend_result["summary"]
					if blend_result["status"] == "ok":
						horizon_builder = ProjectionHorizonBuilder()
						horizon_result = horizon_builder.build(target_date=target_date, dry_run=args.dry_run)
						run_summary["projection_horizons"] = {
							"status": horizon_result["status"],
							"daily_output": str(horizon_result["daily_output_path"]),
							"weekly_output": str(horizon_result["weekly_output_path"]),
						}
						if "reason" in horizon_result:
							run_summary["projection_horizons"]["reason"] = horizon_result["reason"]
						if "summary" in horizon_result:
							run_summary["projection_horizons"]["summary"] = horizon_result["summary"]
						if horizon_result["status"] == "ok":
							view_model_builder = ViewModelBuilder()
							view_model_result = view_model_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["view_models"] = {
								"status": view_model_result["status"],
								"output": str(view_model_result.get("outputs", {}).get("view_league_daily", "")),
								"outputs": {k: str(v) for k, v in view_model_result.get("outputs", {}).items()},
							}
							if "reason" in view_model_result:
								run_summary["view_models"]["reason"] = view_model_result["reason"]
							if "summary" in view_model_result:
								run_summary["view_models"]["summary"] = view_model_result["summary"]
							clap_builder = ClapV2Builder()
							clap_result = clap_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["clap_v2"] = {
								"status": clap_result["status"],
								"output": str(clap_result["output_path"]),
								"player_history_output": str(clap_result.get("player_history_output_path", "")),
								"matchup_output": str(clap_result["matchup_output_path"]),
								"calibration_output": str(clap_result["calibration_output_path"]),
							}
							if "reason" in clap_result:
								run_summary["clap_v2"]["reason"] = clap_result["reason"]
							if "summary" in clap_result:
								run_summary["clap_v2"]["summary"] = clap_result["summary"]
							free_agent_builder = FreeAgentCandidatesBuilder()
							free_agent_result = free_agent_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["free_agent_candidates"] = {
								"status": free_agent_result["status"],
								"output": str(free_agent_result["output_path"]),
							}
							if "reason" in free_agent_result:
								run_summary["free_agent_candidates"]["reason"] = free_agent_result["reason"]
							if "summary" in free_agent_result:
								run_summary["free_agent_candidates"]["summary"] = free_agent_result["summary"]
							weekly_digest_builder = WeeklyDigestBuilder()
							digest_result = weekly_digest_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["weekly_digest"] = {
								"status": digest_result["status"],
								"output": str(digest_result["output_path"]),
								"text_output": str(digest_result["text_output_path"]),
							}
							if "reason" in digest_result:
								run_summary["weekly_digest"]["reason"] = digest_result["reason"]
							if "summary" in digest_result:
								run_summary["weekly_digest"]["summary"] = digest_result["summary"]
							weekly_email_builder = WeeklyEmailBuilder()
							email_result = weekly_email_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["weekly_email"] = {
								"status": email_result["status"],
								"output": str(email_result["output_path"]),
								"text_output": str(email_result["text_output_path"]),
							}
							if "reason" in email_result:
								run_summary["weekly_email"]["reason"] = email_result["reason"]
							calibration_builder = WeeklyCalibrationBuilder()
							calibration_result = calibration_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["weekly_calibration"] = {
								"status": calibration_result["status"],
								"output": str(calibration_result["output_path"]),
							}
							if "reason" in calibration_result:
								run_summary["weekly_calibration"]["reason"] = calibration_result["reason"]
							if "summary" in calibration_result:
								run_summary["weekly_calibration"]["summary"] = calibration_result["summary"]
							artifact_history_builder = ArtifactHistoryBuilder()
							history_result = artifact_history_builder.build(target_date=target_date, dry_run=args.dry_run)
							run_summary["artifact_history"] = {
								"status": history_result["status"],
								"output": str(history_result["output_path"]),
							}
							if "reason" in history_result:
								run_summary["artifact_history"]["reason"] = history_result["reason"]
							if "summary" in history_result:
								run_summary["artifact_history"]["summary"] = history_result["summary"]
						else:
							run_summary["view_models"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["clap_v2"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["free_agent_candidates"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["weekly_digest"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["weekly_email"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["weekly_calibration"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
							run_summary["artifact_history"] = {"status": "skipped", "reason": "PROJECTION_HORIZONS_NOT_READY"}
					else:
						run_summary["projection_horizons"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["view_models"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["clap_v2"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["free_agent_candidates"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["weekly_digest"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["weekly_email"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["weekly_calibration"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
						run_summary["artifact_history"] = {"status": "skipped", "reason": "PLAYER_BLEND_NOT_READY"}
				else:
					run_summary["player_blend"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["projection_horizons"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["view_models"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["clap_v2"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["free_agent_candidates"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["weekly_digest"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["weekly_email"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["weekly_calibration"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
					run_summary["artifact_history"] = {"status": "skipped", "reason": "PLAYER_PRIORS_NOT_READY"}
			else:
				run_summary["transactions"] = {"status": "skipped"}
				run_summary["roster_state"] = {"status": "skipped"}
				run_summary["recompute_trigger"] = {"status": "skipped"}
				run_summary["player_priors"] = {"status": "skipped"}
				run_summary["player_eligibility"] = {"status": "skipped"}
				run_summary["player_blend"] = {"status": "skipped"}
				run_summary["projection_horizons"] = {"status": "skipped"}
				run_summary["view_models"] = {"status": "skipped"}
				run_summary["clap_v2"] = {"status": "skipped"}
				run_summary["free_agent_candidates"] = {"status": "skipped"}
				run_summary["weekly_digest"] = {"status": "skipped"}
				run_summary["weekly_email"] = {"status": "skipped"}
				run_summary["weekly_calibration"] = {"status": "skipped"}
				run_summary["artifact_history"] = {"status": "skipped"}

		run_summary["retention_cleanup"] = cleanup_old_raw_snapshots(target_date=target_date, dry_run=args.dry_run)
	except (
		IngestionRunLockError,
		AuthError,
		FetchError,
		NormalizeError,
		TransactionLedgerError,
		TeamWeeklyTotalsError,
		RosterStateError,
		RecomputeTriggerError,
		PlayerPriorError,
		PlayerEligibilityError,
		PlayerBlendError,
		ProjectionHorizonError,
		ClapV2Error,
		ViewModelError,
		FreeAgentCandidatesError,
		WeeklyDigestError,
		WeeklyEmailError,
		WeeklyCalibrationError,
		ArtifactHistoryError,
		StorageWriteError,
		ValueError,
	) as error:
		run_summary["status"] = "failed"
		run_summary["error"] = str(error)
		try:
			status_result = write_ingestion_status_index(
				target_date=target_date,
				run_summary=run_summary,
				dry_run=args.dry_run,
			)
			run_summary["status_index"] = {"status": "ok", "output": str(status_result["output_path"]), "codes": status_result["codes"]}
		except StatusIndexError as status_error:
			run_summary["status_index"] = {"status": "failed", "error_short": str(status_error)}
		append_run_log(target_date, run_summary)
		STORAGE_RECORDER.record_run_event(
			event_type="pipeline_failed",
			target_date=target_date.strftime("%Y-%m-%d"),
			status="failed",
			payload={"error": str(error)},
		)
		print(f"Ingestion failed: {error}")
		release_run_lock(lock_info)
		return 1

	run_summary["status"] = "ok"
	try:
		status_result = write_ingestion_status_index(
			target_date=target_date,
			run_summary=run_summary,
			dry_run=args.dry_run,
		)
		run_summary["status_index"] = {"status": "ok", "output": str(status_result["output_path"]), "codes": status_result["codes"]}
	except StatusIndexError as error:
		run_summary["status_index"] = {"status": "failed", "error_short": str(error)}
	append_run_log(target_date, run_summary)
	STORAGE_RECORDER.record_run_event(
		event_type="pipeline_complete",
		target_date=target_date.strftime("%Y-%m-%d"),
		status="ok",
		payload={"status_index": run_summary.get("status_index", {})},
	)
	release_run_lock(lock_info)
	print("Ingestion completed successfully.")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
