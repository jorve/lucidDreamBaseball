from datetime import datetime, timezone

from analytics.io import read_json, write_json
from analytics.validators import ValidationError, validate_ingestion_status_payload
from project_config import (
	get_ingestion_config,
	get_ingestion_status_latest_path,
	get_transactions_latest_path,
)


UTC = timezone.utc


class StatusIndexError(RuntimeError):
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


def _resource_row(name, entry, fallback_error=None):
	status = "skipped"
	path_value = None
	error_short = None
	if isinstance(entry, dict):
		status = entry.get("status", status)
		path_value = (
			entry.get("output")
			or entry.get("raw_dir")
			or entry.get("diagnostics")
			or entry.get("path")
		)
		error_short = entry.get("error_short")
	if status == "dry_run":
		status = "skipped"
	if status == "ok":
		error_short = None
	elif error_short is None:
		error_short = fallback_error or f"{name.upper()}_{status.upper()}"
	return {
		"name": name,
		"status": status,
		"path": str(path_value) if path_value else "",
		"error_short": error_short,
	}


def _transaction_stream_snapshot(now_utc, max_age_hours):
	path_value = get_transactions_latest_path()
	if not path_value.exists():
		return {
			"status": "unknown",
			"max_age_hours": max_age_hours,
			"last_event_utc": None,
			"freshness_hours": None,
			"event_count": 0,
		}
	payload = read_json(path_value)
	events = payload.get("events", [])
	if not events:
		return {
			"status": "unknown",
			"max_age_hours": max_age_hours,
			"last_event_utc": None,
			"freshness_hours": None,
			"event_count": 0,
		}
	event_ts_values = [_parse_iso_utc(event.get("event_ts")) for event in events]
	event_ts_values = [value for value in event_ts_values if value is not None]
	if not event_ts_values:
		return {
			"status": "unknown",
			"max_age_hours": max_age_hours,
			"last_event_utc": None,
			"freshness_hours": None,
			"event_count": len(events),
		}
	last_event = max(event_ts_values)
	freshness_hours = (now_utc - last_event).total_seconds() / 3600.0
	return {
		"status": "stale" if freshness_hours > max_age_hours else "fresh",
		"max_age_hours": max_age_hours,
		"last_event_utc": last_event.isoformat().replace("+00:00", "Z"),
		"freshness_hours": round(freshness_hours, 3),
		"event_count": len(events),
	}


def _eligibility_change_snapshot(run_summary):
	info = run_summary.get("player_eligibility", {})
	changes = info.get("changes_summary", {}) if isinstance(info, dict) else {}
	added = int(changes.get("added_count", 0) or 0)
	removed = int(changes.get("removed_count", 0) or 0)
	updated = int(changes.get("updated_count", 0) or 0)
	return {
		"added_count": added,
		"removed_count": removed,
		"updated_count": updated,
		"has_changes": (added + removed + updated) > 0,
	}


def write_ingestion_status_index(target_date, run_summary, dry_run=False):
	now_utc = datetime.now(UTC)
	now_utc_str = now_utc.isoformat().replace("+00:00", "Z")
	index_path = get_ingestion_status_latest_path()
	previous = read_json(index_path) if index_path.exists() else {}

	ingestion_cfg = get_ingestion_config()
	ingestion_max_age_hours = float(ingestion_cfg.get("health_max_age_hours", 30))
	txn_max_age_hours = float(ingestion_cfg.get("transaction_health_max_age_hours", 168))

	run_status = run_summary.get("status", "failed")
	previous_success = _parse_iso_utc(previous.get("last_success_utc"))
	if run_status == "ok":
		last_success = now_utc
	elif previous_success is not None:
		last_success = previous_success
	else:
		last_success = now_utc
	last_success_utc = last_success.isoformat().replace("+00:00", "Z")
	ingestion_freshness_hours = (now_utc - last_success).total_seconds() / 3600.0

	resources = [
		_resource_row("auth", run_summary.get("auth"), fallback_error="AUTH_NOT_OK"),
		_resource_row("fetch", run_summary.get("fetch"), fallback_error="FETCH_NOT_OK"),
		_resource_row("normalize", run_summary.get("normalize"), fallback_error="NORMALIZE_NOT_OK"),
		_resource_row("transactions", run_summary.get("transactions"), fallback_error="TRANSACTIONS_NOT_OK"),
		_resource_row("roster_state", run_summary.get("roster_state"), fallback_error="ROSTER_STATE_NOT_OK"),
		_resource_row("recompute_trigger", run_summary.get("recompute_trigger"), fallback_error="RECOMPUTE_TRIGGER_NOT_OK"),
		_resource_row("player_priors", run_summary.get("player_priors"), fallback_error="PLAYER_PRIORS_NOT_OK"),
		_resource_row("player_eligibility", run_summary.get("player_eligibility"), fallback_error="PLAYER_ELIGIBILITY_NOT_OK"),
		_resource_row("player_blend", run_summary.get("player_blend"), fallback_error="PLAYER_BLEND_NOT_OK"),
		_resource_row("projection_horizons", run_summary.get("projection_horizons"), fallback_error="PROJECTION_HORIZONS_NOT_OK"),
		_resource_row("view_models", run_summary.get("view_models"), fallback_error="VIEW_MODELS_NOT_OK"),
		_resource_row("clap_v2", run_summary.get("clap_v2"), fallback_error="CLAP_V2_NOT_OK"),
		_resource_row("free_agent_candidates", run_summary.get("free_agent_candidates"), fallback_error="FREE_AGENT_CANDIDATES_NOT_OK"),
		_resource_row("weekly_digest", run_summary.get("weekly_digest"), fallback_error="WEEKLY_DIGEST_NOT_OK"),
		_resource_row("weekly_email", run_summary.get("weekly_email"), fallback_error="WEEKLY_EMAIL_NOT_OK"),
		_resource_row("weekly_calibration", run_summary.get("weekly_calibration"), fallback_error="WEEKLY_CALIBRATION_NOT_OK"),
		_resource_row("artifact_history", run_summary.get("artifact_history"), fallback_error="ARTIFACT_HISTORY_NOT_OK"),
	]

	codes = []
	if run_status != "ok":
		codes.append("INGESTION_RUN_FAILED")
	if any(resource["status"] == "failed" for resource in resources):
		codes.append("INGESTION_RESOURCE_FAILED")
	if run_summary.get("roster_state", {}).get("integrity", {}).get("status") == "error":
		codes.append("ROSTER_INTEGRITY_ERROR")
	elif run_summary.get("roster_state", {}).get("integrity", {}).get("status") == "warning":
		codes.append("ROSTER_INTEGRITY_WARNING")
	eligibility_changes = _eligibility_change_snapshot(run_summary)
	if eligibility_changes["has_changes"]:
		codes.append("ELIGIBILITY_UPDATED")
	if eligibility_changes["added_count"] > 0:
		codes.append("ELIGIBILITY_ADDED")
	if eligibility_changes["removed_count"] > 0:
		codes.append("ELIGIBILITY_REMOVED")

	transaction_stream = _transaction_stream_snapshot(now_utc, txn_max_age_hours)
	if transaction_stream["status"] == "stale":
		codes.append("HEALTH_TXN_FEED_STALE")
	if ingestion_freshness_hours > ingestion_max_age_hours:
		codes.append("HEALTH_INGESTION_STALE")
	if not codes:
		codes.append("ok")

	payload = {
		"schema_version": "1.0",
		"status": "failed" if run_status != "ok" else "ok",
		"last_success_utc": last_success_utc,
		"target_date": target_date.strftime("%Y-%m-%d"),
		"freshness_hours": round(ingestion_freshness_hours, 3),
		"resources": resources,
		"codes": codes,
		"eligibility_changes": eligibility_changes,
		"transaction_stream": transaction_stream,
		"generated_at_utc": now_utc_str,
	}
	try:
		validate_ingestion_status_payload(payload)
	except ValidationError as error:
		raise StatusIndexError(f"Invalid ingestion status payload: {error}")

	if not dry_run:
		write_json(index_path, payload)
	return {"status": "ok", "output_path": index_path, "codes": codes}
