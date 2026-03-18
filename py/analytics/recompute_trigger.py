from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_ingestion_config,
	get_recompute_request_latest_path,
	get_roster_state_diagnostics_latest_path,
	get_transactions_latest_path,
)


UTC = timezone.utc


class RecomputeTriggerError(RuntimeError):
	pass


class RecomputeTriggerBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.recompute_cfg = self.ingestion_cfg.get("recompute", {})

	def build(self, target_date, dry_run=False, transaction_summary=None, roster_integrity=None):
		transactions_payload = self._load_transactions_payload()
		events = transactions_payload.get("events", [])
		summary = transactions_payload.get("summary", {})

		if transaction_summary is None:
			new_event_count = int(summary.get("events_new", len(events)))
		else:
			new_event_count = int(transaction_summary.get("events_new", 0))

		if roster_integrity is None:
			roster_integrity = self._load_roster_integrity()

		affected_team_ids, affected_player_ids = self._derive_affected_scope(events)
		triggered = new_event_count > 0
		reason_codes = ["NEW_TRANSACTION_EVENTS"] if triggered else ["NO_NEW_APPLIED_EVENTS"]

		high_risk_integrity = self._has_high_risk_integrity(roster_integrity)
		fallback_full = bool(
			triggered
			and high_risk_integrity
			and self.recompute_cfg.get("force_full_on_integrity_error", True)
		)
		recommended_scope = "full" if fallback_full else ("incremental" if triggered else "none")

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"triggered": triggered,
			"reason_codes": reason_codes,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"event_window": {
				"first_new_event_id": events[0]["event_id"] if events else None,
				"last_new_event_id": events[-1]["event_id"] if events else None,
				"new_event_count": new_event_count,
			},
			"affected_team_ids": affected_team_ids,
			"affected_player_ids": affected_player_ids,
			"recommended_scope": recommended_scope,
			"fallback_full_recompute": fallback_full,
		}

		output_path = get_recompute_request_latest_path()
		if not dry_run:
			write_json(output_path, payload)

		return {
			"status": "ok",
			"output_path": output_path,
			"triggered": triggered,
			"recommended_scope": recommended_scope,
			"fallback_full_recompute": fallback_full,
			"affected_team_count": len(affected_team_ids),
			"affected_player_count": len(affected_player_ids),
		}

	def _load_transactions_payload(self):
		path_value = get_transactions_latest_path()
		if not path_value.exists():
			return {"events": [], "summary": {"events_new": 0}}
		return read_json(path_value)

	def _load_roster_integrity(self):
		path_value = get_roster_state_diagnostics_latest_path()
		if not path_value.exists():
			return {}
		diagnostics = read_json(path_value)
		return diagnostics.get("integrity_checks", {})

	def _derive_affected_scope(self, events):
		team_ids = set()
		player_ids = set()
		for event in events:
			team_from = event.get("team_from")
			team_to = event.get("team_to")
			if isinstance(team_from, dict) and team_from.get("team_id") is not None:
				team_ids.add(str(team_from["team_id"]))
			if isinstance(team_to, dict) and team_to.get("team_id") is not None:
				team_ids.add(str(team_to["team_id"]))
			for player in event.get("players", []):
				player_id = player.get("player_id")
				if player_id is not None:
					player_ids.add(str(player_id))
		return sorted(team_ids, key=self._id_sort_key), sorted(player_ids, key=self._id_sort_key)

	def _has_high_risk_integrity(self, integrity):
		if not isinstance(integrity, dict):
			return False
		if integrity.get("status") == "error":
			return True
		if int(integrity.get("duplicate_player_assignments", 0)) > 0:
			return True
		if int(integrity.get("atomic_trade_failures", 0)) > 0:
			return True
		return False

	def _id_sort_key(self, value):
		text = str(value)
		return (0, int(text)) if text.isdigit() else (1, text)
