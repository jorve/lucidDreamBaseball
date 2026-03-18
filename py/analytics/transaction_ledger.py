from datetime import datetime, timezone
from pathlib import Path

from analytics.ids import build_event_id, canonical_payload_hash
from analytics.io import read_json, write_json
from analytics.validators import ValidationError, validate_transactions_payload
from project_config import (
	get_ingestion_raw_dir,
	get_transactions_latest_path,
	get_transactions_quarantine_latest_path,
)


UTC = timezone.utc
SOURCE_PRIORITY = {
	"transactions": 1,
	"league_transactions": 2,
	"activity": 3,
	"moves": 4,
	"league_lineup": 5,
}


class TransactionLedgerError(RuntimeError):
	pass


class TransactionLedgerBuilder:
	def __init__(self, league_id="luciddreambaseball"):
		self.league_id = league_id

	def build(self, target_date, dry_run=False):
		raw_dir = get_ingestion_raw_dir(target_date)
		manifest_path = raw_dir / "manifest.json"
		if not manifest_path.exists():
			raise TransactionLedgerError(f"Missing ingestion manifest: {manifest_path}")
		manifest = read_json(manifest_path)

		raw_records = self._extract_raw_records(raw_dir, manifest)
		events = []
		quarantined = []
		for record in raw_records:
			try:
				event = self._normalize_record(record)
				events.append(event)
			except TransactionLedgerError as error:
				reason_code = str(error)
				quarantined.append(
					{
						"reason_code": reason_code,
						"error": reason_code,
						"source_resource": record.get("_resource_name"),
						"raw_event_key": self._raw_event_key(record),
						"raw_record": self._safe_raw_record(record),
					}
				)

		deduped_events, dedupe_stats = self._dedupe_events(events)
		deduped_events.sort(key=lambda event: (event["event_ts"], event["event_id"]))
		for event in deduped_events:
			event.pop("_source_priority", None)
			event.pop("_semantic_key", None)
			event.pop("_content_hash", None)
		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"source": {
				"provider": "cbssports",
				"league_id": self.league_id,
				"target_date": target_date.strftime("%Y-%m-%d"),
			},
			"summary": {
				"events_seen": len(raw_records),
				"events_new": len(deduped_events),
				"events_deduped": dedupe_stats["events_deduped"],
				"events_failed": len(quarantined),
				"events_inferred": 0,
				"events_quarantined": len(quarantined),
				"events_aliased": dedupe_stats["events_aliased"],
				"events_collisions": dedupe_stats["events_collisions"],
			},
			"events": deduped_events,
		}
		payload["content_hash"] = canonical_payload_hash(
			{
				"source": payload["source"],
				"summary": payload["summary"],
				"events": payload["events"],
			}
		)
		try:
			validate_transactions_payload(payload)
		except ValidationError as error:
			raise TransactionLedgerError(f"Invalid transactions payload: {error}")

		output_path = get_transactions_latest_path()
		quarantine_path = get_transactions_quarantine_latest_path()
		if not dry_run:
			write_json(output_path, payload)
			write_json(
				quarantine_path,
				{
					"schema_version": "1.0",
					"generated_at_utc": now_utc,
					"target_date": target_date.strftime("%Y-%m-%d"),
					"quarantined": quarantined,
				},
			)

		return {
			"status": "ok",
			"output_path": output_path,
			"quarantine_path": quarantine_path,
			"summary": payload["summary"],
		}

	def _extract_raw_records(self, raw_dir, manifest):
		resources = manifest.get("resources", {})
		records = []
		interesting_names = {"transactions", "league_transactions", "moves", "activity", "league_lineup"}
		for resource_name, resource in resources.items():
			if resource.get("status") != "ok":
				continue
			file_value = resource.get("file")
			if not file_value:
				continue
			file_path = Path(file_value)
			if not file_path.exists():
				continue
			payload = read_json(file_path)
			for candidate in self._find_transaction_candidates(payload):
				record = dict(candidate)
				record["_resource_name"] = resource_name
				record["_source_priority"] = SOURCE_PRIORITY.get(resource_name, 50)
				record["_raw_event_key"] = self._raw_event_key(candidate)
				records.append(record)
			if resource_name in interesting_names and not records:
				# Keep behavior predictable if endpoint exists but schema unknown.
				records.extend([])
		return records

	def _find_transaction_candidates(self, payload):
		candidates = []
		stack = [payload]
		while stack:
			current = stack.pop()
			if isinstance(current, dict):
				lower_keys = {str(key).lower() for key in current.keys()}
				if (
					{"event_type", "players"} <= lower_keys
					or "transaction_type" in lower_keys
					or "move_type" in lower_keys
					or ("type" in lower_keys and any(key in lower_keys for key in {"player_id", "players"}))
				):
					candidates.append(current)
				for value in current.values():
					if isinstance(value, (dict, list)):
						stack.append(value)
			elif isinstance(current, list):
				for item in current:
					if isinstance(item, (dict, list)):
						stack.append(item)
		return candidates

	def _normalize_record(self, record):
		event_type_raw = (
			record.get("event_type")
			or record.get("transaction_type")
			or record.get("move_type")
			or record.get("type")
		)
		event_type = self._normalize_event_type(event_type_raw)
		if not event_type:
			raise TransactionLedgerError("TXN_EVENT_TYPE_UNKNOWN")

		event_ts = self._normalize_timestamp(record.get("event_ts") or record.get("timestamp") or record.get("ts"))
		if not event_ts:
			raise TransactionLedgerError("TXN_TIMESTAMP_MISSING")

		players = self._normalize_players(record)
		if not players:
			raise TransactionLedgerError("TXN_PLAYER_MISSING")

		team_from = self._normalize_team(record.get("team_from") or record.get("from_team"))
		team_to = self._normalize_team(record.get("team_to") or record.get("to_team"))

		if event_type == "add" and not team_to:
			raise TransactionLedgerError("TXN_TEAM_MISSING")
		if event_type == "drop" and not team_from:
			raise TransactionLedgerError("TXN_TEAM_MISSING")
		if event_type == "trade" and (not team_from or not team_to):
			raise TransactionLedgerError("TXN_TEAM_MISSING")

		team_ids = []
		if team_from:
			team_ids.append(team_from["team_id"])
		if team_to:
			team_ids.append(team_to["team_id"])
		event_ts_compact = event_ts.replace("-", "").replace(":", "").replace("T", "").replace("Z", "")
		event_id = build_event_id(event_ts_compact, event_type, team_ids, [player["player_id"] for player in players])

		event = {
			"event_id": event_id,
			"event_ts": event_ts,
			"event_type": event_type,
			"players": players,
			"source": "cbs_api",
			"ingested_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"validation_code": "ok",
			"_source_priority": int(record.get("_source_priority", 50)),
		}
		if team_from:
			event["team_from"] = team_from
		if team_to:
			event["team_to"] = team_to
		event["_semantic_key"] = self._semantic_key(event)
		event["_content_hash"] = self._event_content_hash(event)
		event["raw_refs"] = {
			"sources": [
				{
					"resource": record.get("_resource_name"),
					"priority": int(record.get("_source_priority", 50)),
					"raw_event_key": record.get("_raw_event_key"),
				}
			]
		}
		return event

	def _normalize_event_type(self, event_type):
		if event_type is None:
			return None
		value = str(event_type).strip().lower()
		if value in {"add", "waiver_add", "pickup"}:
			return "add"
		if value in {"drop", "waiver_drop", "release"}:
			return "drop"
		if value in {"trade", "deal"}:
			return "trade"
		return None

	def _normalize_timestamp(self, value):
		if not value:
			return None
		text = str(value).strip()
		if text.endswith("Z"):
			return text
		try:
			parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
			return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
		except Exception:
			return None

	def _normalize_players(self, record):
		candidates = record.get("players")
		if isinstance(candidates, dict):
			candidates = [candidates]
		if not isinstance(candidates, list):
			player_id = record.get("player_id") or record.get("id")
			player_name = record.get("player_name") or record.get("name")
			if player_id:
				candidates = [{"player_id": player_id, "player_name": player_name}]
			else:
				return []

		normalized = []
		for player in candidates:
			if not isinstance(player, dict):
				continue
			player_id = player.get("player_id") or player.get("id")
			if player_id is None:
				continue
			normalized.append(
				{
					"player_id": str(player_id),
					"player_name": player.get("player_name") or player.get("name") or f"UNKNOWN_{player_id}",
					"movement": player.get("movement") or "unknown",
				}
			)
		return normalized

	def _normalize_team(self, team):
		if not isinstance(team, dict):
			return None
		team_id = team.get("team_id") or team.get("id")
		if team_id is None:
			return None
		return {
			"team_id": str(team_id),
			"team_name": team.get("team_name") or team.get("name") or f"TEAM_{team_id}",
		}

	def _dedupe_events(self, events):
		by_id = {}
		semantic_to_id = {}
		deduped = 0
		aliased = 0
		collisions = 0
		for event in events:
			event_id = event["event_id"]
			semantic_key = event.get("_semantic_key")
			if semantic_key in semantic_to_id and semantic_to_id[semantic_key] != event_id:
				canonical = by_id[semantic_to_id[semantic_key]]
				self._merge_source_refs(canonical, event)
				alias_ids = canonical.setdefault("raw_refs", {}).setdefault("alias_event_ids", [])
				if event_id not in alias_ids:
					alias_ids.append(event_id)
				deduped += 1
				aliased += 1
				continue
			if event_id in by_id:
				existing = by_id[event_id]
				if existing.get("_content_hash") == event.get("_content_hash"):
					preferred = self._prefer_event(existing, event)
					other = event if preferred is existing else existing
					self._merge_source_refs(preferred, other)
					by_id[event_id] = preferred
					deduped += 1
					continue
				collisions += 1
				collision_id = self._collision_event_id(event_id, by_id)
				event["event_id"] = collision_id
				event["validation_code"] = "warn_collision"
				event_id = collision_id
			by_id[event_id] = event
			semantic_to_id.setdefault(semantic_key, event_id)
		return list(by_id.values()), {
			"events_deduped": deduped,
			"events_aliased": aliased,
			"events_collisions": collisions,
		}

	def _prefer_event(self, left, right):
		return left if int(left.get("_source_priority", 50)) <= int(right.get("_source_priority", 50)) else right

	def _merge_source_refs(self, canonical, duplicate):
		canonical_refs = canonical.setdefault("raw_refs", {}).setdefault("sources", [])
		for source in duplicate.get("raw_refs", {}).get("sources", []):
			if source not in canonical_refs:
				canonical_refs.append(source)

	def _collision_event_id(self, base_event_id, by_id):
		version = 2
		while True:
			candidate = f"{base_event_id}_v{version}"
			if candidate not in by_id:
				return candidate
			version += 1

	def _semantic_key(self, event):
		teams = []
		if isinstance(event.get("team_from"), dict):
			teams.append(str(event["team_from"].get("team_id")))
		if isinstance(event.get("team_to"), dict):
			teams.append(str(event["team_to"].get("team_id")))
		players = sorted(str(player.get("player_id")) for player in event.get("players", []) if player.get("player_id") is not None)
		return "|".join(
			[
				event.get("event_ts", ""),
				event.get("event_type", ""),
				",".join(sorted(team for team in teams if team and team != "None")),
				",".join(players),
			]
		)

	def _event_content_hash(self, event):
		payload = {
			"event_ts": event.get("event_ts"),
			"event_type": event.get("event_type"),
			"team_from": event.get("team_from"),
			"team_to": event.get("team_to"),
			"players": event.get("players"),
		}
		return canonical_payload_hash(payload)

	def _raw_event_key(self, record):
		for key in ("event_id", "transaction_id", "id", "key"):
			if record.get(key) is not None:
				return str(record.get(key))
		return None

	def _safe_raw_record(self, record):
		return {key: value for key, value in record.items() if not str(key).startswith("_")}
