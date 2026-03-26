from datetime import datetime

from analytics.schemas import (
	INGESTION_RESOURCE_STATUSES,
	INGESTION_ROOT_STATUSES,
	INGESTION_STATUS_REQUIRED_RESOURCE_FIELDS,
	INGESTION_STATUS_REQUIRED_ROOT_FIELDS,
	INGESTION_STATUS_SCHEMA_VERSION,
	PLAYER_REGISTRY_REQUIRED_ROOT_FIELDS,
	PLAYER_REGISTRY_SCHEMA_VERSION,
	ROSTER_INTEGRITY_STATUSES,
	ROSTER_REQUIRED_PLAYER_FIELDS,
	ROSTER_REQUIRED_ROOT_FIELDS,
	ROSTER_REQUIRED_TEAM_FIELDS,
	ROSTER_STATE_SCHEMA_VERSION,
	TRANSACTIONS_REQUIRED_EVENT_FIELDS,
	TRANSACTIONS_REQUIRED_ROOT_FIELDS,
	TRANSACTIONS_SCHEMA_VERSION,
	TRANSACTION_EVENT_TYPES,
	TRANSACTION_VALIDATION_CODES,
)


class ValidationError(ValueError):
	def __init__(self, code, message):
		self.code = code
		super().__init__(f"{code}: {message}")


def _require_fields(payload, required_fields, object_name):
	missing = sorted(field for field in required_fields if field not in payload)
	if missing:
		raise ValidationError(
			"SCHEMA_REQUIRED_FIELD_MISSING",
			f"{object_name} missing required fields: {', '.join(missing)}",
		)


def _parse_utc_timestamp(value, object_name, field_name):
	try:
		datetime.fromisoformat(value.replace("Z", "+00:00"))
	except Exception:
		raise ValidationError(
			"SCHEMA_INVALID_TIMESTAMP",
			f"{object_name}.{field_name} must be an ISO-8601 UTC timestamp.",
		)


def validate_transactions_payload(payload):
	_require_fields(payload, TRANSACTIONS_REQUIRED_ROOT_FIELDS, "transactions")
	if payload["schema_version"] != TRANSACTIONS_SCHEMA_VERSION:
		raise ValidationError("SCHEMA_UNSUPPORTED_VERSION", "Unsupported transactions schema version.")
	_parse_utc_timestamp(payload["generated_at_utc"], "transactions", "generated_at_utc")
	if not isinstance(payload["events"], list):
		raise ValidationError("SCHEMA_INVALID_TYPE", "transactions.events must be a list.")

	for idx, event in enumerate(payload["events"]):
		_require_fields(event, TRANSACTIONS_REQUIRED_EVENT_FIELDS, f"event[{idx}]")
		_parse_utc_timestamp(event["event_ts"], f"event[{idx}]", "event_ts")
		_parse_utc_timestamp(event["ingested_at_utc"], f"event[{idx}]", "ingested_at_utc")
		if event["event_type"] not in TRANSACTION_EVENT_TYPES:
			raise ValidationError("TXN_EVENT_TYPE_UNKNOWN", f"event[{idx}] has unknown event_type.")
		if event["validation_code"] not in TRANSACTION_VALIDATION_CODES:
			raise ValidationError("SCHEMA_INVALID_ENUM", f"event[{idx}] has invalid validation_code.")
		if not isinstance(event["players"], list) or len(event["players"]) == 0:
			raise ValidationError("SCHEMA_INVALID_TYPE", f"event[{idx}].players must be a non-empty list.")
		for player_idx, player in enumerate(event["players"]):
			if "player_id" not in player or "player_name" not in player:
				raise ValidationError(
					"SCHEMA_REQUIRED_FIELD_MISSING",
					f"event[{idx}].players[{player_idx}] missing player_id/player_name.",
				)
	return True


def validate_roster_state_payload(payload):
	_require_fields(payload, ROSTER_REQUIRED_ROOT_FIELDS, "roster_state")
	if payload["schema_version"] != ROSTER_STATE_SCHEMA_VERSION:
		raise ValidationError("SCHEMA_UNSUPPORTED_VERSION", "Unsupported roster state schema version.")
	_parse_utc_timestamp(payload["as_of_utc"], "roster_state", "as_of_utc")
	if payload["integrity"].get("status") not in ROSTER_INTEGRITY_STATUSES:
		raise ValidationError("SCHEMA_INVALID_ENUM", "roster_state.integrity.status is invalid.")
	if not isinstance(payload["teams"], list):
		raise ValidationError("SCHEMA_INVALID_TYPE", "roster_state.teams must be a list.")

	player_to_team = {}
	for team_idx, team in enumerate(payload["teams"]):
		_require_fields(team, ROSTER_REQUIRED_TEAM_FIELDS, f"team[{team_idx}]")
		if not isinstance(team["players"], list):
			raise ValidationError("SCHEMA_INVALID_TYPE", f"team[{team_idx}].players must be a list.")
		for player_idx, player in enumerate(team["players"]):
			_require_fields(player, ROSTER_REQUIRED_PLAYER_FIELDS, f"team[{team_idx}].players[{player_idx}]")
			player_id = str(player["player_id"])
			owner_team = str(team["team_id"])
			if player_id in player_to_team and player_to_team[player_id] != owner_team:
				raise ValidationError(
					"ROSTER_DUPLICATE_PLAYER_ASSIGNMENT",
					f"player_id {player_id} appears on multiple teams.",
				)
			player_to_team[player_id] = owner_team
	return True


def validate_ingestion_status_payload(payload):
	_require_fields(payload, INGESTION_STATUS_REQUIRED_ROOT_FIELDS, "ingestion_status")
	if payload["schema_version"] != INGESTION_STATUS_SCHEMA_VERSION:
		raise ValidationError("SCHEMA_UNSUPPORTED_VERSION", "Unsupported ingestion status schema version.")
	_parse_utc_timestamp(payload["last_success_utc"], "ingestion_status", "last_success_utc")
	if payload["status"] not in INGESTION_ROOT_STATUSES:
		raise ValidationError("SCHEMA_INVALID_ENUM", "ingestion_status.status is invalid.")
	if not isinstance(payload["freshness_hours"], (int, float)):
		raise ValidationError("SCHEMA_INVALID_TYPE", "ingestion_status.freshness_hours must be numeric.")
	if not isinstance(payload["resources"], list):
		raise ValidationError("SCHEMA_INVALID_TYPE", "ingestion_status.resources must be a list.")
	for idx, resource in enumerate(payload["resources"]):
		_require_fields(resource, INGESTION_STATUS_REQUIRED_RESOURCE_FIELDS, f"resource[{idx}]")
		if resource["status"] not in INGESTION_RESOURCE_STATUSES:
			raise ValidationError("SCHEMA_INVALID_ENUM", f"resource[{idx}].status is invalid.")
		if resource["status"] == "ok" and resource["error_short"] is not None:
			raise ValidationError("SCHEMA_INVALID_VALUE", f"resource[{idx}].error_short must be null when status is ok.")
	return True


def validate_player_registry_payload(payload):
	_require_fields(payload, PLAYER_REGISTRY_REQUIRED_ROOT_FIELDS, "player_registry")
	if payload["schema_version"] != PLAYER_REGISTRY_SCHEMA_VERSION:
		raise ValidationError("SCHEMA_UNSUPPORTED_VERSION", "Unsupported player registry schema version.")
	_parse_utc_timestamp(payload["generated_at_utc"], "player_registry", "generated_at_utc")
	if not isinstance(payload.get("players"), list):
		raise ValidationError("SCHEMA_INVALID_TYPE", "player_registry.players must be a list.")
	if not isinstance(payload.get("review_queue"), list):
		raise ValidationError("SCHEMA_INVALID_TYPE", "player_registry.review_queue must be a list.")
	return True
