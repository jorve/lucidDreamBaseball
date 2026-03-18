TRANSACTIONS_SCHEMA_VERSION = "1.0"
ROSTER_STATE_SCHEMA_VERSION = "1.0"
INGESTION_STATUS_SCHEMA_VERSION = "1.0"

TRANSACTION_EVENT_TYPES = {"add", "drop", "trade"}
TRANSACTION_VALIDATION_CODES = {"ok", "warn_missing_optional", "warn_collision", "error_quarantined"}

INGESTION_ROOT_STATUSES = {"ok", "failed"}
INGESTION_RESOURCE_STATUSES = {"ok", "optional_failed", "failed", "skipped"}
ROSTER_INTEGRITY_STATUSES = {"ok", "warning", "error"}

TRANSACTIONS_REQUIRED_ROOT_FIELDS = {
	"schema_version",
	"generated_at_utc",
	"source",
	"summary",
	"events",
}
TRANSACTIONS_REQUIRED_EVENT_FIELDS = {
	"event_id",
	"event_ts",
	"event_type",
	"players",
	"source",
	"ingested_at_utc",
	"validation_code",
}

ROSTER_REQUIRED_ROOT_FIELDS = {
	"schema_version",
	"as_of_utc",
	"base_snapshot",
	"event_apply_window",
	"teams",
	"integrity",
}
ROSTER_REQUIRED_TEAM_FIELDS = {"team_id", "team_name", "players"}
ROSTER_REQUIRED_PLAYER_FIELDS = {"player_id", "player_name"}

INGESTION_STATUS_REQUIRED_ROOT_FIELDS = {
	"schema_version",
	"status",
	"last_success_utc",
	"target_date",
	"freshness_hours",
	"resources",
}
INGESTION_STATUS_REQUIRED_RESOURCE_FIELDS = {"name", "status", "path", "error_short"}
