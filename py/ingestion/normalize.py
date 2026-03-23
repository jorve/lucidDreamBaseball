import json
from datetime import datetime, timezone
from pathlib import Path

from project_config import (
	CURRENT_WEEK,
	CURRENT_YEAR,
	get_data_year_dir,
	get_ingestion_raw_dir,
	get_json_output_path,
)
from storage import StorageRecorder


UTC = timezone.utc


class NormalizeError(RuntimeError):
	pass


class IngestionNormalizer:
	def __init__(self):
		self.data_year_dir = get_data_year_dir(CURRENT_YEAR)
		self.data_year_dir.mkdir(parents=True, exist_ok=True)
		self.storage_recorder = StorageRecorder()

	def normalize(self, target_date, dry_run=False):
		raw_dir = get_ingestion_raw_dir(target_date)
		output_map = self._build_output_map(target_date)
		if dry_run:
			return {"raw_dir": raw_dir, "outputs": {key: str(value) for key, value in output_map.items()}}

		live_scoring = self._load_resource(raw_dir, "live_scoring", target_date)
		schedule = self._load_resource(raw_dir, "schedule", target_date)
		rosters = self._load_optional_resource(raw_dir, "rosters", target_date)
		player_stats = self._load_optional_resource(raw_dir, "player_stats", target_date)

		self._validate_live_scoring(live_scoring)
		self._validate_schedule(schedule)

		self._write_json(output_map["week_snapshot"], live_scoring)
		self._write_json(output_map["schedule_data"], schedule)
		self._write_json(output_map["schedule_json"], schedule)
		if rosters is not None:
			self._write_json(output_map["rosters_json"], rosters)
		if player_stats is not None:
			self._write_json(output_map["player_stats_json"], player_stats)

		summary_payload = {
			"normalized_at_utc": datetime.now(UTC).isoformat(),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"outputs": {key: str(value) for key, value in output_map.items()},
			"optional_resources_present": {
				"rosters": rosters is not None,
				"player_stats": player_stats is not None,
			},
		}
		self._write_json(output_map["ingestion_summary_json"], summary_payload)
		return {"raw_dir": raw_dir, "outputs": output_map}

	def _build_output_map(self, target_date):
		date_stamp = target_date.strftime("%Y-%m-%d")
		return {
			"week_snapshot": self.data_year_dir / f"week{CURRENT_WEEK}.json",
			"schedule_data": self.data_year_dir / "schedule.json",
			"schedule_json": get_json_output_path("schedule.json"),
			"rosters_json": get_json_output_path("rosters_latest.json"),
			"player_stats_json": get_json_output_path("player_stats_latest.json"),
			"ingestion_summary_json": get_json_output_path(f"ingestion_summary_{date_stamp}.json"),
		}

	def _resource_path(self, raw_dir: Path, resource_name, target_date):
		return raw_dir / f"{resource_name}_{target_date.strftime('%Y-%m-%d')}.json"

	def _load_resource(self, raw_dir, resource_name, target_date):
		path_value = self._resource_path(raw_dir, resource_name, target_date)
		if not path_value.exists():
			raise NormalizeError(f"Missing raw resource {resource_name}: {path_value}")
		with path_value.open() as infile:
			return json.load(infile)

	def _load_optional_resource(self, raw_dir, resource_name, target_date):
		path_value = self._resource_path(raw_dir, resource_name, target_date)
		if not path_value.exists():
			return None
		with path_value.open() as infile:
			return json.load(infile)

	def _validate_live_scoring(self, payload):
		try:
			teams = payload["body"]["live_scoring"]["teams"]
		except Exception as error:
			raise NormalizeError(f"Invalid live scoring payload structure: {error}")
		if not isinstance(teams, list) or len(teams) == 0:
			raise NormalizeError("Live scoring payload has no teams.")

	def _validate_schedule(self, payload):
		try:
			periods = payload["body"]["schedule"]["periods"]
		except Exception as error:
			raise NormalizeError(f"Invalid schedule payload structure: {error}")
		if not isinstance(periods, list) or len(periods) == 0:
			raise NormalizeError("Schedule payload has no periods.")

	def _write_json(self, path_value, payload):
		path_value.parent.mkdir(parents=True, exist_ok=True)
		with path_value.open("w") as outfile:
			json.dump(payload, outfile, indent=2)
		self.storage_recorder.record_json_artifact(
			path_value=path_value,
			payload=payload,
			artifact_kind="normalized",
			write_source="ingestion.normalize._write_json",
		)
