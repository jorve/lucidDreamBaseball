import json
import re
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from project_config import (
	CURRENT_WEEK,
	get_ingestion_cbs_config,
	get_ingestion_config,
	get_ingestion_raw_dir,
)
from storage import StorageRecorder


UTC = timezone.utc


class FetchError(RuntimeError):
	pass


class CbsApiFetcher:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.cbs_cfg = get_ingestion_cbs_config()
		self.base_url = self.cbs_cfg.get("base_url", "https://api.cbssports.com/fantasy").rstrip("/")
		self.league_id = self.cbs_cfg.get("league_id", "luciddreambaseball")
		self.version = self.cbs_cfg.get("version", "3.0")
		self.response_format = self.cbs_cfg.get("response_format", "json")
		self.endpoints = self._build_endpoints(self.cbs_cfg.get("endpoints", {}))
		self.optional_resources = set(self.ingestion_cfg.get("optional_resources", ["player_stats"]))
		self.live_scoring_include_players = bool(self.ingestion_cfg.get("live_scoring_include_players", False))
		request_policy = self.ingestion_cfg.get("request_policy", {})
		self.max_attempts = int(request_policy.get("max_attempts", self.ingestion_cfg.get("max_attempts", 3)))
		self.retry_backoff_seconds = float(request_policy.get("retry_backoff_seconds", self.ingestion_cfg.get("retry_backoff_seconds", 2.0)))
		self.min_interval_seconds = float(request_policy.get("min_interval_seconds", 1.5))
		self.jitter_seconds = float(request_policy.get("jitter_seconds", 0.75))
		self.user_agent = str(
			request_policy.get(
				"user_agent",
				"lucidDreamBaseball-ingestion/1.0 (+respectful-request-policy)",
			)
		)
		self._last_request_time = None
		self.storage_recorder = StorageRecorder()

	def _build_endpoints(self, configured_endpoints):
		defaults = {
			"live_scoring": "league/scoring/live",
			"schedule": "league/schedule",
			"rosters": "league/rosters",
			"player_stats": "stats/players",
		}
		defaults.update(configured_endpoints)
		return defaults

	def fetch_all(self, target_date, auth_session, dry_run=False):
		raw_dir = get_ingestion_raw_dir(target_date)
		successful_payloads = {}
		manifest = {
			"target_date": target_date.strftime("%Y-%m-%d"),
			"resources": {},
			"fetched_at": datetime.now(UTC).isoformat(),
		}

		for resource_name, endpoint in self.endpoints.items():
			file_path = raw_dir / f"{resource_name}_{target_date.strftime('%Y-%m-%d')}.json"
			metadata_path = raw_dir / f"{resource_name}_{target_date.strftime('%Y-%m-%d')}.meta.json"
			if dry_run:
				manifest["resources"][resource_name] = {
					"file": str(file_path),
					"metadata": str(metadata_path),
					"status": "dry_run",
				}
				continue

			try:
				payload = self._request_resource(resource_name, endpoint, target_date, auth_session)
				if resource_name == "schedule":
					payload = self._expand_schedule_for_all_periods(
						base_payload=payload,
						endpoint=endpoint,
						target_date=target_date,
						auth_session=auth_session,
					)
				if resource_name == "rosters":
					payload = self._expand_rosters_for_all_teams(
						base_payload=payload,
						endpoint=endpoint,
						target_date=target_date,
						auth_session=auth_session,
						live_scoring_payload=successful_payloads.get("live_scoring"),
					)
				self._write_json(file_path, payload)
				self._write_metadata(metadata_path, resource_name, endpoint, target_date)
				manifest["resources"][resource_name] = {
					"file": str(file_path),
					"metadata": str(metadata_path),
					"status": "ok",
				}
				successful_payloads[resource_name] = payload
			except FetchError as error:
				if resource_name in self.optional_resources:
					manifest["resources"][resource_name] = {
						"file": str(file_path),
						"metadata": str(metadata_path),
						"status": "optional_failed",
						"error": self._summarize_optional_error(str(error)),
					}
					continue
				raise

		manifest_path = raw_dir / "manifest.json"
		if not dry_run:
			self._write_json(manifest_path, manifest)
		return {"raw_dir": raw_dir, "manifest_path": manifest_path, "manifest": manifest}

	def _request_resource(self, resource_name, endpoint, target_date, auth_session, additional_params=None):
		try:
			import requests
		except ImportError as error:
			raise FetchError(
				"`requests` is required for live ingestion. Install dependencies with "
				"`python -m pip install -r requirements.txt`."
			) from error

		endpoint_candidates = self._build_endpoint_candidates(resource_name, endpoint)
		params = self._build_params(resource_name, target_date, auth_session)
		if additional_params:
			params.update(additional_params)
		headers = auth_session.as_request_headers()
		headers.setdefault("Accept", "application/json, text/plain, */*")
		headers.setdefault("User-Agent", self.user_agent)

		last_error = None
		for endpoint_value in endpoint_candidates:
			if endpoint_value.startswith("http://") or endpoint_value.startswith("https://"):
				url = endpoint_value
			else:
				url = f"{self.base_url}/{endpoint_value.lstrip('/')}"
			for attempt in range(1, self.max_attempts + 1):
				try:
					if resource_name == "live_scoring" and not auth_session.api_token:
						raise FetchError(
							"CBS live scoring requires an API token. "
							"Token extraction failed during auth refresh. "
							"Set CBS_API_TOKEN or update token extraction selectors/patterns."
						)
					self._wait_for_request_slot()
					response = requests.get(url, params=params, headers=headers, timeout=45)
					response.raise_for_status()
					return response.json()
				except Exception as error:
					last_error = error
					status_code = self._status_code_from_error(error)
					if attempt < self.max_attempts and self._should_retry_status(status_code):
						time.sleep(self.retry_backoff_seconds * attempt)
		raise FetchError(f"Failed to fetch resource '{resource_name}' from {self.base_url}: {last_error}")

	def _wait_for_request_slot(self):
		# Keep request cadence conservative to reduce provider-side abuse signals.
		now = time.monotonic()
		if self._last_request_time is not None:
			elapsed = now - self._last_request_time
			target_gap = self.min_interval_seconds + random.uniform(0.0, max(0.0, self.jitter_seconds))
			if elapsed < target_gap:
				time.sleep(target_gap - elapsed)
		self._last_request_time = time.monotonic()

	def _status_code_from_error(self, error):
		response = getattr(error, "response", None)
		if response is None:
			return None
		return getattr(response, "status_code", None)

	def _should_retry_status(self, status_code):
		if status_code is None:
			return True
		# Retry only transient classes: timeout-ish, conflict-ish, rate-limit, server errors.
		if status_code in {408, 409, 425, 429}:
			return True
		if 500 <= int(status_code) <= 599:
			return True
		return False

	def _expand_rosters_for_all_teams(self, base_payload, endpoint, target_date, auth_session, live_scoring_payload):
		base_teams = self._extract_roster_teams(base_payload)
		teams_by_id = {str(team.get("id")): team for team in base_teams if team.get("id") is not None}
		target_team_ids = self._extract_team_ids_from_live_scoring(live_scoring_payload)
		if not target_team_ids:
			return base_payload

		param_candidates = ["team_id", "team", "id", "owner_team_id"]
		for team_id in target_team_ids:
			if team_id in teams_by_id:
				continue
			for param_name in param_candidates:
				try:
					candidate_payload = self._request_resource(
						resource_name="rosters",
						endpoint=endpoint,
						target_date=target_date,
						auth_session=auth_session,
						additional_params={param_name: team_id},
					)
				except FetchError:
					continue
				candidate_teams = self._extract_roster_teams(candidate_payload)
				for team in candidate_teams:
					team_key = str(team.get("id")) if team.get("id") is not None else None
					if team_key:
						teams_by_id[team_key] = team
				if team_id in teams_by_id:
					break

		merged = json.loads(json.dumps(base_payload))
		merged["body"]["rosters"]["teams"] = list(teams_by_id.values())
		return merged

	def _expand_schedule_for_all_periods(self, base_payload, endpoint, target_date, auth_session):
		expected_periods = int(self.ingestion_cfg.get("schedule_expected_periods", 20))
		periods_by_id = self._extract_schedule_periods_by_id(base_payload)
		if len(periods_by_id) >= expected_periods:
			return base_payload

		# First try providers' common "all periods" query conventions.
		all_param_candidates = [
			{"period": "all"},
			{"periods": "all"},
			{"period": "0"},
		]
		for params in all_param_candidates:
			try:
				candidate_payload = self._request_resource(
					resource_name="schedule",
					endpoint=endpoint,
					target_date=target_date,
					auth_session=auth_session,
					additional_params=params,
				)
			except FetchError:
				continue
			periods_by_id.update(self._extract_schedule_periods_by_id(candidate_payload))
			if len(periods_by_id) >= expected_periods:
				break

		# If provider still returns a partial schedule, fan out by period.
		if len(periods_by_id) < expected_periods:
			for period_number in range(1, expected_periods + 1):
				for params in (
					{"period": str(period_number)},
					{"period": f"{period_number}0"},
				):
					try:
						candidate_payload = self._request_resource(
							resource_name="schedule",
							endpoint=endpoint,
							target_date=target_date,
							auth_session=auth_session,
							additional_params=params,
						)
					except FetchError:
						continue
					periods_by_id.update(self._extract_schedule_periods_by_id(candidate_payload))
					if str(period_number) in periods_by_id:
						break

		return self._merge_schedule_periods(base_payload, periods_by_id)

	def _extract_roster_teams(self, roster_payload):
		try:
			teams = roster_payload["body"]["rosters"]["teams"]
		except Exception:
			return []
		return teams if isinstance(teams, list) else []

	def _extract_team_ids_from_live_scoring(self, live_scoring_payload):
		try:
			teams = live_scoring_payload["body"]["live_scoring"]["teams"]
		except Exception:
			return []
		ids = []
		for team in teams:
			team_id = team.get("id")
			if team_id is not None:
				ids.append(str(team_id))
		return ids

	def _extract_schedule_periods_by_id(self, schedule_payload):
		try:
			periods = schedule_payload["body"]["schedule"]["periods"]
		except Exception:
			return {}
		if not isinstance(periods, list):
			return {}
		periods_by_id = {}
		for period in periods:
			period_id = str(period.get("id")) if isinstance(period, dict) and period.get("id") is not None else None
			if period_id:
				periods_by_id[period_id] = period
		return periods_by_id

	def _merge_schedule_periods(self, base_payload, periods_by_id):
		if not periods_by_id:
			return base_payload
		ordered_periods = sorted(
			periods_by_id.values(),
			key=lambda period: int(period.get("id", 9999)) if str(period.get("id", "")).isdigit() else 9999,
		)
		merged = json.loads(json.dumps(base_payload))
		merged.setdefault("body", {}).setdefault("schedule", {})["periods"] = ordered_periods
		return merged

	def _build_endpoint_candidates(self, resource_name, endpoint):
		candidates = [endpoint]
		if endpoint.startswith("http://") or endpoint.startswith("https://"):
			return candidates
		if resource_name == "schedule":
			if endpoint.endswith("schedule"):
				candidates.append(endpoint + "s")
			if endpoint.endswith("schedules"):
				candidates.append(endpoint[:-1])
		if resource_name == "player_stats":
			candidates.extend([
				"league/stats/players",
				"players/stats",
				"stats/players",
			])
		# de-duplicate while preserving order
		ordered = []
		seen = set()
		for item in candidates:
			if item not in seen:
				ordered.append(item)
				seen.add(item)
		return ordered

	def _build_params(self, resource_name, target_date, auth_session):
		if resource_name in {"league_lineup", "scout_team"}:
			# League-domain endpoints use cookie auth and often reject fantasy API query params.
			return {}
		params = {
			"version": self.version,
			"league_id": self.league_id,
			"response_format": self.response_format,
		}
		if auth_session.api_token:
			params["access_token"] = auth_session.api_token
		if resource_name == "live_scoring":
			params["period"] = f"{CURRENT_WEEK}0"
			if not self.live_scoring_include_players:
				params["no_players"] = 1
		if resource_name in {"rosters", "player_stats"}:
			params["date"] = target_date.strftime("%Y-%m-%d")
		return params

	def _write_metadata(self, metadata_path, resource_name, endpoint, target_date):
		metadata = {
			"resource": resource_name,
			"endpoint": endpoint,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"request_time_utc": datetime.now(UTC).isoformat(),
			"league_id": self.league_id,
		}
		self._write_json(metadata_path, metadata)

	def _write_json(self, path_value: Path, payload):
		with path_value.open("w") as outfile:
			json.dump(payload, outfile, indent=2)
		self.storage_recorder.record_json_artifact(
			path_value=path_value,
			payload=payload,
			artifact_kind="raw",
			write_source="ingestion.fetch_cbs_data._write_json",
		)

	def _summarize_optional_error(self, error_text):
		status_match = re.search(r"(\d{3})\s+Client Error:\s+([A-Za-z ]+)\s+for url:\s+(\S+)", error_text)
		if status_match:
			status_code, status_label, failing_url = status_match.groups()
			url_path = urlparse(failing_url).path
			return f"{status_code} {status_label.strip()} at {url_path}"
		if len(error_text) > 180:
			return error_text[:177] + "..."
		return error_text
