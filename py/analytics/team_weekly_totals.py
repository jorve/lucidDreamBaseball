import re
from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_ingestion_raw_dir,
	get_team_weekly_totals_latest_path,
	get_team_weekly_totals_state_path,
)


UTC = timezone.utc


class TeamWeeklyTotalsError(RuntimeError):
	pass


class TeamWeeklyTotalsBuilder:
	TRACKED_CATEGORIES = {"HR", "R", "aRBI", "aSB", "K", "HRA", "MGS", "VIJAY", "ERA", "aWHIP", "OBP", "OPS"}
	SCORE_CATEGORIES = ["HR", "R", "OBP", "OPS", "aRBI", "aSB", "K", "HRA", "aWHIP", "VIJAY", "ERA", "MGS"]
	DERIVED_RATE_CATEGORIES = {"OBP", "OPS", "ERA", "aWHIP"}
	PER_APP_CATEGORIES = {"MGS", "VIJAY"}

	def build(self, target_date, dry_run=False):
		target_date_str = target_date.strftime("%Y-%m-%d")
		raw_dir = get_ingestion_raw_dir(target_date)
		live_path = raw_dir / f"live_scoring_{target_date_str}.json"
		schedule_path = raw_dir / f"schedule_{target_date_str}.json"
		state_path = get_team_weekly_totals_state_path()
		latest_path = get_team_weekly_totals_latest_path()

		if not live_path.exists():
			raise TeamWeeklyTotalsError(f"Missing live scoring snapshot: {live_path}")
		if not schedule_path.exists():
			raise TeamWeeklyTotalsError(f"Missing schedule snapshot: {schedule_path}")

		live_payload = read_json(live_path)
		schedule_payload = read_json(schedule_path)

		teams = self._extract_live_teams(live_payload)
		periods = self._schedule_periods(schedule_payload)
		season_start = self._season_start_date(periods)
		state_payload = self._load_state(state_path)
		self._prune_state_to_schedule_ranges(state_payload, periods, season_start)
		target_day = self._parse_date(target_date_str)
		if target_day is None or season_start is None or target_day < season_start:
			if not dry_run:
				write_json(state_path, state_payload)
			return {
				"status": "skipped",
				"reason": "OUTSIDE_SCORING_SEASON",
				"state_path": state_path,
				"output_path": latest_path,
				"summary": {
					"target_date": target_date_str,
					"season_start": season_start.isoformat() if season_start else None,
				},
			}

		active_period = self._resolve_active_period(periods, target_day)
		if active_period is None:
			if not dry_run:
				write_json(state_path, state_payload)
			return {
				"status": "skipped",
				"reason": "OUTSIDE_ALL_SCORING_PERIODS",
				"state_path": state_path,
				"output_path": latest_path,
				"summary": {
					"target_date": target_date_str,
					"period_id": None,
					"period_start": None,
					"period_end": None,
				},
			}

		period_key = self._period_key(active_period)
		# Guard against cross-period duplication if schedule resolution changes on reruns.
		self._remove_target_date_from_all_periods(state_payload, target_date_str)
		self._remove_target_date_from_season_roto(state_payload, target_date_str)
		period_state = state_payload.setdefault("periods", {}).setdefault(
			period_key,
			{
				"period": active_period,
				"teams": {},
			},
		)
		period_state["period"] = active_period

		updated_team_count = 0
		player_count = 0
		season_roto = state_payload.setdefault("season_roto", {"teams": {}})
		for team in teams:
			team_id = str(team.get("id"))
			if not team_id:
				continue
			team_state = period_state["teams"].setdefault(
				team_id,
				{
					"team_id": team_id,
					"team_name": team.get("name") or team.get("short_name") or team_id,
					"team_abbr": team.get("long_abbr") or team.get("abbr") or team.get("short_name") or team_id,
					"categories": {},
					"players": {},
					"period_snapshot": {},
				},
			)
			team_state["team_name"] = team.get("name") or team_state.get("team_name")
			team_state["team_abbr"] = team.get("long_abbr") or team.get("abbr") or team_state.get("team_abbr")
			player_rollups = self._extract_player_rollups(team)
			team_today_totals = self._extract_team_today_totals(team, player_rollups)
			team_period_totals = self._extract_team_period_totals(team, player_rollups)
			team_state["period_snapshot"] = team_period_totals
			season_team_state = season_roto["teams"].setdefault(
				team_id,
				{
					"team_id": team_id,
					"team_name": team.get("name") or team.get("short_name") or team_id,
					"team_abbr": team.get("long_abbr") or team.get("abbr") or team.get("short_name") or team_id,
					"categories": {},
					"derived_inputs": {"daily": {}},
					"players": {},
				},
			)
			season_team_state["team_name"] = team.get("name") or season_team_state.get("team_name")
			season_team_state["team_abbr"] = team.get("long_abbr") or team.get("abbr") or season_team_state.get("team_abbr")
			team_today_components = self._extract_team_today_components(player_rollups)
			team_derived_inputs = team_state.setdefault("derived_inputs", {"daily": {}})
			team_derived_inputs.setdefault("daily", {})[target_date_str] = team_today_components
			season_derived_inputs = season_team_state.setdefault("derived_inputs", {"daily": {}})
			season_derived_inputs.setdefault("daily", {})[target_date_str] = team_today_components

			for name, value in team_today_totals.items():
				category_state = team_state["categories"].setdefault(
					name,
					{
						"is_bad": name in {"ERA", "aWHIP", "HRA"},
						"daily_values": {},
						"weekly_total": 0.0,
					},
				)
				if value is None:
					continue
				category_state["daily_values"][target_date_str] = round(value, 6)
				category_state["weekly_total"] = round(sum(category_state["daily_values"].values()), 6)
				season_category_state = season_team_state["categories"].setdefault(
					name,
					{
						"daily_values": {},
						"season_total": 0.0,
					},
				)
				season_category_state["daily_values"][target_date_str] = round(value, 6)
				season_category_state["season_total"] = round(sum(season_category_state["daily_values"].values()), 6)
			self._recompute_rate_categories_for_team_week(team_state, target_date_str)
			self._recompute_rate_categories_for_season(season_team_state, target_date_str)
			self._recompute_per_app_categories_for_team_week(team_state, target_date_str)
			self._recompute_per_app_categories_for_season(season_team_state, target_date_str)

			for rollup in player_rollups:
				player = rollup["player"]
				player_id = str(player.get("id", "")).strip()
				if not player_id:
					continue
				player_state = team_state["players"].setdefault(
					player_id,
					{
						"player_id": player_id,
						"player_name": player.get("fullname") or player.get("lastname") or player_id,
						"roster_pos": player.get("roster_pos"),
						"categories": {},
						"period_snapshot": {},
					},
				)
				player_state["player_name"] = player.get("fullname") or player_state.get("player_name")
				player_state["roster_pos"] = player.get("roster_pos") or player_state.get("roster_pos")
				player_state["status"] = player.get("status")
				player_state["counted_for_team_totals"] = bool(rollup.get("is_active"))
				player_today_totals = rollup.get("today_totals", {})
				player_period_totals = rollup.get("period_totals", {})
				player_state["period_snapshot"] = player_period_totals
				for name, value in player_today_totals.items():
					category_state = player_state["categories"].setdefault(
						name,
						{
							"daily_values": {},
							"weekly_total": 0.0,
						},
					)
					if value is None:
						continue
					category_state["daily_values"][target_date_str] = round(value, 6)
					category_state["weekly_total"] = round(sum(category_state["daily_values"].values()), 6)

				season_player_state = season_team_state["players"].setdefault(
					player_id,
					{
						"player_id": player_id,
						"player_name": player.get("fullname") or player.get("lastname") or player_id,
						"roster_pos": player.get("roster_pos"),
						"status": player.get("status"),
						"categories": {},
						"derived_inputs": {"daily": {}},
					},
				)
				season_player_state["player_name"] = player.get("fullname") or season_player_state.get("player_name")
				season_player_state["roster_pos"] = player.get("roster_pos") or season_player_state.get("roster_pos")
				season_player_state["status"] = player.get("status")
				player_today_components = rollup.get("today_components", {})
				season_player_inputs = season_player_state.setdefault("derived_inputs", {"daily": {}})
				season_player_inputs.setdefault("daily", {})[target_date_str] = player_today_components
				for name, value in player_today_totals.items():
					season_category_state = season_player_state["categories"].setdefault(
						name,
						{
							"daily_values": {},
							"season_total": 0.0,
						},
					)
					if value is None:
						continue
					season_category_state["daily_values"][target_date_str] = round(value, 6)
					season_category_state["season_total"] = round(sum(season_category_state["daily_values"].values()), 6)
				self._recompute_rate_categories_for_player_season(season_player_state, target_date_str)
				self._recompute_per_app_categories_for_player_season(season_player_state, target_date_str)
				player_count += 1
			updated_team_count += 1

		state_payload["schema_version"] = "1.0"
		state_payload["updated_at_utc"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		state_payload["active_period_key"] = period_key

		latest_payload = self._build_latest_payload(
			target_date_str=target_date_str,
			period_key=period_key,
			period_state=period_state,
			season_roto=state_payload.get("season_roto", {}),
		)

		if not dry_run:
			write_json(state_path, state_payload)
			write_json(latest_path, latest_payload)

		return {
			"status": "ok",
			"state_path": state_path,
			"output_path": latest_path,
			"summary": {
				"period_key": period_key,
				"team_count": updated_team_count,
				"player_count": player_count,
				"matchup_count": len(latest_payload.get("matchups", [])),
				"team_period_mismatch_count": latest_payload.get("validation", {}).get("team_period_mismatch_count", 0),
				"player_period_mismatch_count": latest_payload.get("validation", {}).get("player_period_mismatch_count", 0),
			},
		}

	def _extract_live_teams(self, live_payload):
		try:
			teams = live_payload["body"]["live_scoring"]["teams"]
		except Exception as error:
			raise TeamWeeklyTotalsError("Invalid live scoring payload shape.") from error
		if not isinstance(teams, list):
			raise TeamWeeklyTotalsError("Live scoring teams payload is not a list.")
		return teams

	def _schedule_periods(self, schedule_payload):
		periods = schedule_payload.get("body", {}).get("schedule", {}).get("periods", [])
		if not isinstance(periods, list):
			return []
		return periods

	def _season_start_date(self, periods):
		starts = []
		for period in periods:
			start = self._parse_date(period.get("start"))
			if start is not None:
				starts.append(start)
		if not starts:
			return None
		return min(starts)

	def _resolve_active_period(self, periods, target):
		for period in periods:
			start = self._parse_date(period.get("start"))
			end = self._parse_date(period.get("end"))
			if start is None or end is None:
				continue
			if start <= target <= end:
				return period
		return None

	def _period_key(self, period):
		return f"period_{period.get('id', 'unknown')}"

	def _load_state(self, state_path):
		if state_path.exists():
			try:
				payload = read_json(state_path)
				if isinstance(payload, dict):
					return payload
			except Exception:
				pass
		return {"schema_version": "1.0", "periods": {}, "season_roto": {"teams": {}}}

	def _prune_state_to_schedule_ranges(self, state_payload, periods, season_start):
		period_ranges = {}
		for period in periods:
			key = self._period_key(period)
			start = self._parse_date(period.get("start"))
			end = self._parse_date(period.get("end"))
			if start is not None and end is not None:
				period_ranges[key] = (start, end)

		stored_periods = state_payload.get("periods", {})
		if isinstance(stored_periods, dict):
			for period_key in list(stored_periods.keys()):
				if period_key not in period_ranges:
					stored_periods.pop(period_key, None)
					continue
				start, end = period_ranges[period_key]
				self._prune_period_state_dates(stored_periods.get(period_key, {}), start, end)
				if not stored_periods.get(period_key, {}).get("teams"):
					stored_periods.pop(period_key, None)

		if season_start is None:
			return
		season_roto = state_payload.get("season_roto", {})
		if not isinstance(season_roto, dict):
			return
		teams = season_roto.get("teams", {})
		if not isinstance(teams, dict):
			return
		for team_id in list(teams.keys()):
			team_state = teams.get(team_id, {})
			categories = team_state.get("categories", {})
			if not isinstance(categories, dict):
				continue
			for category_name in list(categories.keys()):
				category_state = categories.get(category_name, {})
				daily = category_state.get("daily_values", {})
				if not isinstance(daily, dict):
					daily = {}
				for date_key in list(daily.keys()):
					date_value = self._parse_date(date_key)
					if date_value is None or date_value < season_start:
						daily.pop(date_key, None)
				category_state["daily_values"] = daily
				category_state["season_total"] = round(sum(daily.values()), 6)
				if not daily:
					categories.pop(category_name, None)
			derived_inputs = team_state.get("derived_inputs", {})
			if isinstance(derived_inputs, dict):
				daily_inputs = derived_inputs.get("daily", {})
				if isinstance(daily_inputs, dict):
					for date_key in list(daily_inputs.keys()):
						date_value = self._parse_date(date_key)
						if date_value is None or date_value < season_start:
							daily_inputs.pop(date_key, None)

			players = team_state.get("players", {})
			if isinstance(players, dict):
				for player_id in list(players.keys()):
					player_state = players.get(player_id, {})
					player_categories = player_state.get("categories", {})
					if isinstance(player_categories, dict):
						for category_name in list(player_categories.keys()):
							category_state = player_categories.get(category_name, {})
							daily = category_state.get("daily_values", {})
							if not isinstance(daily, dict):
								daily = {}
							for date_key in list(daily.keys()):
								date_value = self._parse_date(date_key)
								if date_value is None or date_value < season_start:
									daily.pop(date_key, None)
							category_state["daily_values"] = daily
							category_state["season_total"] = round(sum(daily.values()), 6)
							if not daily:
								player_categories.pop(category_name, None)
					player_inputs = player_state.get("derived_inputs", {})
					if isinstance(player_inputs, dict):
						player_daily_inputs = player_inputs.get("daily", {})
						if isinstance(player_daily_inputs, dict):
							for date_key in list(player_daily_inputs.keys()):
								date_value = self._parse_date(date_key)
								if date_value is None or date_value < season_start:
									player_daily_inputs.pop(date_key, None)
					if not player_categories:
						players.pop(player_id, None)
			if not categories:
				teams.pop(team_id, None)

	def _prune_period_state_dates(self, period_state, start, end):
		if not isinstance(period_state, dict):
			return
		teams = period_state.get("teams", {})
		if not isinstance(teams, dict):
			return
		for team_id in list(teams.keys()):
			team_state = teams.get(team_id, {})
			if not isinstance(team_state, dict):
				continue
			categories = team_state.get("categories", {})
			if isinstance(categories, dict):
				for category_name in list(categories.keys()):
					category_state = categories.get(category_name, {})
					daily = category_state.get("daily_values", {})
					if not isinstance(daily, dict):
						daily = {}
					for date_key in list(daily.keys()):
						date_value = self._parse_date(date_key)
						if date_value is None or date_value < start or date_value > end:
							daily.pop(date_key, None)
					category_state["daily_values"] = daily
					category_state["weekly_total"] = round(sum(daily.values()), 6)
					if not daily:
						categories.pop(category_name, None)
			derived_inputs = team_state.get("derived_inputs", {})
			if isinstance(derived_inputs, dict):
				daily_inputs = derived_inputs.get("daily", {})
				if isinstance(daily_inputs, dict):
					for date_key in list(daily_inputs.keys()):
						date_value = self._parse_date(date_key)
						if date_value is None or date_value < start or date_value > end:
							daily_inputs.pop(date_key, None)
			players = team_state.get("players", {})
			if isinstance(players, dict):
				for player_id in list(players.keys()):
					player_state = players.get(player_id, {})
					player_categories = player_state.get("categories", {})
					if not isinstance(player_categories, dict):
						continue
					for category_name in list(player_categories.keys()):
						category_state = player_categories.get(category_name, {})
						daily = category_state.get("daily_values", {})
						if not isinstance(daily, dict):
							daily = {}
						for date_key in list(daily.keys()):
							date_value = self._parse_date(date_key)
							if date_value is None or date_value < start or date_value > end:
								daily.pop(date_key, None)
						category_state["daily_values"] = daily
						category_state["weekly_total"] = round(sum(daily.values()), 6)
						if not daily:
							player_categories.pop(category_name, None)
					if not player_categories:
						players.pop(player_id, None)
			if not categories:
				teams.pop(team_id, None)

	def _remove_target_date_from_season_roto(self, state_payload, target_date_str):
		season_roto = state_payload.get("season_roto", {})
		if not isinstance(season_roto, dict):
			return
		teams = season_roto.get("teams", {})
		if not isinstance(teams, dict):
			return
		team_keys = list(teams.keys())
		for team_key in team_keys:
			team_state = teams.get(team_key)
			if not isinstance(team_state, dict):
				continue
			categories = team_state.get("categories", {})
			if not isinstance(categories, dict):
				continue
			category_keys = list(categories.keys())
			for category_key in category_keys:
				category_state = categories.get(category_key)
				if not isinstance(category_state, dict):
					continue
				daily_values = category_state.get("daily_values", {})
				if not isinstance(daily_values, dict):
					daily_values = {}
				if target_date_str in daily_values:
					daily_values.pop(target_date_str, None)
				category_state["daily_values"] = daily_values
				category_state["season_total"] = round(sum(daily_values.values()), 6)
				if not daily_values:
					categories.pop(category_key, None)
			if not categories:
				teams.pop(team_key, None)
				continue
			derived_inputs = team_state.get("derived_inputs", {})
			if isinstance(derived_inputs, dict):
				daily_inputs = derived_inputs.get("daily", {})
				if isinstance(daily_inputs, dict) and target_date_str in daily_inputs:
					daily_inputs.pop(target_date_str, None)

			players = team_state.get("players", {})
			if isinstance(players, dict):
				for player_id in list(players.keys()):
					player_state = players.get(player_id, {})
					player_categories = player_state.get("categories", {})
					if not isinstance(player_categories, dict):
						continue
					for category_name in list(player_categories.keys()):
						category_state = player_categories.get(category_name, {})
						daily_values = category_state.get("daily_values", {})
						if not isinstance(daily_values, dict):
							daily_values = {}
						if target_date_str in daily_values:
							daily_values.pop(target_date_str, None)
						category_state["daily_values"] = daily_values
						category_state["season_total"] = round(sum(daily_values.values()), 6)
						if not daily_values:
							player_categories.pop(category_name, None)
					player_inputs = player_state.get("derived_inputs", {})
					if isinstance(player_inputs, dict):
						player_daily_inputs = player_inputs.get("daily", {})
						if isinstance(player_daily_inputs, dict) and target_date_str in player_daily_inputs:
							player_daily_inputs.pop(target_date_str, None)
					if not player_categories:
						players.pop(player_id, None)

	def _remove_target_date_from_all_periods(self, state_payload, target_date_str):
		periods = state_payload.get("periods", {})
		if not isinstance(periods, dict):
			return
		period_keys = list(periods.keys())
		for period_key in period_keys:
			period_state = periods.get(period_key)
			if not isinstance(period_state, dict):
				continue
			teams = period_state.get("teams", {})
			if not isinstance(teams, dict):
				continue
			team_keys = list(teams.keys())
			for team_key in team_keys:
				team_state = teams.get(team_key)
				if not isinstance(team_state, dict):
					continue
				categories = team_state.get("categories", {})
				if not isinstance(categories, dict):
					continue
				category_keys = list(categories.keys())
				for category_key in category_keys:
					category_state = categories.get(category_key)
					if not isinstance(category_state, dict):
						continue
					daily_values = category_state.get("daily_values", {})
					if not isinstance(daily_values, dict):
						daily_values = {}
					if target_date_str in daily_values:
						daily_values.pop(target_date_str, None)
					category_state["daily_values"] = daily_values
					category_state["weekly_total"] = round(sum(daily_values.values()), 6)
					if not daily_values:
						categories.pop(category_key, None)
				if not categories:
					teams.pop(team_key, None)
					continue
				derived_inputs = team_state.get("derived_inputs", {})
				if isinstance(derived_inputs, dict):
					daily_inputs = derived_inputs.get("daily", {})
					if isinstance(daily_inputs, dict) and target_date_str in daily_inputs:
						daily_inputs.pop(target_date_str, None)

				players = team_state.get("players", {})
				if isinstance(players, dict):
					player_keys = list(players.keys())
					for player_key in player_keys:
						player_state = players.get(player_key)
						if not isinstance(player_state, dict):
							continue
						player_categories = player_state.get("categories", {})
						if not isinstance(player_categories, dict):
							continue
						player_category_keys = list(player_categories.keys())
						for player_category_key in player_category_keys:
							player_category_state = player_categories.get(player_category_key)
							if not isinstance(player_category_state, dict):
								continue
							daily_values = player_category_state.get("daily_values", {})
							if not isinstance(daily_values, dict):
								daily_values = {}
							if target_date_str in daily_values:
								daily_values.pop(target_date_str, None)
							player_category_state["daily_values"] = daily_values
							player_category_state["weekly_total"] = round(sum(daily_values.values()), 6)
							if not daily_values:
								player_categories.pop(player_category_key, None)
						if not player_categories:
							players.pop(player_key, None)
			if not teams:
				periods.pop(period_key, None)

	def _build_latest_payload(self, target_date_str, period_key, period_state, season_roto):
		teams_state = period_state.get("teams", {})
		team_rows = []
		team_period_mismatch_count = 0
		player_period_mismatch_count = 0
		for team_id, team_state in teams_state.items():
			category_totals = {}
			latest_snapshot = {}
			max_days = 0
			for category_name, category_state in team_state.get("categories", {}).items():
				category_totals[category_name] = round(self._safe_float(category_state.get("weekly_total")) or 0.0, 6)
				daily_values = category_state.get("daily_values", {})
				latest_snapshot[category_name] = daily_values.get(target_date_str)
				max_days = max(max_days, len(daily_values))
			team_period_check = self._period_consistency_check(category_totals, team_state.get("period_snapshot", {}))
			if team_period_check["mismatch_count"] > 0:
				team_period_mismatch_count += 1

			player_rows = []
			for player_id, player_state in (team_state.get("players", {}) or {}).items():
				player_totals = {}
				player_days = 0
				for category_name, category_state in (player_state.get("categories", {}) or {}).items():
					player_totals[category_name] = round(self._safe_float(category_state.get("weekly_total")) or 0.0, 6)
					player_days = max(player_days, len(category_state.get("daily_values", {})))
				player_check = self._period_consistency_check(player_totals, player_state.get("period_snapshot", {}))
				if player_check["mismatch_count"] > 0:
					player_period_mismatch_count += 1
				player_rows.append(
					{
						"player_id": player_id,
						"player_name": player_state.get("player_name"),
						"roster_pos": player_state.get("roster_pos"),
						"status": player_state.get("status"),
						"counted_for_team_totals": bool(player_state.get("counted_for_team_totals")),
						"category_totals": player_totals,
						"period_snapshot": player_state.get("period_snapshot", {}),
						"period_check": player_check,
						"days_captured": player_days,
					}
				)
			player_rows.sort(key=lambda row: str(row.get("player_name") or row.get("player_id")))

			team_rows.append(
				{
					"team_id": team_id,
					"team_name": team_state.get("team_name"),
					"team_abbr": team_state.get("team_abbr"),
					"category_totals": category_totals,
					"latest_snapshot": latest_snapshot,
					"period_snapshot": team_state.get("period_snapshot", {}),
					"period_check": team_period_check,
					"days_captured": max_days,
					"players": player_rows,
				}
			)
		team_rows.sort(key=lambda row: str(row.get("team_abbr") or row.get("team_name") or row.get("team_id")))

		matchups = []
		team_by_id = {str(row.get("team_id")): row for row in team_rows}
		for matchup in period_state.get("period", {}).get("matchups", []) or []:
			away_team = matchup.get("away_team", {})
			home_team = matchup.get("home_team", {})
			away_id = str(away_team.get("id", ""))
			home_id = str(home_team.get("id", ""))
			away_row = team_by_id.get(away_id, {})
			home_row = team_by_id.get(home_id, {})
			score = self._compute_matchup_score(away_row, home_row)
			matchups.append(
				{
					"matchup_id": str(matchup.get("id", "")),
					"away_team_id": away_id,
					"home_team_id": home_id,
					"away_team_abbr": away_team.get("long_abbr") or away_team.get("name"),
					"home_team_abbr": home_team.get("long_abbr") or home_team.get("name"),
					"score": score,
				}
			)

		return {
			"schema_version": "1.0",
			"target_date": target_date_str,
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"period_key": period_key,
			"period": period_state.get("period", {}),
			"teams": team_rows,
			"matchups": matchups,
			"season_roto": self._build_season_roto_snapshot(season_roto, target_date_str),
			"validation": {
				"team_period_mismatch_count": team_period_mismatch_count,
				"player_period_mismatch_count": player_period_mismatch_count,
			},
		}

	def _compute_matchup_score(self, away_row, home_row):
		away_totals = away_row.get("category_totals", {})
		home_totals = home_row.get("category_totals", {})
		if not away_totals and not home_totals:
			return {"away": 0.0, "home": 12.0}

		lower_is_better = {"ERA", "aWHIP", "HRA"}
		away_points = 0.0
		home_points = 0.0
		for category in self.SCORE_CATEGORIES:
			away_value = self._safe_float(away_totals.get(category))
			home_value = self._safe_float(home_totals.get(category))
			if away_value is None:
				away_value = 0.0
			if home_value is None:
				home_value = 0.0
			if abs(away_value - home_value) < 1e-12:
				# League tiebreak rule: tied category goes to home team.
				home_points += 1.0
				continue
			if category in lower_is_better:
				away_wins = away_value < home_value
			else:
				away_wins = away_value > home_value
			if away_wins:
				away_points += 1.0
			else:
				home_points += 1.0
		return {"away": round(away_points, 3), "home": round(home_points, 3)}

	def _parse_date(self, value):
		if not value:
			return None
		text = str(value).strip()
		for fmt in ("%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"):
			try:
				return datetime.strptime(text, fmt).date()
			except ValueError:
				continue
		return None

	def _safe_float(self, value):
		try:
			return float(value)
		except Exception:
			return None

	def _normalize_category_name(self, value):
		if value is None:
			return None
		name = str(value).strip()
		if not name:
			return None
		if name == "ASB":
			return "aSB"
		return name

	def _build_season_roto_snapshot(self, season_roto, target_date_str):
		teams = season_roto.get("teams", {}) if isinstance(season_roto, dict) else {}
		rows = []
		for team_id, team_state in (teams or {}).items():
			categories = {}
			for category_name, category_state in (team_state.get("categories", {}) or {}).items():
				categories[category_name] = round(self._safe_float(category_state.get("season_total")) or 0.0, 6)
			player_rows = []
			for player_id, player_state in (team_state.get("players", {}) or {}).items():
				player_categories = {}
				for category_name, category_state in (player_state.get("categories", {}) or {}).items():
					player_categories[category_name] = round(self._safe_float(category_state.get("season_total")) or 0.0, 6)
				player_rows.append(
					{
						"player_id": str(player_id),
						"player_name": player_state.get("player_name"),
						"roster_pos": player_state.get("roster_pos"),
						"status": player_state.get("status"),
						"category_totals": player_categories,
					}
				)
			player_rows.sort(key=lambda row: str(row.get("player_name") or row.get("player_id")))
			rows.append(
				{
					"team_id": str(team_id),
					"team_name": team_state.get("team_name"),
					"team_abbr": team_state.get("team_abbr"),
					"category_totals": categories,
					"players": player_rows,
				}
			)
		rows.sort(key=lambda row: str(row.get("team_abbr") or row.get("team_name") or row.get("team_id")))
		return {"target_date": target_date_str, "teams": rows}

	def _extract_player_rollups(self, team):
		rollups = []
		for player in team.get("players", []) or []:
			status = str(player.get("status", "")).strip().lower()
			today_parsed = self._parse_stats_blob_detailed(player.get("stats_today", ""))
			period_parsed = self._parse_stats_blob_detailed(player.get("stats_period", ""))
			rollups.append(
				{
					"player": player,
					"is_active": status == "active",
					"today_totals": today_parsed["categories"],
					"period_totals": period_parsed["categories"],
					"today_components": today_parsed["components"],
					"period_components": period_parsed["components"],
				}
			)
		return rollups

	def _extract_team_today_totals(self, team, player_rollups):
		from_active_players = {}
		for rollup in player_rollups:
			if not rollup.get("is_active"):
				continue
			self._merge_totals(from_active_players, rollup.get("today_totals", {}))
		if from_active_players:
			return from_active_players

		combined_today = " , ".join(
			[
				str(team.get("active_live_stats_today", "") or ""),
				str(team.get("reserve_live_stats_today", "") or ""),
			]
		).strip(" ,")
		parsed = self._parse_stats_blob(combined_today)
		if parsed:
			return parsed
		# Fallback for off-season / sparse payloads that don't populate *_today fields.
		fallback = {}
		for category in team.get("categories", []):
			name = self._normalize_category_name(category.get("name"))
			if not name:
				continue
			value = self._safe_float(category.get("value"))
			if value is None:
				continue
			fallback[name] = value
		return fallback

	def _extract_team_period_totals(self, team, player_rollups):
		from_active_players = {}
		for rollup in player_rollups:
			if not rollup.get("is_active"):
				continue
			self._merge_totals(from_active_players, rollup.get("period_totals", {}))
		if from_active_players:
			return from_active_players

		combined_period = str(team.get("active_live_stats", "") or "").strip()
		return self._parse_stats_blob(combined_period)

	def _extract_team_today_components(self, player_rollups):
		combined = self._empty_components()
		for rollup in player_rollups:
			if not rollup.get("is_active"):
				continue
			self._merge_components(combined, rollup.get("today_components", {}))
		return combined

	def _period_consistency_check(self, accumulated_totals, period_snapshot):
		categories = sorted(set(accumulated_totals.keys()) | set(period_snapshot.keys()))
		mismatches = {}
		for category in categories:
			accumulated = self._safe_float(accumulated_totals.get(category))
			period_value = self._safe_float(period_snapshot.get(category))
			if accumulated is None or period_value is None:
				continue
			diff = round(accumulated - period_value, 6)
			if abs(diff) >= 0.001:
				mismatches[category] = {
					"accumulated": round(accumulated, 6),
					"stats_period": round(period_value, 6),
					"difference": diff,
				}
		return {
			"mismatch_count": len(mismatches),
			"mismatches": mismatches,
		}

	def _parse_stats_blob(self, text):
		if not text:
			return {}
		normalized = str(text).replace(" - ", ", ")
		parts = [part.strip() for part in normalized.split(",") if str(part).strip()]
		values = {}

		def add_value(category_name, numeric_value):
			name = self._normalize_category_name(category_name)
			if not name or name not in self.TRACKED_CATEGORIES:
				return
			values[name] = round((values.get(name, 0.0) + float(numeric_value)), 6)

		for part in parts:
			numbered = re.match(r"^(-?\d+(?:\.\d+)?)\s+([A-Za-z][A-Za-z0-9]*)$", part)
			if numbered:
				add_value(numbered.group(2), numbered.group(1))
				continue
			for standalone in ("HR", "HRA", "ASB", "aSB"):
				if part == standalone:
					add_value(standalone, 1.0)
					break
		return values

	def _merge_totals(self, target, source):
		for name, value in (source or {}).items():
			if value is None:
				continue
			target[name] = round((target.get(name, 0.0) + float(value)), 6)

	def _empty_components(self):
		return {
			"AB": 0.0,
			"H": 0.0,
			"BB": 0.0,
			"TB": 0.0,
			"IP_OUTS": 0.0,
			"ER": 0.0,
			"HA": 0.0,
			"HB": 0.0,
			"BBI": 0.0,
			"MGS_SUM": 0.0,
			"VIJAY_SUM": 0.0,
			"PITCH_APPS": 0.0,
		}

	def _merge_components(self, target, source):
		for key in target.keys():
			target[key] = round(float(target.get(key, 0.0)) + float((source or {}).get(key, 0.0)), 6)

	def _aggregate_component_daily(self, daily_inputs):
		combined = self._empty_components()
		for value in (daily_inputs or {}).values():
			self._merge_components(combined, value or {})
		return combined

	def _compute_rates_from_components(self, components):
		ab = float(components.get("AB", 0.0) or 0.0)
		h = float(components.get("H", 0.0) or 0.0)
		bb = float(components.get("BB", 0.0) or 0.0)
		tb = float(components.get("TB", 0.0) or 0.0)
		ip_outs = float(components.get("IP_OUTS", 0.0) or 0.0)
		er = float(components.get("ER", 0.0) or 0.0)
		ha = float(components.get("HA", 0.0) or 0.0)
		hb = float(components.get("HB", 0.0) or 0.0)
		bbi = float(components.get("BBI", 0.0) or 0.0)
		denom_obp = ab + bb
		obp = ((h + bb) / denom_obp) if denom_obp > 0 else 0.0
		slg = (tb / ab) if ab > 0 else 0.0
		ops = obp + slg
		ip = (ip_outs / 3.0) if ip_outs > 0 else 0.0
		era = (9.0 * er / ip) if ip > 0 else 0.0
		awhip = ((ha + hb + bbi) / ip) if ip > 0 else 0.0
		return {"OBP": obp, "OPS": ops, "ERA": era, "aWHIP": awhip}

	def _recompute_rate_categories_for_team_week(self, team_state, target_date_str):
		derived_inputs = team_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		weekly_components = self._aggregate_component_daily(daily)
		weekly_rates = self._compute_rates_from_components(weekly_components)
		day_components = daily.get(target_date_str, {})
		day_rates = self._compute_rates_from_components(day_components or {})
		for category in self.DERIVED_RATE_CATEGORIES:
			category_state = team_state.setdefault("categories", {}).setdefault(
				category,
				{"is_bad": category in {"ERA", "aWHIP"}, "daily_values": {}, "weekly_total": 0.0},
			)
			category_state["daily_values"][target_date_str] = round(day_rates.get(category, 0.0), 6)
			category_state["weekly_total"] = round(weekly_rates.get(category, 0.0), 6)

	def _recompute_rate_categories_for_season(self, season_team_state, target_date_str):
		derived_inputs = season_team_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		season_components = self._aggregate_component_daily(daily)
		season_rates = self._compute_rates_from_components(season_components)
		day_components = daily.get(target_date_str, {})
		day_rates = self._compute_rates_from_components(day_components or {})
		for category in self.DERIVED_RATE_CATEGORIES:
			category_state = season_team_state.setdefault("categories", {}).setdefault(
				category,
				{"daily_values": {}, "season_total": 0.0},
			)
			category_state["daily_values"][target_date_str] = round(day_rates.get(category, 0.0), 6)
			category_state["season_total"] = round(season_rates.get(category, 0.0), 6)

	def _recompute_per_app_categories_for_team_week(self, team_state, target_date_str):
		derived_inputs = team_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		weekly_components = self._aggregate_component_daily(daily)
		day_components = daily.get(target_date_str, {})
		if (
			float(weekly_components.get("MGS_SUM", 0.0) or 0.0) <= 0.0
			and float(weekly_components.get("VIJAY_SUM", 0.0) or 0.0) <= 0.0
		):
			# No parsed per-appearance inputs available; keep prior additive fallback values.
			return
		for category, sum_key in (("MGS", "MGS_SUM"), ("VIJAY", "VIJAY_SUM")):
			category_state = team_state.setdefault("categories", {}).setdefault(
				category,
				{"is_bad": False, "daily_values": {}, "weekly_total": 0.0},
			)
			day_value = float((day_components or {}).get(sum_key, 0.0) or 0.0)
			weekly_value = float(weekly_components.get(sum_key, 0.0) or 0.0)
			category_state["daily_values"][target_date_str] = round(day_value, 6)
			category_state["weekly_total"] = round(weekly_value, 6)

	def _recompute_per_app_categories_for_season(self, season_team_state, target_date_str):
		derived_inputs = season_team_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		season_components = self._aggregate_component_daily(daily)
		day_components = daily.get(target_date_str, {})
		if (
			float(season_components.get("MGS_SUM", 0.0) or 0.0) <= 0.0
			and float(season_components.get("VIJAY_SUM", 0.0) or 0.0) <= 0.0
		):
			# No parsed per-appearance inputs available; keep prior additive fallback values.
			return
		for category, sum_key in (("MGS", "MGS_SUM"), ("VIJAY", "VIJAY_SUM")):
			category_state = season_team_state.setdefault("categories", {}).setdefault(
				category,
				{"daily_values": {}, "season_total": 0.0},
			)
			day_value = float((day_components or {}).get(sum_key, 0.0) or 0.0)
			season_value = float(season_components.get(sum_key, 0.0) or 0.0)
			category_state["daily_values"][target_date_str] = round(day_value, 6)
			category_state["season_total"] = round(season_value, 6)

	def _recompute_rate_categories_for_player_season(self, season_player_state, target_date_str):
		derived_inputs = season_player_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		season_components = self._aggregate_component_daily(daily)
		season_rates = self._compute_rates_from_components(season_components)
		day_components = daily.get(target_date_str, {})
		day_rates = self._compute_rates_from_components(day_components or {})
		for category in self.DERIVED_RATE_CATEGORIES:
			category_state = season_player_state.setdefault("categories", {}).setdefault(
				category,
				{"daily_values": {}, "season_total": 0.0},
			)
			category_state["daily_values"][target_date_str] = round(day_rates.get(category, 0.0), 6)
			category_state["season_total"] = round(season_rates.get(category, 0.0), 6)

	def _recompute_per_app_categories_for_player_season(self, season_player_state, target_date_str):
		derived_inputs = season_player_state.get("derived_inputs", {})
		daily = derived_inputs.get("daily", {}) if isinstance(derived_inputs, dict) else {}
		season_components = self._aggregate_component_daily(daily)
		day_components = daily.get(target_date_str, {})
		if (
			float(season_components.get("MGS_SUM", 0.0) or 0.0) <= 0.0
			and float(season_components.get("VIJAY_SUM", 0.0) or 0.0) <= 0.0
		):
			return
		for category, sum_key in (("MGS", "MGS_SUM"), ("VIJAY", "VIJAY_SUM")):
			category_state = season_player_state.setdefault("categories", {}).setdefault(
				category,
				{"daily_values": {}, "season_total": 0.0},
			)
			day_value = float((day_components or {}).get(sum_key, 0.0) or 0.0)
			season_value = float(season_components.get(sum_key, 0.0) or 0.0)
			category_state["daily_values"][target_date_str] = round(day_value, 6)
			category_state["season_total"] = round(season_value, 6)

	def _parse_stats_blob_detailed(self, text):
		if not text:
			return {"categories": {}, "components": self._empty_components()}
		normalized = str(text).replace(" - ", ", ")
		parts = [part.strip() for part in normalized.split(",") if str(part).strip()]
		values = {}
		components = self._empty_components()
		hits_from_ab = 0.0
		doubles = 0.0
		triples = 0.0
		homers = 0.0

		def add_value(category_name, numeric_value):
			name = self._normalize_category_name(category_name)
			if not name or name not in self.TRACKED_CATEGORIES:
				return
			values[name] = round((values.get(name, 0.0) + float(numeric_value)), 6)

		for part in parts:
			hit_ab = re.match(r"^(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)$", part)
			if hit_ab:
				hits = self._safe_float(hit_ab.group(1)) or 0.0
				ab = self._safe_float(hit_ab.group(2)) or 0.0
				components["H"] += hits
				components["AB"] += ab
				hits_from_ab += hits
				continue

			numbered = re.match(r"^(-?\d+(?:\.\d+)?)\s+([A-Za-z][A-Za-z0-9]*)$", part)
			if numbered:
				num_raw = numbered.group(1)
				num = self._safe_float(num_raw) or 0.0
				token = numbered.group(2)
				add_value(token, num)
				normalized_token = self._normalize_category_name(token)
				if normalized_token == "BB":
					components["BB"] += num
				elif normalized_token == "BBI":
					components["BBI"] += num
				elif normalized_token == "ER":
					components["ER"] += num
				elif normalized_token == "HA":
					components["HA"] += num
				elif normalized_token == "HB":
					components["HB"] += num
				elif normalized_token == "HBP":
					components["HB"] += num
				elif normalized_token == "INN":
					components["IP_OUTS"] += self._innings_to_outs(num_raw)
				elif normalized_token == "MGS":
					components["MGS_SUM"] += num
				elif normalized_token == "VIJAY":
					components["VIJAY_SUM"] += num
				elif normalized_token == "2B":
					doubles += num
				elif normalized_token == "3B":
					triples += num
				elif normalized_token == "HR":
					homers += num
				continue

			for standalone in ("HR", "HRA", "ASB", "aSB", "BB", "HB", "HBP"):
				if part == standalone:
					add_value(standalone, 1.0)
					if standalone in {"ASB", "aSB"}:
						pass
					elif standalone == "HR":
						homers += 1.0
					elif standalone == "BB":
						components["BB"] += 1.0
					elif standalone in {"HB", "HBP"}:
						components["HB"] += 1.0
					break

		if hits_from_ab > 0:
			singles = max(hits_from_ab - doubles - triples - homers, 0.0)
			components["TB"] += singles + (2.0 * doubles) + (3.0 * triples) + (4.0 * homers)
		if self._has_pitching_activity(values, components):
			components["PITCH_APPS"] += 1.0
		return {"categories": values, "components": components}

	def _innings_to_outs(self, raw_value):
		text = str(raw_value).strip()
		if "." in text:
			whole_str, frac_str = text.split(".", 1)
			whole = int(whole_str or "0")
			frac = frac_str[:1] if frac_str else "0"
			if frac == "1":
				return float((whole * 3) + 1)
			if frac == "2":
				return float((whole * 3) + 2)
			return float(whole * 3)
		value = self._safe_float(text) or 0.0
		return float(value * 3.0)

	def _has_pitching_activity(self, categories, components):
		pitching_tokens = {"K", "HRA", "MGS", "VIJAY", "ERA", "aWHIP"}
		if any(token in categories for token in pitching_tokens):
			return True
		if float(components.get("IP_OUTS", 0.0) or 0.0) > 0:
			return True
		if float(components.get("ER", 0.0) or 0.0) > 0:
			return True
		if float(components.get("HA", 0.0) or 0.0) > 0:
			return True
		if float(components.get("HB", 0.0) or 0.0) > 0:
			return True
		if float(components.get("BBI", 0.0) or 0.0) > 0:
			return True
		return False
