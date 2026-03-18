from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_player_projection_daily_latest_path,
	get_player_projection_weekly_latest_path,
	get_view_gm_daily_latest_path,
	get_view_gm_weekly_latest_path,
	get_view_league_daily_latest_path,
	get_view_league_weekly_latest_path,
)


UTC = timezone.utc


class ViewModelError(RuntimeError):
	pass


class ViewModelBuilder:
	def build(self, target_date, dry_run=False):
		daily_projection_path = get_player_projection_daily_latest_path()
		weekly_projection_path = get_player_projection_weekly_latest_path()
		league_daily_path = get_view_league_daily_latest_path()
		league_weekly_path = get_view_league_weekly_latest_path()
		gm_daily_path = get_view_gm_daily_latest_path()
		gm_weekly_path = get_view_gm_weekly_latest_path()

		if not daily_projection_path.exists() or not weekly_projection_path.exists():
			return {
				"status": "skipped",
				"reason": "PROJECTION_HORIZON_ARTIFACTS_MISSING",
				"outputs": {
					"view_league_daily": league_daily_path,
					"view_league_weekly": league_weekly_path,
					"view_gm_daily": gm_daily_path,
					"view_gm_weekly": gm_weekly_path,
				},
			}

		daily_projection = read_json(daily_projection_path)
		weekly_projection = read_json(weekly_projection_path)

		league_daily_payload = self._build_league_payload(
			target_date=target_date,
			horizon_name="daily",
			projection_payload=daily_projection,
		)
		league_weekly_payload = self._build_league_payload(
			target_date=target_date,
			horizon_name="weekly",
			projection_payload=weekly_projection,
		)
		gm_daily_payload = self._build_gm_payload(
			target_date=target_date,
			horizon_name="daily",
			projection_payload=daily_projection,
		)
		gm_weekly_payload = self._build_gm_payload(
			target_date=target_date,
			horizon_name="weekly",
			projection_payload=weekly_projection,
		)

		if not dry_run:
			write_json(league_daily_path, league_daily_payload)
			write_json(league_weekly_path, league_weekly_payload)
			write_json(gm_daily_path, gm_daily_payload)
			write_json(gm_weekly_path, gm_weekly_payload)

		return {
			"status": "ok",
			"outputs": {
				"view_league_daily": league_daily_path,
				"view_league_weekly": league_weekly_path,
				"view_gm_daily": gm_daily_path,
				"view_gm_weekly": gm_weekly_path,
			},
			"summary": {
				"daily_players": len(daily_projection.get("players", [])),
				"weekly_players": len(weekly_projection.get("players", [])),
			},
		}

	def _build_league_payload(self, target_date, horizon_name, projection_payload):
		players = list(projection_payload.get("players", []))
		window = projection_payload.get("window", {})
		generated_at_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		players_sorted_points = sorted(players, key=lambda row: self._float_value(row.get("projected_points_window")), reverse=True)
		players_with_delta = [row for row in players if isinstance(row.get("performance_delta"), (int, float))]
		players_sorted_over = sorted(players_with_delta, key=lambda row: self._float_value(row.get("performance_delta")), reverse=True)
		players_sorted_under = sorted(players_with_delta, key=lambda row: self._float_value(row.get("performance_delta")))
		weekly_summary = self._weekly_market_summary(players) if horizon_name == "weekly" else None
		payload = {
			"schema_version": "1.0",
			"view_type": "league",
			"horizon": horizon_name,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"generated_at_utc": generated_at_utc,
			"window": window,
			"summary": {
				"player_count": len(players),
				"projected_points_total": round(sum(self._float_value(row.get("projected_points_window")) for row in players), 6),
			},
			"leaders": {
				"projected_points": [self._leader_row(row, include_delta=False) for row in players_sorted_points[:15]],
				"overperformers": [self._leader_row(row, include_delta=True) for row in players_sorted_over[:10]],
				"underperformers": [self._leader_row(row, include_delta=True) for row in players_sorted_under[:10]],
			},
		}
		if weekly_summary is not None:
			payload["weekly_summary"] = weekly_summary
		return payload

	def _build_gm_payload(self, target_date, horizon_name, projection_payload):
		players = list(projection_payload.get("players", []))
		window = projection_payload.get("window", {})
		generated_at_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		players_sorted = sorted(players, key=lambda row: self._float_value(row.get("projected_points_window")), reverse=True)
		role_counts = {}
		for row in players_sorted:
			role = str(row.get("player_role", "unknown"))
			role_counts[role] = role_counts.get(role, 0) + 1

		return {
			"schema_version": "1.0",
			"view_type": "gm",
			"horizon": horizon_name,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"generated_at_utc": generated_at_utc,
			"window": window,
			"summary": {
				"player_count": len(players_sorted),
				"role_counts": role_counts,
				"projected_points_total": round(sum(self._float_value(row.get("projected_points_window")) for row in players_sorted), 6),
			},
			"players": players_sorted,
		}

	def _leader_row(self, row, include_delta):
		payload = {
			"player_id": str(row.get("player_id", "")),
			"player_name": row.get("player_name", ""),
			"player_role": row.get("player_role", "unknown"),
			"projected_points_window": round(self._float_value(row.get("projected_points_window")), 6),
			"performance_flag": row.get("performance_flag", "insufficient_data"),
		}
		if include_delta:
			payload["performance_delta"] = row.get("performance_delta")
		return payload

	def _float_value(self, value):
		try:
			return float(value)
		except Exception:
			return 0.0

	def _weekly_market_summary(self, players):
		flag_counts = {"overperforming": 0, "underperforming": 0, "on_track": 0, "insufficient_data": 0}
		for row in players:
			flag = str(row.get("performance_flag", "insufficient_data"))
			if flag not in flag_counts:
				flag = "insufficient_data"
			flag_counts[flag] += 1

		categories = ["aRBI", "aSB", "MGS", "VIJAY"]
		category_summary = {}
		for category in categories:
			with_category_flag = [row for row in players if isinstance(row.get("category_performance_flags"), dict)]
			over_rows = [row for row in with_category_flag if row.get("category_performance_flags", {}).get(category) == "overperforming"]
			under_rows = [row for row in with_category_flag if row.get("category_performance_flags", {}).get(category) == "underperforming"]
			top_over = sorted(over_rows, key=lambda row: self._float_value(row.get("category_delta_pct", {}).get(category)), reverse=True)
			top_under = sorted(under_rows, key=lambda row: self._float_value(row.get("category_delta_pct", {}).get(category)))
			category_summary[category] = {
				"overperforming_count": len(over_rows),
				"underperforming_count": len(under_rows),
				"top_overperformers": [self._category_row(row, category) for row in top_over[:10]],
				"top_underperformers": [self._category_row(row, category) for row in top_under[:10]],
			}

		return {
			"overall_performance_counts": flag_counts,
			"category_summary": category_summary,
		}

	def _category_row(self, row, category):
		return {
			"player_id": str(row.get("player_id", "")),
			"player_name": row.get("player_name", ""),
			"player_role": row.get("player_role", "unknown"),
			"category": category,
			"category_delta_pct": row.get("category_delta_pct", {}).get(category),
			"category_flag": row.get("category_performance_flags", {}).get(category, "insufficient_data"),
		}
