from datetime import datetime, timedelta, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_artifact_history_latest_path,
	get_ingestion_config,
	get_ingestion_raw_dir,
	get_weekly_calibration_latest_path,
)


UTC = timezone.utc


class WeeklyCalibrationError(RuntimeError):
	pass


class WeeklyCalibrationBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.calibration_cfg = self.ingestion_cfg.get("calibration", {})
		self.projections_cfg = self.ingestion_cfg.get("projections", {})
		self.week_end_weekday = int(self.projections_cfg.get("scoring_week_end_weekday", 6))
		if self.week_end_weekday < 0 or self.week_end_weekday > 6:
			self.week_end_weekday = 6

	def build(self, target_date, dry_run=False):
		output_path = get_weekly_calibration_latest_path()
		if not self.calibration_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "WEEKLY_CALIBRATION_DISABLED", "output_path": output_path}

		week_start, week_end = self._previous_completed_week(target_date)
		projection_payload = self._load_projection_for_week(week_start, week_end)
		if projection_payload is None:
			return {"status": "skipped", "reason": "WEEKLY_CALIBRATION_PROJECTION_MISSING", "output_path": output_path}

		ytd_end = self._load_ytd_points(week_end)
		ytd_start = self._load_ytd_points(week_end - timedelta(days=7))
		if not ytd_end or not ytd_start:
			return {"status": "skipped", "reason": "WEEKLY_CALIBRATION_REALIZED_MISSING", "output_path": output_path}

		rows = []
		for player in projection_payload.get("players", []):
			player_id = str(player.get("player_id"))
			if player_id not in ytd_end or player_id not in ytd_start:
				continue
			projected = self._float_value(player.get("projected_points_window"))
			realized = self._float_value(ytd_end[player_id]) - self._float_value(ytd_start[player_id])
			error = realized - projected
			rows.append(
				{
					"player_id": player_id,
					"player_name": player.get("player_name", f"UNKNOWN_{player_id}"),
					"player_role": player.get("player_role", "unknown"),
					"projected_points_week": round(projected, 6),
					"realized_points_week": round(realized, 6),
					"error_points": round(error, 6),
					"abs_error_points": round(abs(error), 6),
				}
			)

		overall = self._metric_snapshot(rows)
		by_role = {}
		for role in ("batters", "sp", "rp", "unknown"):
			role_rows = [row for row in rows if str(row.get("player_role", "unknown")) == role]
			if role_rows:
				by_role[role] = self._metric_snapshot(role_rows)

		trend = self._trend_snapshot(overall.get("mae_points"))
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"calibration_week": {
				"start_date": week_start.strftime("%Y-%m-%d"),
				"end_date": week_end.strftime("%Y-%m-%d"),
			},
			"summary": {
				"players_calibrated": len(rows),
			},
			"metrics": {
				"overall": overall,
				"by_role": by_role,
				"trend": trend,
			},
			"players": rows[:500],
		}
		if not dry_run:
			write_json(output_path, payload)
		return {"status": "ok", "output_path": output_path, "summary": payload["summary"]}

	def _previous_completed_week(self, target_date):
		target_day = target_date if isinstance(target_date, datetime) else datetime.combine(target_date, datetime.min.time())
		current_weekday = int(target_day.weekday())
		days_since_week_end = (current_weekday - self.week_end_weekday) % 7
		if days_since_week_end == 0:
			days_since_week_end = 7
		week_end = target_day - timedelta(days=days_since_week_end)
		week_start = week_end - timedelta(days=6)
		return week_start, week_end

	def _load_projection_for_week(self, week_start, week_end):
		history_root = get_artifact_history_latest_path().parent / "history"
		for offset in range(0, 8):
			candidate_day = week_start + timedelta(days=offset)
			candidate_path = history_root / candidate_day.strftime("%Y-%m-%d") / "player_projection_weekly_latest.json"
			if not candidate_path.exists():
				continue
			payload = read_json(candidate_path)
			window = payload.get("window", {})
			if window.get("start_date") == week_start.strftime("%Y-%m-%d") and window.get("end_date") == week_end.strftime("%Y-%m-%d"):
				return payload
		return None

	def _load_ytd_points(self, date_value):
		raw_dir = get_ingestion_raw_dir(date_value)
		roster_path = raw_dir / f"rosters_{date_value.strftime('%Y-%m-%d')}.json"
		if not roster_path.exists():
			return {}
		payload = read_json(roster_path)
		teams = payload.get("body", {}).get("rosters", {}).get("teams", [])
		points = {}
		for team in teams:
			for player in team.get("players", []):
				player_id = player.get("id")
				if player_id is None:
					continue
				try:
					points[str(player_id)] = float(player.get("ytd_points"))
				except Exception:
					continue
		return points

	def _metric_snapshot(self, rows):
		if not rows:
			return {"count": 0, "mae_points": None, "bias_points": None}
		count = len(rows)
		mae = sum(self._float_value(row.get("abs_error_points")) for row in rows) / count
		bias = sum(self._float_value(row.get("error_points")) for row in rows) / count
		return {"count": count, "mae_points": round(mae, 6), "bias_points": round(bias, 6)}

	def _trend_snapshot(self, current_mae):
		trend_weeks = int(self.calibration_cfg.get("trend_weeks", 4))
		degrade_pct = float(self.calibration_cfg.get("degrade_mae_pct", 5.0))
		history_root = get_artifact_history_latest_path().parent / "history"
		mae_values = []
		if history_root.exists():
			for child in sorted(history_root.iterdir(), reverse=True):
				if not child.is_dir():
					continue
				calibration_path = child / "weekly_calibration_latest.json"
				if not calibration_path.exists():
					continue
				payload = read_json(calibration_path)
				value = payload.get("metrics", {}).get("overall", {}).get("mae_points")
				if isinstance(value, (int, float)):
					mae_values.append(float(value))
				if len(mae_values) >= trend_weeks:
					break
		if current_mae is None or not mae_values:
			return {"status": "unknown", "recent_mae_points": []}
		recent_avg = sum(mae_values) / len(mae_values)
		upper = recent_avg * (1.0 + (degrade_pct / 100.0))
		lower = recent_avg * (1.0 - (degrade_pct / 100.0))
		if current_mae > upper:
			status = "degrading"
		elif current_mae < lower:
			status = "improving"
		else:
			status = "flat"
		return {
			"status": status,
			"recent_mae_points": [round(value, 6) for value in mae_values],
			"recent_avg_mae_points": round(recent_avg, 6),
		}

	def _float_value(self, value):
		try:
			return float(value)
		except Exception:
			return 0.0
