from datetime import datetime, timedelta, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_ingestion_config,
	get_player_projection_daily_latest_path,
	get_player_projection_deltas_latest_path,
	get_player_projection_weekly_latest_path,
	get_preseason_player_priors_path,
)


UTC = timezone.utc


class ProjectionHorizonError(RuntimeError):
	pass


class ProjectionHorizonBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.player_blend_cfg = self.ingestion_cfg.get("player_blend", {})
		self.projections_cfg = self.ingestion_cfg.get("projections", {})
		legacy_week_end = self.player_blend_cfg.get("scoring_week_end_weekday")
		week_end_value = self.projections_cfg.get("scoring_week_end_weekday", legacy_week_end if legacy_week_end is not None else 6)
		self.week_end_weekday = int(week_end_value)
		if self.week_end_weekday < 0 or self.week_end_weekday > 6:
			self.week_end_weekday = 6

	def build(self, target_date, dry_run=False):
		daily_path = get_player_projection_daily_latest_path()
		weekly_path = get_player_projection_weekly_latest_path()
		priors_path = get_preseason_player_priors_path()
		blend_path = get_player_projection_deltas_latest_path()
		if not priors_path.exists() or not blend_path.exists():
			return {
				"status": "skipped",
				"reason": "PROJECTION_INPUTS_MISSING",
				"daily_output_path": daily_path,
				"weekly_output_path": weekly_path,
			}

		priors_payload = read_json(priors_path)
		blend_payload = read_json(blend_path)
		priors_by_player = {str(player["player_id"]): player for player in priors_payload.get("players", [])}
		blend_by_player = {str(player["player_id"]): player for player in blend_payload.get("players", [])}
		if not priors_by_player or not blend_by_player:
			return {
				"status": "skipped",
				"reason": "PROJECTION_INPUTS_EMPTY",
				"daily_output_path": daily_path,
				"weekly_output_path": weekly_path,
			}

		season_days = max(1.0, float(self.player_blend_cfg.get("season_days", 183)))
		daily_rows = self._build_window_rows(priors_by_player, blend_by_player, window_days=1, season_days=season_days)
		weekly_start, weekly_end, weekly_days = self._weekly_window(target_date)
		weekly_rows = self._build_window_rows(priors_by_player, blend_by_player, window_days=weekly_days, season_days=season_days)

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		daily_payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"window": {
				"label": "daily",
				"start_date": target_date.strftime("%Y-%m-%d"),
				"end_date": target_date.strftime("%Y-%m-%d"),
				"days": 1,
			},
			"summary": {"players": len(daily_rows)},
			"players": daily_rows,
		}
		weekly_payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"window": {
				"label": "weekly_remaining",
				"start_date": weekly_start.strftime("%Y-%m-%d"),
				"end_date": weekly_end.strftime("%Y-%m-%d"),
				"days": weekly_days,
				"week_end_weekday": self.week_end_weekday,
			},
			"summary": {"players": len(weekly_rows)},
			"players": weekly_rows,
		}
		if not dry_run:
			write_json(daily_path, daily_payload)
			write_json(weekly_path, weekly_payload)
		return {
			"status": "ok",
			"daily_output_path": daily_path,
			"weekly_output_path": weekly_path,
			"summary": {
				"players_daily": len(daily_rows),
				"players_weekly": len(weekly_rows),
				"weekly_days": weekly_days,
			},
		}

	def _weekly_window(self, target_date):
		# Python weekday: Monday=0 ... Sunday=6.
		current_weekday = int(target_date.weekday())
		days_until_end = (self.week_end_weekday - current_weekday) % 7
		window_days = days_until_end + 1
		window_start = target_date
		window_end = target_date + timedelta(days=window_days - 1)
		return window_start, window_end, window_days

	def _build_window_rows(self, priors_by_player, blend_by_player, window_days, season_days):
		rows = []
		for player_id, prior in priors_by_player.items():
			blend = blend_by_player.get(player_id)
			if not blend:
				continue
			appearances = prior.get("projected_appearances")
			try:
				appearances = float(appearances)
			except Exception:
				appearances = None
			window_appearances = 0.0
			if appearances is not None and appearances > 0:
				window_appearances = (appearances / season_days) * float(window_days)

			blended_projection = float(blend.get("blended_projection", prior.get("prior_projection", 0.0)) or 0.0)
			per_app_projection = blended_projection / appearances if appearances and appearances > 0 else None
			window_projection = per_app_projection * window_appearances if per_app_projection is not None else (blended_projection / season_days) * float(window_days)

			role = prior.get("player_role", "unknown")
			row = {
				"player_id": str(player_id),
				"player_name": prior.get("player_name", f"UNKNOWN_{player_id}"),
				"player_role": role,
				"projected_appearances_window": round(window_appearances, 6),
				"projected_points_window": round(window_projection, 6),
				"blended_projection_season": round(blended_projection, 6),
				"performance_delta": blend.get("performance_delta"),
				"performance_delta_pct": blend.get("performance_delta_pct"),
				"performance_flag": blend.get("performance_flag", "insufficient_data"),
				"category_delta_pct": blend.get("category_delta_pct", {}),
				"category_performance_flags": blend.get("category_performance_flags", {}),
			}
			if role == "batters":
				a_rbi_per_app = prior.get("aRBI_per_app")
				a_sb_per_app = prior.get("aSB_per_app")
				if a_rbi_per_app is not None:
					row["aRBI_window"] = round(float(a_rbi_per_app) * window_appearances, 6)
				if a_sb_per_app is not None:
					row["aSB_window"] = round(float(a_sb_per_app) * window_appearances, 6)
			if role in {"sp", "rp"}:
				mgs_per_app = prior.get("MGS_per_app")
				vijay_per_app = prior.get("VIJAY_per_app")
				if mgs_per_app is not None:
					row["MGS_window"] = round(float(mgs_per_app) * window_appearances, 6)
				if vijay_per_app is not None:
					row["VIJAY_window"] = round(float(vijay_per_app) * window_appearances, 6)
			rows.append(row)
		rows.sort(key=lambda row: (row["player_role"], row["player_id"]))
		return rows
