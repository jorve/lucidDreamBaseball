from datetime import datetime, timedelta, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_free_agent_candidates_latest_path,
	get_ingestion_config,
	get_player_projection_weekly_latest_path,
	get_view_league_weekly_latest_path,
	get_weekly_digest_latest_path,
	get_weekly_email_payload_latest_path,
	get_weekly_email_text_latest_path,
)


UTC = timezone.utc


class WeeklyEmailError(RuntimeError):
	pass


class WeeklyEmailBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.projections_cfg = self.ingestion_cfg.get("projections", {})
		self.email_cfg = self.projections_cfg.get("weekly_email", {})
		self.week_end_weekday = int(self.projections_cfg.get("scoring_week_end_weekday", 6))
		if self.week_end_weekday < 0 or self.week_end_weekday > 6:
			self.week_end_weekday = 6
		self.top_players = int(self.email_cfg.get("top_players", 8))
		self.top_swaps = int(self.email_cfg.get("top_swaps", 5))
		self.send_day_weekday = int(self.email_cfg.get("send_day_weekday", 0))
		if self.send_day_weekday < 0 or self.send_day_weekday > 6:
			self.send_day_weekday = 0
		self.send_time_local = str(self.email_cfg.get("send_time_local", "08:00"))
		self.subject_template = str(self.email_cfg.get("subject_template", "LDB Weekly Recap + Outlook ({week_start} to {week_end})"))
		recipients = self.email_cfg.get("recipients", [])
		self.recipients = recipients if isinstance(recipients, list) else []

	def build(self, target_date, dry_run=False):
		output_path = get_weekly_email_payload_latest_path()
		text_path = get_weekly_email_text_latest_path()
		if not self.email_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "WEEKLY_EMAIL_DISABLED", "output_path": output_path, "text_output_path": text_path}

		league_weekly_path = get_view_league_weekly_latest_path()
		projection_weekly_path = get_player_projection_weekly_latest_path()
		digest_latest_path = get_weekly_digest_latest_path()
		if not league_weekly_path.exists() or not projection_weekly_path.exists() or not digest_latest_path.exists():
			return {"status": "skipped", "reason": "WEEKLY_EMAIL_INPUTS_MISSING", "output_path": output_path, "text_output_path": text_path}

		week_start, week_end = self._week_bounds(target_date)
		prev_start = week_start - timedelta(days=7)
		prev_end = week_end - timedelta(days=7)
		next_start = week_start + timedelta(days=7)
		next_end = week_end + timedelta(days=7)

		current_league = read_json(league_weekly_path)
		current_projection = read_json(projection_weekly_path)
		current_digest = read_json(digest_latest_path)
		prev_digest_payload, prev_available = self._load_previous_week_digest(prev_end)
		free_agents_path = get_free_agent_candidates_latest_path()
		free_agents_payload = read_json(free_agents_path) if free_agents_path.exists() else {}

		lookback = self._lookback_section(prev_digest_payload, prev_start, prev_end, prev_available)
		lookahead = self._lookahead_section(current_league, current_projection, free_agents_payload, next_start, next_end)
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"lookback_week": lookback,
			"lookahead_week": lookahead,
			"delivery_metadata": self._delivery_metadata(week_start, week_end),
			"source_snapshot": {
				"current_week_window": current_digest.get("window", {}),
				"previous_week_history_available": prev_available,
			},
		}
		text_value = self._render_text_email(payload)
		if not dry_run:
			write_json(output_path, payload)
			text_path.write_text(text_value, encoding="utf-8")
		return {"status": "ok", "output_path": output_path, "text_output_path": text_path}

	def _delivery_metadata(self, week_start, week_end):
		subject = self.subject_template.format(
			week_start=week_start.strftime("%Y-%m-%d"),
			week_end=week_end.strftime("%Y-%m-%d"),
		)
		send_dt = self._next_send_datetime(week_start)
		return {
			"subject": subject,
			"send_schedule": {
				"send_day_weekday": self.send_day_weekday,
				"send_time_local": self.send_time_local,
				"next_recommended_send_local": send_dt,
			},
			"recipients": list(self.recipients),
			"delivery_mode": "generation_only",
		}

	def _next_send_datetime(self, reference_date):
		reference_weekday = int(reference_date.weekday())
		days_until_send = (self.send_day_weekday - reference_weekday) % 7
		send_date = reference_date + timedelta(days=days_until_send)
		hour, minute = self._parse_hhmm(self.send_time_local)
		return f"{send_date.strftime('%Y-%m-%d')} {hour:02d}:{minute:02d}"

	def _parse_hhmm(self, value):
		try:
			parts = str(value).split(":")
			hour = int(parts[0])
			minute = int(parts[1]) if len(parts) > 1 else 0
		except Exception:
			return 8, 0
		if hour < 0 or hour > 23:
			hour = 8
		if minute < 0 or minute > 59:
			minute = 0
		return hour, minute

	def _week_bounds(self, target_date):
		target_day = target_date if isinstance(target_date, datetime) else datetime.combine(target_date, datetime.min.time())
		current_weekday = int(target_day.weekday())
		days_until_end = (self.week_end_weekday - current_weekday) % 7
		week_end = target_day + timedelta(days=days_until_end)
		week_start = week_end - timedelta(days=6)
		return week_start, week_end

	def _load_previous_week_digest(self, prev_end_date):
		history_path = get_weekly_digest_latest_path().parent / "history" / f"weekly_digest_{prev_end_date.strftime('%Y-%m-%d')}.json"
		if history_path.exists():
			return read_json(history_path), True
		return {}, False

	def _lookback_section(self, prev_digest_payload, prev_start, prev_end, available):
		if not available:
			return {
				"status": "insufficient_history",
				"window": {"start_date": prev_start.strftime("%Y-%m-%d"), "end_date": prev_end.strftime("%Y-%m-%d")},
				"summary": {"note": "Previous-week digest snapshot not found yet."},
				"top_overperformers": [],
				"top_underperformers": [],
			}
		return {
			"status": "ok",
			"window": {"start_date": prev_start.strftime("%Y-%m-%d"), "end_date": prev_end.strftime("%Y-%m-%d")},
			"summary": prev_digest_payload.get("summary", {}),
			"top_overperformers": list(prev_digest_payload.get("top_overperformers", []))[: self.top_players],
			"top_underperformers": list(prev_digest_payload.get("top_underperformers", []))[: self.top_players],
			"category_spotlight": prev_digest_payload.get("category_spotlight", {}),
		}

	def _lookahead_section(self, current_league, current_projection, free_agents_payload, next_start, next_end):
		current_window = current_projection.get("window", {})
		current_days = max(1.0, float(current_window.get("days", 7)))
		projection_rows = list(current_projection.get("players", []))
		scaled_rows = []
		for row in projection_rows:
			per_day = self._float_value(row.get("projected_points_window")) / current_days
			projected_next_week = per_day * 7.0
			scaled = dict(row)
			scaled["projected_points_next_week"] = round(projected_next_week, 6)
			scaled_rows.append(scaled)
		scaled_rows.sort(key=lambda row: self._float_value(row.get("projected_points_next_week")), reverse=True)
		replacement_rows = free_agents_payload.get("replacement_suggestions", {}).get("suggestions", [])
		return {
			"status": "ok",
			"window": {"start_date": next_start.strftime("%Y-%m-%d"), "end_date": next_end.strftime("%Y-%m-%d")},
			"summary": {
				"player_count": len(scaled_rows),
				"top_projection_points": self._float_value(scaled_rows[0].get("projected_points_next_week")) if scaled_rows else 0.0,
			},
			"projected_top_players": [self._projection_row(row) for row in scaled_rows[: self.top_players]],
			"projected_category_pulse": current_league.get("weekly_summary", {}).get("category_summary", {}),
			"recommended_swaps": list(replacement_rows)[: self.top_swaps],
		}

	def _projection_row(self, row):
		return {
			"player_id": str(row.get("player_id", "")),
			"player_name": row.get("player_name", ""),
			"player_role": row.get("player_role", "unknown"),
			"projected_points_next_week": round(self._float_value(row.get("projected_points_next_week")), 6),
			"performance_flag": row.get("performance_flag", "insufficient_data"),
		}

	def _render_text_email(self, payload):
		lookback = payload.get("lookback_week", {})
		lookahead = payload.get("lookahead_week", {})
		lines = [
			f"LDB Weekly Email ({payload.get('target_date')})",
			"",
			f"Previous Week ({lookback.get('window', {}).get('start_date', '?')} -> {lookback.get('window', {}).get('end_date', '?')})",
		]
		if lookback.get("status") != "ok":
			lines.append(f"- {lookback.get('summary', {}).get('note', 'Insufficient history.')}")
		else:
			lines.append(f"- Overperforming: {lookback.get('summary', {}).get('overperforming_count', 0)}")
			lines.append(f"- Underperforming: {lookback.get('summary', {}).get('underperforming_count', 0)}")
			lines.append("- Top overperformers:")
			for row in lookback.get("top_overperformers", [])[:5]:
				lines.append(self._player_line(row))
			lines.append("- Top underperformers:")
			for row in lookback.get("top_underperformers", [])[:5]:
				lines.append(self._player_line(row))
		lines.extend(
			[
				"",
				f"Upcoming Week ({lookahead.get('window', {}).get('start_date', '?')} -> {lookahead.get('window', {}).get('end_date', '?')})",
				"- Projected top players:",
			]
		)
		for row in lookahead.get("projected_top_players", [])[:5]:
			lines.append(f"- {row.get('player_name', '?')} ({row.get('player_role', 'unknown')}) {self._float_value(row.get('projected_points_next_week')):.2f}")
		lines.append("- Recommended swaps:")
		swaps = lookahead.get("recommended_swaps", [])
		if not swaps:
			lines.append("- No positive swaps identified.")
		for row in swaps[:5]:
			add_player = row.get("add_player", {})
			drop_player = row.get("drop_player", {})
			lines.append(
				f"- {row.get('team_name', 'TEAM')} add {add_player.get('player_name', '?')} / "
				f"drop {drop_player.get('player_name', '?')} "
				f"(net weekly {self._float_value(row.get('net_points_weekly')):.2f})"
			)
		return "\n".join(lines) + "\n"

	def _player_line(self, row):
		return (
			f"- {row.get('player_name', '?')} ({row.get('player_role', 'unknown')}) "
			f"delta={self._float_value(row.get('performance_delta')):.2f} "
			f"flag={row.get('performance_flag', 'insufficient_data')}"
		)

	def _float_value(self, value):
		try:
			return float(value)
		except Exception:
			return 0.0
