from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_free_agent_candidates_latest_path,
	get_view_gm_weekly_latest_path,
	get_view_league_weekly_latest_path,
	get_weekly_digest_latest_path,
	get_weekly_digest_latest_text_path,
)


UTC = timezone.utc


class WeeklyDigestError(RuntimeError):
	pass


class WeeklyDigestBuilder:
	def build(self, target_date, dry_run=False):
		output_path = get_weekly_digest_latest_path()
		text_path = get_weekly_digest_latest_text_path()
		league_path = get_view_league_weekly_latest_path()
		gm_path = get_view_gm_weekly_latest_path()
		free_agents_path = get_free_agent_candidates_latest_path()

		if not league_path.exists() or not gm_path.exists():
			return {
				"status": "skipped",
				"reason": "WEEKLY_VIEW_ARTIFACTS_MISSING",
				"output_path": output_path,
				"text_output_path": text_path,
			}

		league_payload = read_json(league_path)
		gm_payload = read_json(gm_path)
		free_agents_payload = read_json(free_agents_path) if free_agents_path.exists() else {}

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		weekly_summary = league_payload.get("weekly_summary", {})
		overall_counts = weekly_summary.get("overall_performance_counts", {})
		category_summary = weekly_summary.get("category_summary", {})
		replacement_summary = free_agents_payload.get("replacement_suggestions", {})
		replacement_rows = replacement_summary.get("suggestions", []) if isinstance(replacement_summary, dict) else []
		top_swaps = replacement_rows[:5]

		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"window": league_payload.get("window", {}),
			"summary": {
				"league_players": int(league_payload.get("summary", {}).get("player_count", 0) or 0),
				"gm_players": int(gm_payload.get("summary", {}).get("player_count", 0) or 0),
				"overperforming_count": int(overall_counts.get("overperforming", 0) or 0),
				"underperforming_count": int(overall_counts.get("underperforming", 0) or 0),
				"replacement_candidates": len(replacement_rows),
			},
			"top_overperformers": list(league_payload.get("leaders", {}).get("overperformers", []))[:8],
			"top_underperformers": list(league_payload.get("leaders", {}).get("underperformers", []))[:8],
			"category_spotlight": self._category_spotlight(category_summary),
			"recommended_swaps": top_swaps,
		}
		text_digest = self._render_text_digest(payload)
		history_json_path = self._history_json_path(payload)
		history_text_path = self._history_text_path(payload)
		if not dry_run:
			write_json(output_path, payload)
			text_path.write_text(text_digest, encoding="utf-8")
			write_json(history_json_path, payload)
			history_text_path.parent.mkdir(parents=True, exist_ok=True)
			history_text_path.write_text(text_digest, encoding="utf-8")
		return {
			"status": "ok",
			"output_path": output_path,
			"text_output_path": text_path,
			"history_output_path": history_json_path,
			"summary": payload["summary"],
		}

	def _category_spotlight(self, category_summary):
		spotlight = {}
		for category in ("aRBI", "aSB", "MGS", "VIJAY"):
			summary = category_summary.get(category, {}) if isinstance(category_summary, dict) else {}
			spotlight[category] = {
				"overperforming_count": int(summary.get("overperforming_count", 0) or 0),
				"underperforming_count": int(summary.get("underperforming_count", 0) or 0),
				"top_overperformer": self._first_or_none(summary.get("top_overperformers", [])),
				"top_underperformer": self._first_or_none(summary.get("top_underperformers", [])),
			}
		return spotlight

	def _first_or_none(self, rows):
		if isinstance(rows, list) and rows:
			return rows[0]
		return None

	def _render_text_digest(self, payload):
		window = payload.get("window", {})
		summary = payload.get("summary", {})
		lines = [
			f"Weekly Digest ({payload.get('target_date')})",
			f"Window: {window.get('start_date', '?')} -> {window.get('end_date', '?')} ({window.get('days', '?')} days)",
			"",
			"League Pulse",
			f"- Overperforming: {summary.get('overperforming_count', 0)}",
			f"- Underperforming: {summary.get('underperforming_count', 0)}",
			f"- Replacement opportunities: {summary.get('replacement_candidates', 0)}",
			"",
			"Top Overperformers",
		]
		for row in payload.get("top_overperformers", [])[:5]:
			lines.append(self._player_line(row))
		lines.append("")
		lines.append("Top Underperformers")
		for row in payload.get("top_underperformers", [])[:5]:
			lines.append(self._player_line(row))
		lines.append("")
		lines.append("Category Spotlight")
		for category, snapshot in payload.get("category_spotlight", {}).items():
			over = snapshot.get("top_overperformer")
			under = snapshot.get("top_underperformer")
			lines.append(
				f"- {category}: over={snapshot.get('overperforming_count', 0)} under={snapshot.get('underperforming_count', 0)}; "
				f"top_over={self._player_short(over)}; top_under={self._player_short(under)}"
			)
		lines.append("")
		lines.append("Recommended Swaps")
		swaps = payload.get("recommended_swaps", [])
		if not swaps:
			lines.append("- No positive swaps identified.")
		for row in swaps[:5]:
			add_player = row.get("add_player", {})
			drop_player = row.get("drop_player", {})
			lines.append(
				f"- {row.get('team_name', 'TEAM')} add {add_player.get('player_name', '?')} / "
				f"drop {drop_player.get('player_name', '?')} "
				f"(net weekly {row.get('net_points_weekly', 0):.2f})"
			)
		return "\n".join(lines) + "\n"

	def _player_line(self, row):
		return (
			f"- {row.get('player_name', '?')} ({row.get('player_role', 'unknown')}) "
			f"delta={self._fmt_float(row.get('performance_delta'))} "
			f"flag={row.get('performance_flag', 'insufficient_data')}"
		)

	def _player_short(self, row):
		if not isinstance(row, dict):
			return "n/a"
		return str(row.get("player_name", "n/a"))

	def _fmt_float(self, value):
		try:
			return f"{float(value):.2f}"
		except Exception:
			return "n/a"

	def _history_json_path(self, payload):
		window = payload.get("window", {})
		week_end = window.get("end_date") or payload.get("target_date")
		return get_weekly_digest_latest_path().parent / "history" / f"weekly_digest_{week_end}.json"

	def _history_text_path(self, payload):
		window = payload.get("window", {})
		week_end = window.get("end_date") or payload.get("target_date")
		return get_weekly_digest_latest_text_path().parent / "history" / f"weekly_digest_{week_end}.txt"
