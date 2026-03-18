import csv
import io
import re
from datetime import datetime, timezone
from pathlib import Path

from analytics.io import read_json, write_json
from project_config import (
	BASE_DIR,
	get_ingestion_config,
	get_player_eligibility_changes_latest_path,
	get_player_eligibility_latest_path,
)


UTC = timezone.utc
POSITION_TOKEN_RE = re.compile(r"^[A-Z0-9]+$")


class PlayerEligibilityError(RuntimeError):
	pass


class PlayerEligibilityBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.elig_cfg = self.ingestion_cfg.get("eligibility", {})

	def build(self, target_date, dry_run=False):
		output_path = get_player_eligibility_latest_path()
		changes_path = get_player_eligibility_changes_latest_path()
		if not self.elig_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "ELIGIBILITY_DISABLED", "output_path": output_path, "changes_path": changes_path}

		season_year = target_date.year
		source_paths = self._resolve_csv_sources(season_year)
		existing_sources = [(role, path_value) for role, path_value in source_paths if path_value.exists()]
		if not existing_sources:
			return {"status": "skipped", "reason": "ELIGIBILITY_CSVS_MISSING", "output_path": output_path, "changes_path": changes_path}

		previous = read_json(output_path) if output_path.exists() else {}
		previous_players = {row["player_key"]: row for row in previous.get("players", []) if row.get("player_key")}

		players_by_key = {}
		source_summary = {}
		for role, csv_path in existing_sources:
			rows, source_invalid = self._read_source_rows(csv_path)
			source_loaded = 0
			for row in rows:
				parsed = self._parse_player_field(row.get("Player", ""))
				if not parsed["name"]:
					source_invalid += 1
					continue
				player_key = self._build_player_key(parsed["name"], parsed["team_abbr"])
				entry = players_by_key.setdefault(
					player_key,
					{
						"player_key": player_key,
						"display_name": parsed["name"],
						"team_abbr": parsed["team_abbr"],
						"roles": set(),
						"native_positions": set(),
						"source_refs": [],
					},
				)
				entry["display_name"] = parsed["name"]
				if parsed["team_abbr"]:
					entry["team_abbr"] = parsed["team_abbr"]
				entry["roles"].add(role)
				for position in parsed["positions"]:
					entry["native_positions"].add(position)
				entry["source_refs"].append({"role": role, "csv_path": str(csv_path)})
				source_loaded += 1
			source_summary[role] = {
				"csv_path": str(csv_path),
				"rows_parsed": len(rows),
				"rows_loaded": source_loaded,
				"rows_invalid": source_invalid,
			}

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		players = []
		for player_key in sorted(players_by_key.keys()):
			row = players_by_key[player_key]
			native_positions = sorted(row["native_positions"])
			slot_positions = self._derive_slot_positions(set(native_positions), set(row["roles"]))
			players.append(
				{
					"player_key": player_key,
					"display_name": row["display_name"],
					"team_abbr": row.get("team_abbr"),
					"roles": sorted(row["roles"]),
					"native_positions": native_positions,
					"slot_positions": slot_positions,
					"source_refs": row["source_refs"],
				}
			)

		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"season_year": season_year,
			"source_csvs": {role: str(path_value) for role, path_value in existing_sources},
			"summary": {
				"players_loaded": len(players),
				"sources_loaded": len(existing_sources),
				"source_summary": source_summary,
			},
			"players": players,
		}
		changes_payload = self._build_changes_payload(previous_players, {row["player_key"]: row for row in players}, now_utc, target_date)

		if not dry_run:
			write_json(output_path, payload)
			write_json(changes_path, changes_payload)
		return {
			"status": "ok",
			"output_path": output_path,
			"changes_path": changes_path,
			"summary": payload["summary"],
			"changes_summary": changes_payload["summary"],
		}

	def _resolve_csv_sources(self, season_year):
		csv_cfg = self.elig_cfg.get("csvs", {})
		sources = []
		for role in ("batters", "sp", "rp"):
			path_value = csv_cfg.get(role)
			if not path_value:
				continue
			sources.append((role, self._resolve_path(path_value, season_year)))
		return sources

	def _resolve_path(self, config_path, season_year):
		resolved = str(config_path).format(year=season_year, current_year=season_year)
		path_value = Path(resolved)
		if not path_value.is_absolute():
			path_value = BASE_DIR / path_value
		return path_value

	def _read_source_rows(self, csv_path):
		text = csv_path.read_text(encoding="utf-8-sig")
		lines = text.splitlines()
		if len(lines) < 2:
			return [], 0
		data_text = "\n".join(lines[1:])
		reader = csv.DictReader(io.StringIO(data_text))
		rows = []
		invalid = 0
		for row in reader:
			if not isinstance(row, dict):
				invalid += 1
				continue
			rows.append(row)
		return rows, invalid

	def _parse_player_field(self, value):
		text = str(value or "").strip()
		if not text:
			return {"name": None, "positions": [], "team_abbr": None}
		parts = text.split("|", 1)
		left = parts[0].strip()
		team = parts[1].strip() if len(parts) > 1 else None
		team_abbr = team.split()[0] if team else None

		name = left
		positions = []
		if " " in left:
			maybe_name, maybe_positions = left.rsplit(" ", 1)
			tokens = [token.strip().upper() for token in maybe_positions.split(",") if token.strip()]
			if tokens and all(POSITION_TOKEN_RE.match(token) for token in tokens):
				name = maybe_name.strip()
				positions = tokens
		return {"name": name, "positions": sorted(set(positions)), "team_abbr": team_abbr}

	def _build_player_key(self, name, team_abbr):
		name_key = re.sub(r"[^a-z0-9]+", "", str(name).lower())
		team_key = re.sub(r"[^a-z0-9]+", "", str(team_abbr or "").lower())
		return f"{name_key}|{team_key}" if team_key else name_key

	def _derive_slot_positions(self, native_positions, roles):
		slots = set()
		if "batters" in roles:
			slots.add("U")
			for position in ("C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "OF"):
				if position in native_positions:
					slots.add(position)
			if any(position in native_positions for position in ("LF", "CF", "RF", "OF")):
				slots.add("OF")
		if "sp" in roles or "SP" in native_positions:
			slots.add("SP")
		if "rp" in roles or "RP" in native_positions:
			slots.add("RP")
		return sorted(slots)

	def _build_changes_payload(self, previous_players, current_players, now_utc, target_date):
		added = []
		removed = []
		updated = []
		for player_key, row in current_players.items():
			previous = previous_players.get(player_key)
			if previous is None:
				added.append({"player_key": player_key, "display_name": row.get("display_name"), "native_positions": row.get("native_positions", []), "slot_positions": row.get("slot_positions", [])})
				continue
			prev_native = sorted(previous.get("native_positions", []))
			cur_native = sorted(row.get("native_positions", []))
			prev_slot = sorted(previous.get("slot_positions", []))
			cur_slot = sorted(row.get("slot_positions", []))
			if prev_native != cur_native or prev_slot != cur_slot:
				updated.append(
					{
						"player_key": player_key,
						"display_name": row.get("display_name"),
						"native_positions_before": prev_native,
						"native_positions_after": cur_native,
						"slot_positions_before": prev_slot,
						"slot_positions_after": cur_slot,
					}
				)
		for player_key, previous in previous_players.items():
			if player_key not in current_players:
				removed.append({"player_key": player_key, "display_name": previous.get("display_name")})

		return {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"summary": {
				"added_count": len(added),
				"removed_count": len(removed),
				"updated_count": len(updated),
			},
			"added": added,
			"removed": removed,
			"updated": updated,
		}
