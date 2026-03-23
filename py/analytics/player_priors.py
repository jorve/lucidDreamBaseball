import csv
from datetime import datetime, timezone
from pathlib import Path

from analytics.io import write_json
from project_config import BASE_DIR, get_ingestion_config, get_preseason_player_priors_path


UTC = timezone.utc


class PlayerPriorError(RuntimeError):
	pass


class PlayerPriorBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.player_blend_cfg = self.ingestion_cfg.get("player_blend", {})

	def build(self, target_date, dry_run=False):
		output_path = get_preseason_player_priors_path()
		if not self.player_blend_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "PLAYER_BLEND_DISABLED", "output_path": output_path}

		players = []
		invalid_rows = 0
		rows_parsed = 0
		source_summary = {}

		season_year = target_date.year
		csv_sources = self._resolve_csv_sources(season_year)
		existing_sources = [(source_key, path_value) for source_key, path_value in csv_sources if path_value.exists()]
		if not existing_sources:
			return {"status": "skipped", "reason": "PRESEASON_CSVS_MISSING", "output_path": output_path}

		for source_key, csv_path in existing_sources:
			source_rows = 0
			source_invalid = 0
			with csv_path.open(newline="", encoding="utf-8-sig") as infile:
				reader = csv.DictReader(infile)
				for row in reader:
					rows_parsed += 1
					source_rows += 1
					player_id = self._first_non_empty(row, ["player_id", "id", "cbs_id"])
					if not player_id:
						player_id = self._first_non_empty(row, ["PlayerId", "MLBAMID"])
					if not player_id:
						invalid_rows += 1
						source_invalid += 1
						continue
					projection_raw = self._first_non_empty(
						row,
						[
							"projection",
							"projected_points",
							"proj_points",
							"preseason_points",
							"SPTS",
							"FPTS",
							"FPTS/G",
							"SPTS/G",
						],
					)
					try:
						projection = float(projection_raw)
					except Exception:
						invalid_rows += 1
						source_invalid += 1
						continue
					player_name = self._first_non_empty(row, ["player_name", "name", "fullname", "Name", "NameASCII"]) or f"UNKNOWN_{player_id}"
					player_row = {
						"player_id": str(player_id),
						"player_name": player_name,
						"prior_projection": projection,
						"player_role": source_key,
						"source_key": source_key,
						"mlbam_id": self._first_non_empty(row, ["MLBAMID", "mlbam_id"]),
						"projected_appearances": self._float_or_none(self._first_non_empty(row, ["G", "APP"])),
						"projection_vol": self._float_or_none(self._first_non_empty(row, ["Vol"])),
					}
					player_row.update(self._derived_scoring_fields(source_key, row, player_row.get("projected_appearances")))
					players.append(player_row)
			source_summary[source_key] = {
				"csv_path": str(csv_path),
				"rows_parsed": source_rows,
				"invalid_rows": source_invalid,
				"players_loaded": source_rows - source_invalid,
			}

		players.sort(key=lambda player: (player["player_id"], player.get("player_role", "")))
		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"season_year": season_year,
			"source_csvs": {source_key: str(path_value) for source_key, path_value in existing_sources},
			"summary": {
				"rows_parsed": rows_parsed,
				"players_loaded": len(players),
				"invalid_rows": invalid_rows,
				"sources_loaded": len(existing_sources),
				"source_summary": source_summary,
			},
			"players": players,
		}
		if not dry_run:
			write_json(output_path, payload)
		return {"status": "ok", "output_path": output_path, "summary": payload["summary"]}

	def _resolve_csv_sources(self, season_year):
		configured_sources = self.player_blend_cfg.get("preseason_csvs", {})
		ordered_roles = ["batters", "sp", "rp"]
		sources = []
		if isinstance(configured_sources, dict):
			for role in ordered_roles:
				config_path = configured_sources.get(role)
				if not config_path:
					continue
				sources.append((role, self._resolve_path(config_path, season_year)))
		if not sources:
			# Backward-compatible single-file fallback.
			config_path = self.player_blend_cfg.get("preseason_csv", "data/{year}/preseason/player_priors.csv")
			sources.append(("all", self._resolve_path(config_path, season_year)))
		return sources

	def _resolve_path(self, config_path, year):
		resolved = str(config_path).format(year=year, current_year=year)
		path_value = Path(resolved)
		if not path_value.is_absolute():
			path_value = BASE_DIR / path_value
		return path_value

	def _first_non_empty(self, row, keys):
		for key in keys:
			value = row.get(key)
			if value is not None and str(value).strip():
				return str(value).strip()
		return None

	def _float_or_none(self, value):
		if value is None:
			return None
		try:
			return float(value)
		except Exception:
			return None

	def _derived_scoring_fields(self, source_key, row, projected_appearances):
		if source_key == "batters":
			rbi = self._float_or_none(self._first_non_empty(row, ["RBI"]))
			gidp = self._float_or_none(self._first_non_empty(row, ["GDP", "GIDP"]))
			sb = self._float_or_none(self._first_non_empty(row, ["SB"]))
			cs = self._float_or_none(self._first_non_empty(row, ["CS"]))
			if None in (rbi, gidp, sb, cs):
				return {}
			appearance_base = projected_appearances if projected_appearances and projected_appearances > 0 else None
			a_rbi = rbi - gidp
			a_sb = sb - (0.5 * cs)
			return {
				"aRBI_total": round(a_rbi, 4),
				"aSB_total": round(a_sb, 4),
				"aRBI_per_app": None if appearance_base is None else round(a_rbi / appearance_base, 6),
				"aSB_per_app": None if appearance_base is None else round(a_sb / appearance_base, 6),
			}

		ip = self._float_or_none(self._first_non_empty(row, ["IP", "INNs"]))
		g = projected_appearances if projected_appearances is not None else self._float_or_none(self._first_non_empty(row, ["G", "APP"]))
		gs = self._float_or_none(self._first_non_empty(row, ["GS"]))
		so = self._float_or_none(self._first_non_empty(row, ["SO", "K"]))
		bb = self._float_or_none(self._first_non_empty(row, ["BB"]))
		h = self._float_or_none(self._first_non_empty(row, ["H"]))
		r = self._float_or_none(self._first_non_empty(row, ["R"]))
		hr = self._float_or_none(self._first_non_empty(row, ["HR"]))
		sv = self._float_or_none(self._first_non_empty(row, ["SV", "S"])) or 0.0
		hold = self._float_or_none(self._first_non_empty(row, ["HLD", "HD"])) or 0.0
		bs = self._float_or_none(self._first_non_empty(row, ["BS"])) or 0.0
		rl = self._float_or_none(self._first_non_empty(row, ["L"])) or 0.0
		if None in (ip, g, so, bb, h, r, hr) or g <= 0:
			return {}

		outs_per_app = (3.0 * ip) / g
		mgs_per_app = 40.0 + (2.0 * outs_per_app) + (1.0 * (so / g)) - (2.0 * (bb / g)) - (2.0 * (h / g)) - (3.0 * (r / g)) - (6.0 * (hr / g))

		gs = gs or 0.0
		inn_per_gs = (ip / gs) if gs > 0 else 0.0
		relief_innings = ip - (inn_per_gs * gs)
		vijay_total = (((relief_innings) + (3.0 * sv) + (3.0 * hold)) / 4.0) - ((bs + rl) * 2.0)
		relief_apps = max((g - gs), 1.0)
		return {
			"MGS_per_app": round(mgs_per_app, 6),
			"VIJAY_per_app": round(vijay_total / relief_apps, 6),
		}
