from __future__ import annotations

import re
from datetime import datetime, timezone

from analytics.io import read_json, write_json
from analytics.validators import ValidationError, validate_player_registry_payload
from project_config import (
	get_ingestion_raw_dir,
	get_ingestion_config,
	get_player_eligibility_latest_path,
	get_preseason_player_priors_path,
	get_player_registry_latest_path,
	get_rosters_latest_path,
)


UTC = timezone.utc
WS_RE = re.compile(r"\s+")


class PlayerRegistryError(RuntimeError):
	pass


def _norm_name(value: str) -> str:
	if value is None:
		return ""
	value = str(value).strip().lower()
	value = WS_RE.sub(" ", value)
	return value


class PlayerRegistryBuilder:
	"""
	Central player registry keyed by CBS Fantasy player id.

	Primary goals:
	- canonicalize CBS roster universe (all teams + scout pool when present)
	- provide a durable crosswalk to projection/prior space (`player_id`) when possible
	- surface disambiguation failures as an explicit review queue
	"""

	def __init__(self, league_id: str = "luciddreambaseball"):
		self.league_id = league_id
		self.ingestion_cfg = get_ingestion_config()
		self.registry_cfg = (self.ingestion_cfg.get("player_registry", {}) if isinstance(self.ingestion_cfg, dict) else {}) or {}
		self.max_review_items = int(self.registry_cfg.get("max_review_items", 250))

	def build(self, target_date, dry_run: bool = False):
		output_path = get_player_registry_latest_path()
		rosters_path = get_rosters_latest_path()
		priors_path = get_preseason_player_priors_path()
		elig_path = get_player_eligibility_latest_path()

		if not rosters_path.exists():
			return {"status": "skipped", "reason": "ROSTERS_LATEST_MISSING", "output_path": output_path}
		rosters_payload = read_json(rosters_path)

		priors_players = read_json(priors_path).get("players", []) if priors_path.exists() else []
		elig_players = read_json(elig_path).get("players", []) if elig_path.exists() else []

		live_scoring_payload = self._try_load_live_scoring(target_date)

		registry = self._build_registry(
			target_date=target_date,
			rosters_payload=rosters_payload,
			live_scoring_payload=live_scoring_payload,
			priors_players=priors_players,
			elig_players=elig_players,
		)
		try:
			validate_player_registry_payload(registry)
		except ValidationError as error:
			raise PlayerRegistryError(f"Invalid player registry payload: {error}")

		if not dry_run:
			write_json(output_path, registry)
		return {"status": "ok", "output_path": output_path, "summary": registry.get("summary", {})}

	def _try_load_live_scoring(self, target_date):
		try:
			raw_dir = get_ingestion_raw_dir(target_date)
			path_value = raw_dir / f"live_scoring_{target_date.strftime('%Y-%m-%d')}.json"
			if not path_value.exists():
				return None
			return read_json(path_value)
		except Exception:
			return None

	def _build_registry(self, target_date, rosters_payload, live_scoring_payload, priors_players, elig_players):
		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")

		priors_by_cbs = {}
		priors_by_name = {}
		for prior in priors_players or []:
			pid = str(prior.get("player_id") or "").strip()
			if not pid or pid == "None":
				continue
			cbs = str(prior.get("cbs_player_id") or "").strip()
			if cbs and cbs != "None":
				priors_by_cbs.setdefault(cbs, []).append(prior)
			name = _norm_name(prior.get("player_name") or "")
			if name:
				priors_by_name.setdefault(name, []).append(prior)

		elig_by_key = {}
		for row in elig_players or []:
			key = row.get("player_key")
			if key:
				elig_by_key[str(key)] = row

		players = {}
		teams = []
		body = (rosters_payload or {}).get("body", {})
		teams_raw = ((body.get("rosters") or {}).get("teams") or []) if isinstance(body, dict) else []
		for team in teams_raw:
			team_id = str(team.get("id", ""))
			team_name = team.get("name") or team.get("long_abbr") or f"TEAM_{team_id}"
			team_abbr = team.get("long_abbr") or team.get("abbr") or None
			teams.append({"team_id": team_id, "team_name": team_name, "team_abbr": team_abbr})
			for p in team.get("players", []) or []:
				cbs_id = str(p.get("id") or "").strip()
				if not cbs_id or cbs_id == "None":
					continue
				entry = players.setdefault(
					cbs_id,
					{
						"cbs_player_id": cbs_id,
						"full_name": None,
						"first_name": None,
						"last_name": None,
						"pro_team": None,
						"position": None,
						"eligible_positions_display": None,
						"owned_by_team_id": None,
						"owned_by_team_name": None,
						"owned_by_team_abbr": None,
						"roster_status": None,
						"roster_pos": None,
						"photo": None,
						"profile_url": None,
						"elias_id": None,
						"source_refs": [],
						"crosswalk": {
							"prior_player_id": None,
							"mlbam_id": None,
							"match_method": None,
							"match_quality": "unmatched",
						},
						"eligibility": {
							"player_key": None,
							"slot_positions": [],
							"native_positions": [],
						},
					},
				)

				entry["full_name"] = entry["full_name"] or p.get("fullname")
				entry["first_name"] = entry["first_name"] or p.get("firstname")
				entry["last_name"] = entry["last_name"] or p.get("lastname")
				entry["pro_team"] = entry["pro_team"] or p.get("pro_team")
				entry["position"] = entry["position"] or p.get("position")
				entry["eligible_positions_display"] = entry["eligible_positions_display"] or p.get("eligible_positions_display") or p.get("eligible")
				entry["owned_by_team_id"] = entry["owned_by_team_id"] or p.get("owned_by_team_id") or team_id
				entry["owned_by_team_name"] = entry["owned_by_team_name"] or team_name
				entry["owned_by_team_abbr"] = entry["owned_by_team_abbr"] or team_abbr
				entry["roster_status"] = entry["roster_status"] or p.get("roster_status") or p.get("pro_status")
				entry["roster_pos"] = entry["roster_pos"] or p.get("roster_pos")
				entry["photo"] = entry["photo"] or p.get("photo")
				entry["elias_id"] = entry["elias_id"] or p.get("elias_id")
				profile_link = p.get("profile_link")
				if profile_link and isinstance(profile_link, str) and "href=" in profile_link and entry["profile_url"] is None:
					# Keep it simple: embed the HTML string; UI can extract if desired.
					entry["profile_url"] = profile_link

				entry["source_refs"].append({"source": "rosters_latest", "team_id": team_id, "team_name": team_name})

		# Optional enrichment from live_scoring (primarily roster_pos + status fields that may differ)
		if isinstance(live_scoring_payload, dict):
			live_teams = (((live_scoring_payload.get("body") or {}).get("live_scoring") or {}).get("teams") or [])
			for t in live_teams:
				for p in t.get("players", []) or []:
					cbs_id = str(p.get("id") or "").strip()
					if not cbs_id or cbs_id == "None":
						continue
					entry = players.get(cbs_id)
					if not entry:
						continue
					entry["full_name"] = entry["full_name"] or p.get("fullname")
					entry["pro_team"] = entry["pro_team"] or p.get("pro_team")
					entry["position"] = entry["position"] or p.get("position")
					entry["eligible_positions_display"] = entry["eligible_positions_display"] or p.get("eligible_positions_display")
					entry["roster_pos"] = entry["roster_pos"] or p.get("roster_pos")
					entry["roster_status"] = entry["roster_status"] or p.get("pro_status")
					entry["source_refs"].append({"source": "live_scoring"})

		# Crosswalk to priors: prefer explicit `cbs_player_id`, else *unique name* match only.
		review_queue = []
		matched_via_cbs = 0
		matched_via_unique_name = 0
		ambiguous_name = 0
		unmatched = 0

		for cbs_id, entry in players.items():
			prior_candidates = priors_by_cbs.get(cbs_id) or []
			if len(prior_candidates) == 1:
				pr = prior_candidates[0]
				entry["crosswalk"] = {
					"prior_player_id": str(pr.get("player_id")),
					"mlbam_id": pr.get("mlbam_id"),
					"match_method": "cbs_player_id",
					"match_quality": "exact",
				}
				matched_via_cbs += 1
			elif len(prior_candidates) > 1:
				# This *shouldn't* happen; surface for review.
				entry["crosswalk"]["match_quality"] = "ambiguous"
				entry["crosswalk"]["match_method"] = "cbs_player_id"
				ambiguous_name += 1
				if len(review_queue) < self.max_review_items:
					review_queue.append(
						{
							"kind": "ambiguous_cbs_player_id",
							"cbs_player_id": cbs_id,
							"player_name": entry.get("full_name") or "",
							"prior_player_ids": [str(p.get("player_id")) for p in prior_candidates[:10]],
						}
					)
			else:
				name_key = _norm_name(entry.get("full_name") or "")
				name_candidates = priors_by_name.get(name_key) or []
				unique = []
				seen = set()
				for p in name_candidates:
					pid = str(p.get("player_id") or "")
					if pid and pid not in seen:
						seen.add(pid)
						unique.append(p)
				if len(unique) == 1:
					pr = unique[0]
					entry["crosswalk"] = {
						"prior_player_id": str(pr.get("player_id")),
						"mlbam_id": pr.get("mlbam_id"),
						"match_method": "unique_name",
						"match_quality": "weak",
					}
					matched_via_unique_name += 1
				elif len(unique) > 1:
					entry["crosswalk"]["match_quality"] = "ambiguous"
					entry["crosswalk"]["match_method"] = "name"
					ambiguous_name += 1
					if len(review_queue) < self.max_review_items:
						review_queue.append(
							{
								"kind": "ambiguous_name",
								"cbs_player_id": cbs_id,
								"player_name": entry.get("full_name") or "",
								"prior_player_ids": [str(p.get("player_id")) for p in unique[:10]],
							}
						)
				else:
					unmatched += 1
					if len(review_queue) < self.max_review_items:
						review_queue.append(
							{
								"kind": "unmatched",
								"cbs_player_id": cbs_id,
								"player_name": entry.get("full_name") or "",
							}
						)

			# Eligibility attachment (name+team key)
			name_key = _norm_name(entry.get("full_name") or "")
			team_key = str(entry.get("pro_team") or "").strip().lower()
			if name_key and team_key:
				player_key = f"{name_key.replace(' ', '')}|{team_key}"
				elig = elig_by_key.get(player_key)
				if elig:
					entry["eligibility"] = {
						"player_key": player_key,
						"slot_positions": list(elig.get("slot_positions", []) or []),
						"native_positions": list(elig.get("native_positions", []) or []),
					}

		players_list = list(players.values())
		players_list.sort(key=lambda row: (row.get("owned_by_team_id") or "", row.get("full_name") or "", row["cbs_player_id"]))

		return {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"source": {
				"provider": "cbssports",
				"league_id": self.league_id,
			},
			"inputs": {
				"rosters_latest": str(get_rosters_latest_path()),
				"preseason_player_priors": str(get_preseason_player_priors_path()),
				"player_eligibility": str(get_player_eligibility_latest_path()),
				"live_scoring_included": bool(live_scoring_payload),
			},
			"summary": {
				"players_total": len(players_list),
				"matched_via_cbs_player_id": matched_via_cbs,
				"matched_via_unique_name": matched_via_unique_name,
				"ambiguous": ambiguous_name,
				"unmatched": unmatched,
				"review_queue_count": len(review_queue),
			},
			"teams": teams,
			"players": players_list,
			"review_queue": review_queue,
		}

