from collections import defaultdict
from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_free_agent_candidates_latest_path,
	get_ingestion_config,
	get_ingestion_raw_dir,
	get_player_eligibility_latest_path,
	get_player_projection_daily_latest_path,
	get_player_projection_weekly_latest_path,
	get_preseason_player_priors_path,
	get_roster_state_latest_path,
)


UTC = timezone.utc


class FreeAgentCandidatesError(RuntimeError):
	pass


class FreeAgentCandidatesBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.projections_cfg = self.ingestion_cfg.get("projections", {})
		self.free_agents_cfg = self.projections_cfg.get("free_agents", {})
		self.daily_weight = float(self.free_agents_cfg.get("daily_weight", 0.3))
		self.weekly_weight = float(self.free_agents_cfg.get("weekly_weight", 0.7))
		if self.daily_weight < 0:
			self.daily_weight = 0.0
		if self.weekly_weight < 0:
			self.weekly_weight = 0.0
		weight_total = self.daily_weight + self.weekly_weight
		if weight_total <= 0:
			self.daily_weight = 0.3
			self.weekly_weight = 0.7
		else:
			self.daily_weight = self.daily_weight / weight_total
			self.weekly_weight = self.weekly_weight / weight_total
		self.max_candidates = int(self.free_agents_cfg.get("max_candidates", 250))
		self.drop_pool_size = int(self.free_agents_cfg.get("drop_pool_size", 8))
		self.max_replacement_suggestions = int(self.free_agents_cfg.get("max_replacement_suggestions", 120))
		self.min_net_gain = float(self.free_agents_cfg.get("min_net_gain", 0.0))

	def build(self, target_date, dry_run=False):
		output_path = get_free_agent_candidates_latest_path()
		if not self.free_agents_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "FREE_AGENT_CANDIDATES_DISABLED", "output_path": output_path}

		priors_path = get_preseason_player_priors_path()
		daily_path = get_player_projection_daily_latest_path()
		weekly_path = get_player_projection_weekly_latest_path()
		if not priors_path.exists() or not daily_path.exists() or not weekly_path.exists():
			return {"status": "skipped", "reason": "FREE_AGENT_INPUTS_MISSING", "output_path": output_path}

		priors_players = read_json(priors_path).get("players", [])
		daily_players = read_json(daily_path).get("players", [])
		weekly_players = read_json(weekly_path).get("players", [])
		if not priors_players:
			return {"status": "skipped", "reason": "FREE_AGENT_UNIVERSE_EMPTY", "output_path": output_path}

		rostered_ids, assignment_meta, roster_teams = self._load_rostered_player_ids(target_date)
		rostered_ids, merge_meta = self._merge_prior_space_rostered_ids(rostered_ids, roster_teams, priors_players)
		if merge_meta:
			assignment_meta = {**assignment_meta, **merge_meta}
		eligibility_map = self._load_eligibility_map()
		daily_map = {str(row.get("player_id")): row for row in daily_players}
		weekly_map = {str(row.get("player_id")): row for row in weekly_players}
		priors_map = {str(row.get("player_id")): row for row in priors_players}

		rows = []
		for prior in priors_players:
			player_id = str(prior.get("player_id"))
			if not player_id or player_id == "None":
				continue
			# League-wide: anyone on any team roster is not a free agent. rostered_ids holds
			# CBS Fantasy ids plus prior-space ids merged from them; also treat a prior row
			# whose cbs_player_id is still on a roster as rostered (defense in depth).
			if self._prior_is_rostered(rostered_ids, prior):
				continue
			weekly_row = weekly_map.get(player_id, {})
			daily_row = daily_map.get(player_id, {})
			weekly_points = self._to_float(weekly_row.get("projected_points_window"))
			daily_points = self._to_float(daily_row.get("projected_points_window"))
			composite_score = (self.weekly_weight * weekly_points) + (self.daily_weight * daily_points)
			rows.append(
				{
					"player_id": player_id,
					"player_name": prior.get("player_name", f"UNKNOWN_{player_id}"),
					"player_role": prior.get("player_role", "unknown"),
					"projected_points_daily": round(daily_points, 6),
					"projected_points_weekly": round(weekly_points, 6),
					"composite_score": round(composite_score, 6),
					"performance_delta": weekly_row.get("performance_delta"),
					"performance_flag": weekly_row.get("performance_flag", "insufficient_data"),
					"slot_positions": eligibility_map.get(player_id, []),
				}
			)
		rows.sort(key=lambda row: (row["composite_score"], row["projected_points_weekly"]), reverse=True)
		if self.max_candidates > 0:
			rows = rows[: self.max_candidates]
		replacement_suggestions = self._build_replacement_suggestions(
			fa_candidates=rows,
			roster_teams=roster_teams,
			eligibility_map=eligibility_map,
			daily_map=daily_map,
			weekly_map=weekly_map,
			priors_map=priors_map,
		)

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"assignment_snapshot": assignment_meta,
			"scoring": {
				"daily_weight": round(self.daily_weight, 6),
				"weekly_weight": round(self.weekly_weight, 6),
			},
			"summary": {
				"candidate_count": len(rows),
				"rostered_player_count": len(rostered_ids),
				"universe_player_count": len(priors_players),
			},
			"candidates": rows,
			"replacement_suggestions": replacement_suggestions,
		}
		if not dry_run:
			write_json(output_path, payload)
		return {
			"status": "ok",
			"output_path": output_path,
			"summary": payload["summary"],
		}

	def _load_rostered_player_ids(self, target_date):
		roster_state_path = get_roster_state_latest_path()
		if roster_state_path.exists():
			payload = read_json(roster_state_path)
			teams = payload.get("teams", [])
			player_ids = set()
			for team in teams:
				for player in team.get("players", []):
					player_id = player.get("player_id")
					if player_id is not None:
						player_ids.add(str(player_id))
			return player_ids, {
				"source": "roster_state_latest",
				"as_of_utc": payload.get("as_of_utc"),
				"teams_count": len(teams),
			}, teams

		raw_dir = get_ingestion_raw_dir(target_date)
		rosters_path = raw_dir / f"rosters_{target_date.strftime('%Y-%m-%d')}.json"
		if not rosters_path.exists():
			raise FreeAgentCandidatesError(f"Missing roster snapshot for free-agent filtering: {rosters_path}")
		payload = read_json(rosters_path)
		teams = payload.get("body", {}).get("rosters", {}).get("teams", [])
		player_ids = set()
		for team in teams:
			for player in team.get("players", []):
				player_id = player.get("id")
				if player_id is not None:
					player_ids.add(str(player_id))
		normalized_teams = []
		for team in teams:
			normalized_teams.append(
				{
					"team_id": str(team.get("id", "")),
					"team_name": team.get("name") or team.get("long_abbr") or f"TEAM_{team.get('id')}",
					"players": [
						{
							"player_id": str(player.get("id")),
							"player_name": player.get("fullname") or player.get("name") or f"UNKNOWN_{player.get('id')}",
						}
						for player in team.get("players", [])
						if player.get("id") is not None
					],
				}
			)
		return player_ids, {
			"source": "raw_rosters",
			"as_of_utc": None,
			"teams_count": len(teams),
		}, normalized_teams

	def _prior_is_rostered(self, rostered_ids, prior):
		pid = str(prior.get("player_id") or "")
		if pid and pid != "None" and pid in rostered_ids:
			return True
		cbs = str(prior.get("cbs_player_id") or "").strip()
		if cbs and cbs != "None" and cbs in rostered_ids:
			return True
		return False

	def _merge_prior_space_rostered_ids(self, rostered_ids, roster_teams, priors_players):
		"""
		Roster snapshots use CBS Fantasy player ids (e.g. 2071264) while preseason priors
		and projections use CSV player_id (e.g. 15640). Those strings never match, so
		rostered stars would incorrectly appear as free-agent candidates.

		Merge order:
		1) If a prior row carries ``cbs_player_id`` (same as roster ``player.id`` / CBS
		   Fantasy id) and that id is rostered, add the prior's ``player_id`` to the set.
		2) Otherwise, for each roster player, if the normalized name matches exactly one
		   prior row, add that prior's ``player_id``. If multiple priors share the name,
		   disambiguate when exactly one prior's ``cbs_player_id`` equals the roster
		   player's CBS id; else count as ambiguous.
		"""
		meta = {}
		if not roster_teams or not priors_players:
			return rostered_ids, meta

		prior_by_id = {}
		for prior in priors_players:
			pid = str(prior.get("player_id") or "")
			if pid and pid != "None":
				prior_by_id[pid] = prior

		cbs_added = 0
		for prior in priors_players:
			cbs = prior.get("cbs_player_id")
			if cbs is None:
				continue
			cbs = str(cbs).strip()
			if not cbs or cbs == "None":
				continue
			if cbs not in rostered_ids:
				continue
			pid = str(prior.get("player_id") or "")
			if not pid or pid == "None":
				continue
			if pid not in rostered_ids:
				rostered_ids.add(pid)
				cbs_added += 1
		if cbs_added:
			meta["prior_space_ids_merged_via_cbs_id"] = cbs_added

		by_name = defaultdict(list)
		for prior in priors_players:
			name = (prior.get("player_name") or "").strip().lower()
			if not name:
				continue
			pid = str(prior.get("player_id") or "")
			if not pid or pid == "None":
				continue
			by_name[name].append(pid)

		name_added = 0
		ambiguous_names = 0
		for team in roster_teams:
			for player in team.get("players", []):
				name = (player.get("player_name") or "").strip().lower()
				if not name:
					continue
				candidates = by_name.get(name)
				if not candidates:
					continue
				unique = list(dict.fromkeys(candidates))
				roster_cbs = str(player.get("player_id") or "")
				if len(unique) == 1:
					prior_pid = unique[0]
					if prior_pid not in rostered_ids:
						rostered_ids.add(prior_pid)
						name_added += 1
				else:
					matched = None
					for pid in unique:
						pr = prior_by_id.get(pid)
						if not pr:
							continue
						if str(pr.get("cbs_player_id") or "").strip() == roster_cbs:
							matched = pid
							break
					if matched and matched not in rostered_ids:
						rostered_ids.add(matched)
						name_added += 1
					elif not matched:
						ambiguous_names += 1

		if name_added:
			meta["prior_space_ids_merged"] = name_added
		if ambiguous_names:
			meta["ambiguous_prior_name_matches"] = ambiguous_names

		if not meta:
			return rostered_ids, {}
		return rostered_ids, meta

	def _load_eligibility_map(self):
		eligibility_path = get_player_eligibility_latest_path()
		if not eligibility_path.exists():
			return {}
		players = read_json(eligibility_path).get("players", [])
		return {str(player.get("player_id")): list(player.get("slot_positions", [])) for player in players}

	def _to_float(self, value):
		try:
			return float(value)
		except Exception:
			return 0.0

	def _build_replacement_suggestions(self, fa_candidates, roster_teams, eligibility_map, daily_map, weekly_map, priors_map):
		suggestions = []
		teams_considered = 0
		drop_pool_total = 0
		for team in roster_teams:
			team_id = str(team.get("team_id", ""))
			team_name = team.get("team_name") or f"TEAM_{team_id}"
			rostered_rows = self._build_team_rostered_rows(
				team=team,
				eligibility_map=eligibility_map,
				daily_map=daily_map,
				weekly_map=weekly_map,
				priors_map=priors_map,
			)
			if not rostered_rows:
				continue
			teams_considered += 1
			drop_pool = sorted(rostered_rows, key=lambda row: row["composite_score"])[: max(1, self.drop_pool_size)]
			drop_pool_total += len(drop_pool)
			for fa in fa_candidates:
				for drop in drop_pool:
					if not self._is_compatible(fa, drop):
						continue
					net_daily = fa["projected_points_daily"] - drop["projected_points_daily"]
					net_weekly = fa["projected_points_weekly"] - drop["projected_points_weekly"]
					net_composite = fa["composite_score"] - drop["composite_score"]
					if net_composite < self.min_net_gain:
						continue
					suggestions.append(
						{
							"team_id": team_id,
							"team_name": team_name,
							"add_player": {
								"player_id": fa["player_id"],
								"player_name": fa["player_name"],
								"player_role": fa["player_role"],
								"slot_positions": fa.get("slot_positions", []),
							},
							"drop_player": {
								"player_id": drop["player_id"],
								"player_name": drop["player_name"],
								"player_role": drop["player_role"],
								"slot_positions": drop.get("slot_positions", []),
							},
							"net_points_daily": round(net_daily, 6),
							"net_points_weekly": round(net_weekly, 6),
							"net_composite_score": round(net_composite, 6),
						}
					)
		suggestions.sort(key=lambda row: (row["net_composite_score"], row["net_points_weekly"]), reverse=True)
		if self.max_replacement_suggestions > 0:
			suggestions = suggestions[: self.max_replacement_suggestions]
		return {
			"summary": {
				"teams_considered": teams_considered,
				"drop_pool_players_considered": drop_pool_total,
				"suggestions_count": len(suggestions),
				"min_net_gain": self.min_net_gain,
			},
			"suggestions": suggestions,
		}

	def _build_team_rostered_rows(self, team, eligibility_map, daily_map, weekly_map, priors_map):
		rows = []
		for player in team.get("players", []):
			player_id = str(player.get("player_id"))
			if not player_id or player_id == "None":
				continue
			prior = priors_map.get(player_id, {})
			daily_row = daily_map.get(player_id, {})
			weekly_row = weekly_map.get(player_id, {})
			daily_points = self._to_float(daily_row.get("projected_points_window"))
			weekly_points = self._to_float(weekly_row.get("projected_points_window"))
			composite = (self.weekly_weight * weekly_points) + (self.daily_weight * daily_points)
			rows.append(
				{
					"player_id": player_id,
					"player_name": player.get("player_name") or prior.get("player_name") or f"UNKNOWN_{player_id}",
					"player_role": prior.get("player_role", "unknown"),
					"projected_points_daily": round(daily_points, 6),
					"projected_points_weekly": round(weekly_points, 6),
					"composite_score": round(composite, 6),
					"slot_positions": eligibility_map.get(player_id, []),
				}
			)
		return rows

	def _is_compatible(self, fa_row, drop_row):
		fa_role = str(fa_row.get("player_role", "unknown"))
		drop_role = str(drop_row.get("player_role", "unknown"))
		if fa_role == drop_role:
			return True
		fa_slots = set(fa_row.get("slot_positions", []))
		drop_slots = set(drop_row.get("slot_positions", []))
		if fa_slots and drop_slots and fa_slots.intersection(drop_slots):
			return True
		if fa_role == "unknown" or drop_role == "unknown":
			return True
		return False
