from copy import deepcopy
from datetime import datetime, timezone

from analytics.io import read_json, write_json
from analytics.validators import ValidationError, validate_roster_state_payload
from project_config import (
	get_ingestion_raw_dir,
	get_roster_state_diagnostics_latest_path,
	get_roster_state_latest_path,
	get_transactions_latest_path,
)


UTC = timezone.utc


class RosterStateError(RuntimeError):
	pass


class RosterStateBuilder:
	def build(self, target_date, dry_run=False):
		raw_dir = get_ingestion_raw_dir(target_date)
		roster_path = raw_dir / f"rosters_{target_date.strftime('%Y-%m-%d')}.json"
		if not roster_path.exists():
			raise RosterStateError(f"Missing roster snapshot: {roster_path}")
		roster_payload = read_json(roster_path)

		transactions_path = get_transactions_latest_path()
		if transactions_path.exists():
			transactions_payload = read_json(transactions_path)
			events = transactions_payload.get("events", [])
		else:
			events = []

		team_map, player_to_team = self._build_base_state(roster_payload)
		ordered_events = sorted(events, key=lambda event: (event.get("event_ts", ""), event.get("event_id", "")))

		quarantined_events = []
		skipped_events = []
		applied_events = []
		atomic_trade_failures = 0

		for event in ordered_events:
			try:
				applied = self._apply_event(event, team_map, player_to_team)
				if applied:
					applied_events.append(event)
				else:
					skipped_events.append(
						{
							"event_id": event.get("event_id"),
							"reason_code": "ROSTER_EVENT_SKIPPED_NOOP",
						}
					)
			except RosterStateError as error:
				error_text = str(error)
				if "ROSTER_TRADE_ATOMICITY_FAIL" in error_text:
					atomic_trade_failures += 1
				quarantined_events.append(
					{
						"event_id": event.get("event_id"),
						"reason_code": error_text,
					}
				)

		if self._has_duplicate_assignments(team_map):
			raise RosterStateError("ROSTER_DUPLICATE_PLAYER_ASSIGNMENT")

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		teams = []
		for team_id in sorted(team_map.keys(), key=self._team_sort_key):
			team = team_map[team_id]
			players = sorted(team["players"].values(), key=lambda player: str(player["player_id"]))
			teams.append({"team_id": team["team_id"], "team_name": team["team_name"], "players": players})

		payload = {
			"schema_version": "1.0",
			"as_of_utc": now_utc,
			"base_snapshot": {
				"source_date": target_date.strftime("%Y-%m-%d"),
				"resource": "rosters",
			},
			"event_apply_window": {
				"first_event_id": applied_events[0]["event_id"] if applied_events else None,
				"last_event_id_applied": applied_events[-1]["event_id"] if applied_events else None,
				"events_applied_count": len(applied_events),
			},
			"teams": teams,
			"integrity": {
				"duplicate_player_assignments": 0,
				"unknown_players_quarantined": len([event for event in quarantined_events if "PLAYER_UNKNOWN" in event["reason_code"]]),
				"atomic_trade_failures": atomic_trade_failures,
				"status": "warning" if quarantined_events else "ok",
			},
		}

		diagnostics = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"events_processed": len(ordered_events),
			"events_applied": len(applied_events),
			"events_skipped": len(skipped_events),
			"quarantined_events_count": len(quarantined_events),
			"quarantined_events": quarantined_events,
			"skipped_events": skipped_events,
			"integrity_checks": payload["integrity"],
		}

		try:
			validate_roster_state_payload(payload)
		except ValidationError as error:
			raise RosterStateError(f"Invalid roster state payload: {error}")

		output_path = get_roster_state_latest_path()
		diagnostics_path = get_roster_state_diagnostics_latest_path()
		if not dry_run:
			write_json(output_path, payload)
			write_json(diagnostics_path, diagnostics)

		return {
			"status": "ok",
			"output_path": output_path,
			"diagnostics_path": diagnostics_path,
			"events_applied": len(applied_events),
			"events_quarantined": len(quarantined_events),
			"integrity": payload["integrity"],
		}

	def _build_base_state(self, roster_payload):
		try:
			teams = roster_payload["body"]["rosters"]["teams"]
		except Exception as error:
			raise RosterStateError(f"ROSTER_PAYLOAD_INVALID: {error}")
		if not isinstance(teams, list) or not teams:
			raise RosterStateError("ROSTER_PAYLOAD_INVALID: no teams present")

		team_map = {}
		player_to_team = {}
		for team in teams:
			team_id = str(team.get("id"))
			if not team_id or team_id == "None":
				continue
			canonical_team = {
				"team_id": team_id,
				"team_name": team.get("name") or team.get("long_abbr") or f"TEAM_{team_id}",
				"players": {},
			}
			for player in team.get("players", []):
				player_id = player.get("id")
				if player_id is None:
					continue
				player_id = str(player_id)
				if player_id in player_to_team and player_to_team[player_id] != team_id:
					raise RosterStateError("ROSTER_DUPLICATE_PLAYER_ASSIGNMENT")
				canonical_player = {
					"player_id": player_id,
					"player_name": player.get("fullname") or player.get("name") or f"UNKNOWN_{player_id}",
					"positions": self._normalize_positions(player),
					"status": player.get("roster_status") or "active",
				}
				canonical_team["players"][player_id] = canonical_player
				player_to_team[player_id] = team_id
			team_map[team_id] = canonical_team
		return team_map, player_to_team

	def _normalize_positions(self, player):
		eligible = player.get("eligible")
		if isinstance(eligible, list):
			return [str(position) for position in eligible]
		if isinstance(eligible, str):
			return [eligible]
		position = player.get("position")
		if position:
			return [str(position)]
		return []

	def _apply_event(self, event, team_map, player_to_team):
		event_type = event.get("event_type")
		if event_type == "add":
			return self._apply_add(event, team_map, player_to_team)
		if event_type == "drop":
			return self._apply_drop(event, team_map, player_to_team)
		if event_type == "trade":
			return self._apply_trade(event, team_map, player_to_team)
		raise RosterStateError("ROSTER_EVENT_TYPE_UNKNOWN")

	def _apply_add(self, event, team_map, player_to_team):
		team_to = self._event_team_id(event.get("team_to"))
		if not team_to or team_to not in team_map:
			raise RosterStateError("ROSTER_TEAM_UNKNOWN")
		applied = False
		for player in event.get("players", []):
			player_id = str(player.get("player_id"))
			if not player_id or player_id == "None":
				raise RosterStateError("ROSTER_PLAYER_UNKNOWN")
			current_team = player_to_team.get(player_id)
			if current_team == team_to:
				continue
			if current_team and current_team != team_to:
				raise RosterStateError("ROSTER_ADD_ALREADY_OWNED")
			team_map[team_to]["players"][player_id] = self._event_player_to_canonical(player, event.get("event_id"))
			player_to_team[player_id] = team_to
			applied = True
		return applied

	def _apply_drop(self, event, team_map, player_to_team):
		team_from = self._event_team_id(event.get("team_from"))
		if not team_from or team_from not in team_map:
			raise RosterStateError("ROSTER_TEAM_UNKNOWN")
		applied = False
		for player in event.get("players", []):
			player_id = str(player.get("player_id"))
			if not player_id or player_id == "None":
				raise RosterStateError("ROSTER_PLAYER_UNKNOWN")
			if player_to_team.get(player_id) != team_from:
				continue
			team_map[team_from]["players"].pop(player_id, None)
			player_to_team.pop(player_id, None)
			applied = True
		return applied

	def _apply_trade(self, event, team_map, player_to_team):
		team_from = self._event_team_id(event.get("team_from"))
		team_to = self._event_team_id(event.get("team_to"))
		if not team_from or not team_to or team_from not in team_map or team_to not in team_map:
			raise RosterStateError("ROSTER_TEAM_UNKNOWN")

		staged_team_map = deepcopy(team_map)
		staged_index = dict(player_to_team)
		moves = []
		for player in event.get("players", []):
			player_id = str(player.get("player_id"))
			if not player_id or player_id == "None":
				raise RosterStateError("ROSTER_PLAYER_UNKNOWN")
			movement = str(player.get("movement", "")).lower()
			if movement == "from_team_to":
				source_team = team_from
				dest_team = team_to
			elif movement == "to_team_from":
				source_team = team_to
				dest_team = team_from
			else:
				owned_by = staged_index.get(player_id)
				if owned_by == team_from:
					source_team = team_from
					dest_team = team_to
				elif owned_by == team_to:
					source_team = team_to
					dest_team = team_from
				else:
					raise RosterStateError("ROSTER_TRADE_ATOMICITY_FAIL")
			if staged_index.get(player_id) != source_team:
				raise RosterStateError("ROSTER_TRADE_ATOMICITY_FAIL")
			player_payload = staged_team_map[source_team]["players"].get(player_id)
			if not player_payload:
				raise RosterStateError("ROSTER_TRADE_ATOMICITY_FAIL")
			moves.append((player_id, source_team, dest_team, player_payload, player))

		for player_id, source_team, _, _, _ in moves:
			staged_team_map[source_team]["players"].pop(player_id, None)
			staged_index.pop(player_id, None)
		for player_id, _, dest_team, player_payload, event_player in moves:
			new_payload = dict(player_payload)
			new_payload["acquired_via_event_id"] = event.get("event_id")
			new_payload["player_name"] = event_player.get("player_name") or new_payload.get("player_name")
			staged_team_map[dest_team]["players"][player_id] = new_payload
			staged_index[player_id] = dest_team

		team_map.clear()
		team_map.update(staged_team_map)
		player_to_team.clear()
		player_to_team.update(staged_index)
		return True

	def _event_player_to_canonical(self, event_player, event_id):
		return {
			"player_id": str(event_player.get("player_id")),
			"player_name": event_player.get("player_name") or f"UNKNOWN_{event_player.get('player_id')}",
			"positions": [],
			"status": "active",
			"acquired_via_event_id": event_id,
		}

	def _event_team_id(self, team_payload):
		if not isinstance(team_payload, dict):
			return None
		team_id = team_payload.get("team_id")
		if team_id is None:
			return None
		return str(team_id)

	def _has_duplicate_assignments(self, team_map):
		seen = {}
		for team_id, team in team_map.items():
			for player_id in team["players"].keys():
				if player_id in seen and seen[player_id] != team_id:
					return True
				seen[player_id] = team_id
		return False

	def _team_sort_key(self, team_id):
		text = str(team_id)
		return (0, int(text)) if text.isdigit() else (1, text)
