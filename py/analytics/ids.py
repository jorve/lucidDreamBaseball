import hashlib
import json


def build_event_id(event_ts_compact, event_type, team_ids, player_ids):
	team_sig = "_".join(sorted(str(team_id) for team_id in team_ids if team_id))
	if not team_sig:
		team_sig = "na"
	sorted_player_ids = sorted(str(player_id) for player_id in player_ids if player_id)
	if len(sorted_player_ids) > 4:
		player_sig = "_".join(sorted_player_ids[:4]) + f"_plus{len(sorted_player_ids) - 4}"
	else:
		player_sig = "_".join(sorted_player_ids) or "na"
	return f"txn_{event_ts_compact}_{event_type}_{team_sig}_{player_sig}"


def canonical_payload_hash(payload):
	encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
	return hashlib.sha1(encoded).hexdigest()
