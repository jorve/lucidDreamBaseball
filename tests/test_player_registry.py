import pathlib
import sys
import unittest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.player_registry import PlayerRegistryBuilder  # noqa: E402


class PlayerRegistryTests(unittest.TestCase):
	def test_matches_prior_by_cbs_player_id(self):
		b = PlayerRegistryBuilder()
		rosters_payload = {
			"body": {
				"rosters": {
					"teams": [
						{
							"id": "1",
							"name": "T1",
							"long_abbr": "T1",
							"players": [
								{
									"id": "2071264",
									"fullname": "Aaron Judge",
									"pro_team": "NYY",
									"position": "RF",
									"eligible_positions_display": "RF,OF",
									"owned_by_team_id": "1",
									"roster_status": "A",
								}
							],
						}
					]
				}
			}
		}
		priors = [{"player_id": "15640", "player_name": "Aaron Judge", "mlbam_id": "592450", "cbs_player_id": "2071264"}]
		reg = b._build_registry(
			target_date=__import__("datetime").date(2026, 3, 25),
			rosters_payload=rosters_payload,
			live_scoring_payload=None,
			priors_players=priors,
			elig_players=[],
		)
		player = next(p for p in reg["players"] if p["cbs_player_id"] == "2071264")
		self.assertEqual(player["crosswalk"]["prior_player_id"], "15640")
		self.assertEqual(player["crosswalk"]["match_method"], "cbs_player_id")
		self.assertEqual(player["crosswalk"]["match_quality"], "exact")

	def test_unmatched_goes_to_review_queue(self):
		b = PlayerRegistryBuilder()
		rosters_payload = {
			"body": {"rosters": {"teams": [{"id": "1", "name": "T1", "players": [{"id": "1", "fullname": "Mystery Guy", "pro_team": "AAA"}]}]}}}
		reg = b._build_registry(
			target_date=__import__("datetime").date(2026, 3, 25),
			rosters_payload=rosters_payload,
			live_scoring_payload=None,
			priors_players=[],
			elig_players=[],
		)
		self.assertEqual(reg["summary"]["unmatched"], 1)
		self.assertTrue(any(item["kind"] == "unmatched" and item["cbs_player_id"] == "1" for item in reg["review_queue"]))

	def test_ambiguous_name_goes_to_review_queue(self):
		b = PlayerRegistryBuilder()
		rosters_payload = {
			"body": {"rosters": {"teams": [{"id": "1", "name": "T1", "players": [{"id": "9", "fullname": "Will Smith", "pro_team": "LAD"}]}]}}}
		priors = [
			{"player_id": "1", "player_name": "Will Smith", "mlbam_id": "A"},
			{"player_id": "2", "player_name": "Will Smith", "mlbam_id": "B"},
		]
		reg = b._build_registry(
			target_date=__import__("datetime").date(2026, 3, 25),
			rosters_payload=rosters_payload,
			live_scoring_payload=None,
			priors_players=priors,
			elig_players=[],
		)
		player = next(p for p in reg["players"] if p["cbs_player_id"] == "9")
		self.assertEqual(player["crosswalk"]["match_quality"], "ambiguous")
		self.assertTrue(any(item["kind"] == "ambiguous_name" for item in reg["review_queue"]))


if __name__ == "__main__":
	unittest.main()

