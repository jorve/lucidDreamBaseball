"""Roster CBS ids vs prior CSV player_id alignment for FA filtering."""
import pathlib
import sys
import unittest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.free_agent_candidates import FreeAgentCandidatesBuilder  # noqa: E402


class MergePriorSpaceRosterIdsTests(unittest.TestCase):
	def test_adds_prior_id_when_cbs_id_differs(self):
		b = FreeAgentCandidatesBuilder()
		rostered = {"2071264"}  # CBS id on roster
		teams = [
			{
				"team_id": "1",
				"team_name": "T1",
				"players": [
					{"player_id": "2071264", "player_name": "Aaron Judge"},
				],
			}
		]
		priors = [
			{"player_id": "15640", "player_name": "Aaron Judge", "player_role": "batters"},
		]
		out, meta = b._merge_prior_space_rostered_ids(set(rostered), teams, priors)
		self.assertIn("2071264", out)
		self.assertIn("15640", out)
		self.assertGreaterEqual(meta.get("prior_space_ids_merged", 0), 1)

	def test_merges_via_cbs_player_id_on_prior(self):
		b = FreeAgentCandidatesBuilder()
		rostered = {"2071264"}
		teams = [
			{
				"team_id": "1",
				"team_name": "T1",
				"players": [
					{"player_id": "2071264", "player_name": "Aaron Judge"},
				],
			}
		]
		priors = [
			{
				"player_id": "15640",
				"player_name": "Aaron Judge",
				"player_role": "batters",
				"cbs_player_id": "2071264",
			},
		]
		out, meta = b._merge_prior_space_rostered_ids(set(rostered), teams, priors)
		self.assertIn("2071264", out)
		self.assertIn("15640", out)
		self.assertGreaterEqual(meta.get("prior_space_ids_merged_via_cbs_id", 0), 1)
		self.assertEqual(meta.get("prior_space_ids_merged", 0), 0)

	def test_ambiguous_name_resolved_when_prior_has_matching_cbs_id(self):
		b = FreeAgentCandidatesBuilder()
		rostered = {"999"}
		teams = [
			{
				"team_id": "1",
				"team_name": "T1",
				"players": [{"player_id": "999", "player_name": "Will Smith"}],
			}
		]
		priors = [
			{"player_id": "1", "player_name": "Will Smith", "player_role": "batters"},
			{
				"player_id": "2",
				"player_name": "Will Smith",
				"player_role": "batters",
				"cbs_player_id": "999",
			},
		]
		out, meta = b._merge_prior_space_rostered_ids(set(rostered), teams, priors)
		self.assertIn("999", out)
		self.assertIn("2", out)
		self.assertNotIn("1", out)
		self.assertEqual(meta.get("ambiguous_prior_name_matches", 0), 0)

	def test_prior_is_rostered_by_cbs_player_id(self):
		b = FreeAgentCandidatesBuilder()
		rostered = {"2071264", "15640"}
		self.assertTrue(
			b._prior_is_rostered(rostered, {"player_id": "99999", "cbs_player_id": "2071264"}),
		)
		self.assertFalse(
			b._prior_is_rostered(rostered, {"player_id": "88888", "cbs_player_id": "FA_ONLY"}),
		)

	def test_ambiguous_name_not_added(self):
		b = FreeAgentCandidatesBuilder()
		rostered = {"999"}
		teams = [
			{
				"team_id": "1",
				"team_name": "T1",
				"players": [{"player_id": "999", "player_name": "Will Smith"}],
			}
		]
		priors = [
			{"player_id": "1", "player_name": "Will Smith", "player_role": "batters"},
			{"player_id": "2", "player_name": "Will Smith", "player_role": "batters"},
		]
		out, meta = b._merge_prior_space_rostered_ids(set(rostered), teams, priors)
		self.assertEqual(out, {"999"})
		self.assertGreater(meta.get("ambiguous_prior_name_matches", 0), 0)


if __name__ == "__main__":
	unittest.main()
