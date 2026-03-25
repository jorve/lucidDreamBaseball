"""
Contract tests: JSON shapes expected by ui/app.js (Free Agents + swaps).

Keeps weekly_digest and free_agent_candidates aligned with the GM UI without a browser.
"""
import json
import unittest
from pathlib import Path
from typing import Any, Dict, Optional


REPO = Path(__file__).resolve().parents[1]


def _load_optional(path: Path) -> Optional[Dict[str, Any]]:
	if not path.is_file():
		return None
	with path.open(encoding="utf-8") as f:
		return json.load(f)


class WeeklyDigestSwapsContractTests(unittest.TestCase):
	def test_recommended_swaps_shape_if_present(self):
		payload = _load_optional(REPO / "json" / "weekly_digest_latest.json")
		if payload is None:
			self.skipTest("weekly_digest_latest.json not present")
		swaps = payload.get("recommended_swaps")
		self.assertIsInstance(swaps, list, "recommended_swaps must be a list")
		for i, s in enumerate(swaps):
			self.assertIsInstance(s, dict, f"recommended_swaps[{i}] must be an object")
			for key in (
				"team_id",
				"team_name",
				"add_player",
				"drop_player",
				"net_points_daily",
				"net_points_weekly",
				"net_composite_score",
			):
				self.assertIn(key, s, f"swap {i} missing {key}")
			self.assertIsInstance(s["add_player"], dict)
			self.assertIsInstance(s["drop_player"], dict)


class FreeAgentCandidatesContractTests(unittest.TestCase):
	def test_candidates_shape_if_present(self):
		payload = _load_optional(REPO / "json" / "free_agent_candidates_latest.json")
		if payload is None:
			self.skipTest("free_agent_candidates_latest.json not present")
		cands = payload.get("candidates")
		if cands is None:
			return
		self.assertIsInstance(cands, list)
		for i, c in enumerate(cands[:20]):
			self.assertIsInstance(c, dict, f"candidates[{i}] must be an object")
			for key in ("player_name", "player_role", "composite_score"):
				self.assertIn(key, c, f"candidate {i} missing {key}")


if __name__ == "__main__":
	unittest.main()
