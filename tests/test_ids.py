import pathlib
import sys
import unittest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.ids import build_event_id  # noqa: E402


class TestIds(unittest.TestCase):
	def test_build_event_id_sorts_team_and_player_ids(self):
		event_id = build_event_id(
			event_ts_compact="20260316183012",
			event_type="trade",
			team_ids=["17", "9"],
			player_ids=["67890", "12345"],
		)
		self.assertEqual(event_id, "txn_20260316183012_trade_17_9_12345_67890")

	def test_build_event_id_truncates_long_player_list(self):
		event_id = build_event_id(
			event_ts_compact="20260316190000",
			event_type="trade",
			team_ids=["12", "3"],
			player_ids=["333", "222", "555", "111", "444"],
		)
		self.assertEqual(event_id, "txn_20260316190000_trade_12_3_111_222_333_444_plus1")


if __name__ == "__main__":
	unittest.main()
