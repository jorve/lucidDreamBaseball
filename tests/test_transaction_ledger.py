import datetime
import json
import pathlib
import sys
import unittest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from analytics.transaction_ledger import TransactionLedgerBuilder, TransactionLedgerError  # noqa: E402


def _canonicalize_transactions_payload(payload):
	copy_payload = json.loads(json.dumps(payload))
	copy_payload.pop("generated_at_utc", None)
	for event in copy_payload.get("events", []):
		event.pop("ingested_at_utc", None)
	return copy_payload


class TestTransactionLedger(unittest.TestCase):
	def setUp(self):
		self.builder = TransactionLedgerBuilder()

	def test_normalize_record_add(self):
		record = {
			"event_type": "add",
			"event_ts": "2026-03-16T11:00:00Z",
			"team_to": {"team_id": "17", "team_name": "Team A"},
			"players": [{"player_id": "12345", "player_name": "Player One"}],
		}
		event = self.builder._normalize_record(record)
		self.assertEqual(event["event_type"], "add")
		self.assertEqual(event["event_id"], "txn_20260316110000_add_17_12345")

	def test_unknown_event_type_raises(self):
		record = {"event_type": "mystery", "event_ts": "2026-03-16T11:00:00Z", "players": [{"player_id": "1"}]}
		with self.assertRaises(TransactionLedgerError):
			self.builder._normalize_record(record)

	def test_dedupe_by_event_id(self):
		event = {
			"event_id": "txn_20260316110000_add_17_12345",
			"event_ts": "2026-03-16T11:00:00Z",
			"event_type": "add",
			"players": [{"player_id": "12345", "player_name": "Player One", "movement": "to_team"}],
			"_semantic_key": "2026-03-16T11:00:00Z|add|17|12345",
			"_content_hash": "abc",
			"_source_priority": 1,
			"raw_refs": {"sources": [{"resource": "transactions", "priority": 1, "raw_event_key": "1"}]},
		}
		events, stats = self.builder._dedupe_events([event, dict(event)])
		self.assertEqual(len(events), 1)
		self.assertEqual(stats["events_deduped"], 1)

	def test_semantic_alias_maps_to_canonical_event(self):
		base = {
			"event_id": "txn_20260316110000_add_17_12345",
			"event_ts": "2026-03-16T11:00:00Z",
			"event_type": "add",
			"players": [{"player_id": "12345", "player_name": "Player One", "movement": "to_team"}],
			"team_to": {"team_id": "17", "team_name": "Team A"},
			"_semantic_key": "2026-03-16T11:00:00Z|add|17|12345",
			"_content_hash": "same",
			"_source_priority": 1,
			"raw_refs": {"sources": [{"resource": "transactions", "priority": 1, "raw_event_key": "1"}]},
		}
		alias = dict(base)
		alias["event_id"] = "txn_alt_identifier"
		alias["raw_refs"] = {"sources": [{"resource": "activity", "priority": 3, "raw_event_key": "A1"}]}
		events, stats = self.builder._dedupe_events([base, alias])
		self.assertEqual(len(events), 1)
		self.assertEqual(stats["events_aliased"], 1)
		self.assertIn("txn_alt_identifier", events[0]["raw_refs"]["alias_event_ids"])

	def test_collision_generates_versioned_event_id(self):
		first = {
			"event_id": "txn_same",
			"event_ts": "2026-03-16T11:00:00Z",
			"event_type": "add",
			"players": [{"player_id": "12345", "player_name": "Player One", "movement": "to_team"}],
			"team_to": {"team_id": "17", "team_name": "Team A"},
			"_semantic_key": "k1",
			"_content_hash": "hash_a",
			"_source_priority": 1,
			"raw_refs": {"sources": [{"resource": "transactions", "priority": 1, "raw_event_key": "1"}]},
			"validation_code": "ok",
		}
		second = dict(first)
		second["_semantic_key"] = "k2"
		second["_content_hash"] = "hash_b"
		events, stats = self.builder._dedupe_events([first, second])
		self.assertEqual(len(events), 2)
		self.assertEqual(stats["events_collisions"], 1)
		self.assertTrue(any(event["event_id"].startswith("txn_same_v") for event in events))

	def test_quarantine_reason_codes_are_specific(self):
		with self.assertRaises(TransactionLedgerError) as err:
			self.builder._normalize_record({"event_type": "add", "players": [{"player_id": "1"}]})
		self.assertEqual(str(err.exception), "TXN_TIMESTAMP_MISSING")

	def test_build_is_idempotent_for_same_snapshot(self):
		target_date = datetime.datetime(2026, 3, 16)
		self.builder.build(target_date=target_date, dry_run=False)
		first_payload = json.loads((PROJECT_ROOT / "json" / "transactions_latest.json").read_text())
		self.builder.build(target_date=target_date, dry_run=False)
		second_payload = json.loads((PROJECT_ROOT / "json" / "transactions_latest.json").read_text())
		self.assertEqual(
			_canonicalize_transactions_payload(first_payload),
			_canonicalize_transactions_payload(second_payload),
		)


if __name__ == "__main__":
	unittest.main()
