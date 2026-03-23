import argparse
import json
import sqlite3
from datetime import datetime, timezone
from hashlib import sha256

from project_config import get_json_output_dir, get_storage_db_path, get_storage_parity_latest_path
from storage import StorageRecorder


UTC = timezone.utc


def parse_args():
	parser = argparse.ArgumentParser(description="Backfill DB storage and run JSON/DB parity checks.")
	parser.add_argument(
		"--mode",
		choices=["backfill", "parity", "both"],
		default="both",
		help="Operation mode.",
	)
	parser.add_argument(
		"--artifact-glob",
		default="*.json",
		help="Glob pattern for artifacts in json output directory.",
	)
	parser.add_argument(
		"--force-write",
		action="store_true",
		help="Force backfill writes even when storage mode is json_only.",
	)
	return parser.parse_args()


def _load_json(path_value):
	with path_value.open() as infile:
		return json.load(infile)


def _hash_payload(payload):
	serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
	return sha256(serialized.encode("utf-8")).hexdigest()


def backfill(glob_pattern, force_write=False):
	recorder = StorageRecorder(force_enabled=force_write)
	json_dir = get_json_output_dir()
	files = sorted(path for path in json_dir.glob(glob_pattern) if path.name != "storage_parity_latest.json")
	results = {"scanned": len(files), "written": 0, "failed": 0, "skipped_invalid_json": 0}
	for file_path in files:
		try:
			payload = _load_json(file_path)
		except Exception:
			results["skipped_invalid_json"] += 1
			continue
		write_result = recorder.record_json_artifact(
			path_value=file_path,
			payload=payload,
			artifact_kind="analytics",
			write_source="storage_backfill_and_parity.backfill",
		)
		if write_result.get("status") == "ok":
			results["written"] += 1
		elif write_result.get("status") == "skipped":
			pass
		else:
			results["failed"] += 1
	return results


def parity(glob_pattern):
	json_dir = get_json_output_dir()
	files = sorted(path for path in json_dir.glob(glob_pattern) if path.name != "storage_parity_latest.json")
	db_path = get_storage_db_path()
	recorder = StorageRecorder()
	summary = {
		"scanned": 0,
		"matched": 0,
		"mismatched": 0,
		"missing_in_db": 0,
		"skipped_invalid_json": 0,
		"failures": [],
	}
	with sqlite3.connect(str(db_path)) as conn:
		recorder._ensure_schema(conn)
		for file_path in files:
			summary["scanned"] += 1
			try:
				payload = _load_json(file_path)
				expected_hash = _hash_payload(payload)
			except Exception as error:
				summary["skipped_invalid_json"] += 1
				summary["failures"].append(
					{
						"artifact_path": str(file_path),
						"reason": f"skipped_invalid_json: {error}",
					}
				)
				continue

			row = conn.execute(
				"""
				SELECT payload_hash
				FROM artifact_writes
				WHERE artifact_path = ?
				ORDER BY id DESC
				LIMIT 1
				""",
				(str(file_path),),
			).fetchone()
			if row is None:
				summary["missing_in_db"] += 1
				summary["failures"].append(
					{
						"artifact_path": str(file_path),
						"reason": "missing_in_db",
					}
				)
				continue
			actual_hash = row[0]
			if actual_hash == expected_hash:
				summary["matched"] += 1
			else:
				summary["mismatched"] += 1
				summary["failures"].append(
					{
						"artifact_path": str(file_path),
						"reason": "hash_mismatch",
						"expected_hash": expected_hash,
						"actual_hash": actual_hash,
					}
				)
	return summary


def main():
	args = parse_args()
	output = {"mode": args.mode}
	if args.mode in {"backfill", "both"}:
		output["backfill"] = backfill(args.artifact_glob, force_write=args.force_write)
	if args.mode in {"parity", "both"}:
		output["parity"] = parity(args.artifact_glob)
		parity_path = get_storage_parity_latest_path()
		parity_payload = {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"artifact_glob": args.artifact_glob,
			"summary": output["parity"],
		}
		with parity_path.open("w") as outfile:
			json.dump(parity_payload, outfile, indent=2)

	print(json.dumps(output, indent=2))
	if args.mode in {"parity", "both"}:
		parity_result = output.get("parity", {})
		if parity_result.get("mismatched", 0) > 0 or parity_result.get("missing_in_db", 0) > 0:
			return 2
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
