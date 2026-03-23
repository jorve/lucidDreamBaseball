import argparse
import json
from datetime import datetime, timedelta, timezone

from project_config import get_ingestion_config, get_ingestion_status_latest_path, get_logs_dir


UTC = timezone.utc


def parse_args():
	parser = argparse.ArgumentParser(description="Check ingestion health from ingestion logs.")
	parser.add_argument(
		"--max-age-hours",
		type=float,
		help="Override max age threshold in hours for latest successful ingestion.",
	)
	return parser.parse_args()


def parse_log_line(line):
	try:
		return json.loads(line)
	except Exception:
		return None


def get_latest_success_record():
	logs_dir = get_logs_dir()
	log_files = sorted(logs_dir.glob("ingestion_*.log"), reverse=True)
	for log_file in log_files:
		with log_file.open() as infile:
			lines = [line.strip() for line in infile.readlines() if line.strip()]
		for raw_line in reversed(lines):
			record = parse_log_line(raw_line)
			if not record:
				continue
			if record.get("status") != "ok":
				continue
			auth_info = record.get("auth", {})
			updated_at_raw = auth_info.get("updated_at")
			if not updated_at_raw:
				continue
			try:
				updated_at = datetime.fromisoformat(updated_at_raw)
			except ValueError:
				continue
			if updated_at.tzinfo is None:
				updated_at = updated_at.replace(tzinfo=UTC)
			return {
				"log_file": str(log_file),
				"record": record,
				"updated_at": updated_at,
			}
	return None


def get_status_index_record():
	path_value = get_ingestion_status_latest_path()
	if not path_value.exists():
		return None
	try:
		with path_value.open() as infile:
			payload = json.load(infile)
	except Exception:
		return None
	return payload


def main():
	args = parse_args()
	ingestion_cfg = get_ingestion_config()
	max_age_hours = args.max_age_hours
	if max_age_hours is None:
		max_age_hours = float(ingestion_cfg.get("health_max_age_hours", 30))
	max_age_delta = timedelta(hours=max_age_hours)
	txn_max_age_hours = float(ingestion_cfg.get("transaction_health_max_age_hours", 168))

	status_index = get_status_index_record()
	if status_index is not None:
		last_success_raw = status_index.get("last_success_utc")
		try:
			last_success = datetime.fromisoformat(str(last_success_raw).replace("Z", "+00:00"))
			if last_success.tzinfo is None:
				last_success = last_success.replace(tzinfo=UTC)
		except Exception:
			last_success = None
		if last_success is None:
			print("UNHEALTHY: ingestion status index is missing a valid last_success_utc.")
			return 1

		now = datetime.now(UTC)
		age = now - last_success
		ingestion_healthy = age <= max_age_delta
		txn_stream = status_index.get("transaction_stream", {})
		txn_status = txn_stream.get("status", "unknown")
		txn_freshness_hours = txn_stream.get("freshness_hours")
		txn_healthy = True
		if txn_status == "stale":
			txn_healthy = False
		elif isinstance(txn_freshness_hours, (int, float)):
			txn_healthy = float(txn_freshness_hours) <= txn_max_age_hours

		storage_parity = status_index.get("storage_parity", {})
		storage_health = storage_parity.get("status", "unknown")
		storage_healthy = storage_health in {"ok", "unknown"}

		if not ingestion_healthy or not txn_healthy or not storage_healthy:
			print(
				"UNHEALTHY: "
				f"ingestion_health={'healthy' if ingestion_healthy else 'stale'}; "
				f"transaction_health={'healthy' if txn_healthy else txn_status}; "
				f"storage_parity={storage_health}."
			)
			print(
				f"ingestion_age={age}; threshold={max_age_delta}; "
				f"transaction_age_hours={txn_freshness_hours}; "
				f"transaction_threshold_hours={txn_max_age_hours}; "
				f"storage_parity_mismatched={storage_parity.get('mismatched')}; "
				f"storage_parity_missing={storage_parity.get('missing_in_db')}"
			)
			if not ingestion_healthy:
				return 2
			if not txn_healthy:
				return 3
			return 4

		print(
			"HEALTHY: "
			f"ingestion_age={age} (threshold={max_age_delta}); "
			f"transaction_health={txn_status}; "
			f"storage_parity={storage_health}"
		)
		return 0

	latest_success = get_latest_success_record()
	if latest_success is None:
		print("UNHEALTHY: No successful ingestion records found in logs.")
		return 1

	now = datetime.now(UTC)
	age = now - latest_success["updated_at"]
	if age > max_age_delta:
		print(
			"UNHEALTHY: Latest successful ingestion is stale "
			f"({age}). Threshold is {max_age_delta}."
		)
		print(f"Log file: {latest_success['log_file']}")
		return 2

	print(
		"HEALTHY: Latest successful ingestion age is "
		f"{age}, within threshold {max_age_delta}."
	)
	print(f"Log file: {latest_success['log_file']}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
