import argparse
import subprocess
import sys
from datetime import datetime

from analytics.weekly_calibration import WeeklyCalibrationBuilder, WeeklyCalibrationError
from analytics.weekly_digest import WeeklyDigestBuilder, WeeklyDigestError
from analytics.weekly_email import WeeklyEmailBuilder, WeeklyEmailError


def parse_args():
	parser = argparse.ArgumentParser(description="Generate weekly digest/email artifacts.")
	parser.add_argument("--date", help="Reference date in YYYY-MM-DD format. Default is today.")
	parser.add_argument("--dry-run", action="store_true", help="Plan actions without writing outputs.")
	parser.add_argument(
		"--ingest-first",
		action="store_true",
		help="Optionally run ingestion first before generating weekly artifacts.",
	)
	return parser.parse_args()


def resolve_target_date(date_arg):
	if date_arg:
		return datetime.strptime(date_arg, "%Y-%m-%d")
	return datetime.now()


def maybe_run_ingestion(target_date, dry_run=False):
	if dry_run:
		return 0
	command = [
		sys.executable,
		"py/run_ingestion.py",
		"--date",
		target_date.strftime("%Y-%m-%d"),
	]
	result = subprocess.run(command, check=False)
	return int(result.returncode)


def main():
	args = parse_args()
	target_date = resolve_target_date(args.date)

	if args.ingest_first:
		exit_code = maybe_run_ingestion(target_date=target_date, dry_run=args.dry_run)
		if exit_code != 0:
			print(f"Weekly generation aborted: ingestion failed with exit code {exit_code}.")
			return exit_code

	try:
		digest_builder = WeeklyDigestBuilder()
		digest_result = digest_builder.build(target_date=target_date, dry_run=args.dry_run)
		print(f"weekly_digest={digest_result['status']}")

		email_builder = WeeklyEmailBuilder()
		email_result = email_builder.build(target_date=target_date, dry_run=args.dry_run)
		print(f"weekly_email={email_result['status']}")

		calibration_builder = WeeklyCalibrationBuilder()
		calibration_result = calibration_builder.build(target_date=target_date, dry_run=args.dry_run)
		print(f"weekly_calibration={calibration_result['status']}")
	except (WeeklyDigestError, WeeklyEmailError, WeeklyCalibrationError, ValueError) as error:
		print(f"Weekly generation failed: {error}")
		return 1

	print("Weekly generation completed.")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
