import argparse
import datetime
import json
import subprocess
import sys
from pathlib import Path

from project_config import BASE_DIR, PATHS, get_recompute_request_latest_path, get_required_input_path


PY_DIR = BASE_DIR / "py"

PIPELINE_STEPS = [
	"scoring",
	"teams",
	"xwins",
	"xmatchups",
	"weekly_xmatchup_preview",
	"week_preview",
	"batter_WAR",
	"schedule_strength",
	"vijay_valuation",
]

REQUIRED_INPUTS_BY_STEP = {
	"xwins": ["key_variables.json", "team_CLAPS.json", "ldbCLAP.json", "team_scores.json"],
	"xmatchups": ["key_variables.json", "team_CLAPS.json", "ldbCLAP.json"],
	"weekly_xmatchup_preview": ["key_variables.json", "ldb_xmatchups.json", "schedule.json"],
	"week_preview": ["key_variables.json", "week_matchups.json", "ldb_xmatchups.json"],
	"batter_WAR": ["key_variables.json", "ldbCLAP.json", "replacementLevel.json"],
	"schedule_strength": ["schedule.json"],
	# vijay_valuation reads the RP priors CSV directly — no JSON input requirement
}


def parse_args():
	parser = argparse.ArgumentParser(
		description="Run Lucid Dream Baseball analysis pipeline."
	)
	parser.add_argument(
		"--from",
		dest="from_step",
		choices=PIPELINE_STEPS,
		help="Run pipeline starting from this step.",
	)
	parser.add_argument(
		"--only",
		nargs="+",
		choices=PIPELINE_STEPS,
		help="Run only specific step(s) in provided order.",
	)
	parser.add_argument(
		"--list",
		action="store_true",
		help="List available pipeline steps and exit.",
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Print planned command execution without running scripts.",
	)
	parser.add_argument(
		"--ingest-first",
		action="store_true",
		help="Run ingestion workflow before analysis steps.",
	)
	parser.add_argument(
		"--ingest-date",
		help="Target ingestion date in YYYY-MM-DD format.",
	)
	parser.add_argument(
		"--skip-input-check",
		action="store_true",
		help="Skip required input existence and staleness checks.",
	)
	parser.add_argument(
		"--max-input-age-hours",
		type=float,
		default=36.0,
		help="Maximum allowed age for required JSON inputs (default: 36).",
	)
	return parser.parse_args()


def resolve_run_plan(args):
	if args.only and args.from_step:
		raise ValueError("Use either --only or --from, not both.")
	if args.only:
		return args.only
	if args.from_step:
		start_index = PIPELINE_STEPS.index(args.from_step)
		return PIPELINE_STEPS[start_index:]
	return PIPELINE_STEPS


def run_step(step_name, dry_run=False):
	script_path = PY_DIR / f"{step_name}.py"
	command = [sys.executable, str(script_path)]
	print(f"-> {step_name}: {' '.join(command)}")
	if dry_run:
		return
	subprocess.run(command, cwd=BASE_DIR, check=True)


def run_ingestion(date_value=None, dry_run=False):
	script_path = PY_DIR / "run_ingestion.py"
	command = [sys.executable, str(script_path)]
	if date_value:
		command.extend(["--date", date_value])
	if dry_run:
		command.append("--dry-run")
	print(f"-> ingestion: {' '.join(command)}")
	if dry_run:
		return
	subprocess.run(command, cwd=BASE_DIR, check=True)


def log_recompute_intent():
	path_value = get_recompute_request_latest_path()
	if not path_value.exists():
		return
	try:
		with path_value.open() as infile:
			payload = json.load(infile)
	except Exception:
		return
	triggered = bool(payload.get("triggered"))
	scope = payload.get("recommended_scope", "unknown")
	teams = payload.get("affected_team_ids", [])
	players = payload.get("affected_player_ids", [])
	reason_codes = payload.get("reason_codes", [])
	print(
		"-> recompute intent: "
		f"triggered={triggered} "
		f"scope={scope} "
		f"teams={len(teams)} "
		f"players={len(players)} "
		f"reasons={','.join(reason_codes) if reason_codes else 'none'}"
	)


def get_required_inputs_for_plan(steps_to_run):
	required_inputs = set()
	for step in steps_to_run:
		required_inputs.update(REQUIRED_INPUTS_BY_STEP.get(step, []))
	return sorted(required_inputs)


def validate_required_inputs(steps_to_run, max_input_age_hours):
	now = datetime.datetime.now()
	max_age = datetime.timedelta(hours=max_input_age_hours)
	required_inputs = get_required_inputs_for_plan(steps_to_run)

	for filename in required_inputs:
		path_value = get_required_input_path(filename)
		file_age = now - datetime.datetime.fromtimestamp(path_value.stat().st_mtime)
		if file_age > max_age:
			raise RuntimeError(
				f"Required input is stale ({file_age}) for {filename}. "
				"Run ingestion first or increase --max-input-age-hours."
			)


def main():
	args = parse_args()

	if args.list:
		print("Pipeline steps:")
		for step in PIPELINE_STEPS:
			print(f"- {step}")
		return 0

	try:
		steps_to_run = resolve_run_plan(args)
	except ValueError as error:
		print(f"Error: {error}")
		return 2

	print("Running pipeline steps:")
	for step in steps_to_run:
		print(f"- {step}")

	if args.ingest_first:
		try:
			run_ingestion(date_value=args.ingest_date, dry_run=args.dry_run)
			if not args.dry_run:
				log_recompute_intent()
		except subprocess.CalledProcessError as error:
			print(f"Ingestion failed (exit code {error.returncode})")
			return error.returncode

	if not args.skip_input_check and not args.dry_run:
		try:
			validate_required_inputs(steps_to_run, args.max_input_age_hours)
		except Exception as error:
			print(f"Input validation failed: {error}")
			return 3

	for step in steps_to_run:
		try:
			run_step(step, dry_run=args.dry_run)
		except subprocess.CalledProcessError as error:
			print(f"Step failed: {step} (exit code {error.returncode})")
			return error.returncode

	print("Pipeline completed successfully.")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
