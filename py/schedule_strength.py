"""Standalone runner for the Schedule Strength Simulator.

Usage:
    python py/schedule_strength.py
    python py/schedule_strength.py --date 2026-04-15
    python py/schedule_strength.py --dry-run
"""

import argparse
import datetime
import sys
from pathlib import Path

# Ensure project root is on path when called as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics.schedule_strength import ScheduleStrengthBuilder


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the LDB Schedule Strength Simulator."
    )
    parser.add_argument(
        "--date",
        help="Target date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute output but do not write artifact to disk.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.date:
        try:
            target_date = datetime.date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD.")
            return 1
    else:
        target_date = datetime.date.today()

    print(f"Schedule Strength Simulator — target date: {target_date}")
    if args.dry_run:
        print("(dry-run mode: no files will be written)")

    builder = ScheduleStrengthBuilder()
    result = builder.build(target_date=target_date, dry_run=args.dry_run)

    status = result.get("status", "unknown")
    if status == "ok":
        summary = result.get("summary", {})
        print(f"  status: ok")
        print(f"  teams: {summary.get('teams', '?')}")
        print(f"  total periods: {summary.get('total_periods', '?')}")
        print(f"  remaining periods: {summary.get('remaining_periods', '?')}")
        print(f"  win probability source: {summary.get('win_probability_source', '?')}")
        if not args.dry_run:
            print(f"  output: {result.get('output_path', '?')}")
    else:
        print(f"  status: {status} (reason: {result.get('reason', '?')})")

    return 0 if status in ("ok", "skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())
