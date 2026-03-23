"""Standalone runner for the VIJAY Valuation module.

Usage:
    python py/vijay_valuation.py
    python py/vijay_valuation.py --date 2026-04-15
    python py/vijay_valuation.py --dry-run
    python py/vijay_valuation.py --top 20
"""

import argparse
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analytics.vijay_valuation import VijayValuationBuilder


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the LDB VIJAY Valuation (risk-adjusted reliever rankings)."
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
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="Print top N relievers to console (default: 15).",
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

    print(f"VIJAY Valuation — target date: {target_date}")
    if args.dry_run:
        print("(dry-run mode: no files will be written)")

    builder = VijayValuationBuilder()
    result = builder.build(target_date=target_date, dry_run=args.dry_run)

    status = result.get("status", "unknown")
    if status == "ok":
        summary = result.get("summary", {})
        print(f"  status: ok")
        print(f"  total relievers ranked: {summary.get('total', '?')}")
        print(f"  rostered: {summary.get('rostered', '?')} | free agents: {summary.get('free_agents', '?')}")
        if not args.dry_run:
            print(f"  output: {result.get('output_path', '?')}")

        top5 = summary.get("top_5", [])
        if top5:
            print(f"\n  Top 5 by risk-adjusted VIJAY:")
            for i, label in enumerate(top5, 1):
                print(f"    {i}. {label}")

        # Print table from the actual payload if available and not dry-run
        if not args.dry_run:
            import json
            output_path = result.get("output_path")
            if output_path:
                with open(output_path) as f:
                    payload = json.load(f)
                relievers = payload.get("relievers", [])[:args.top]
                if relievers:
                    print(f"\n  {'Rk':>3}  {'Name':<22} {'Role':<14} {'Risk':<12} {'SV':>5} {'HLD':>5} {'BS':>5} {'BS%':>6} {'VIJAY':>7} {'Adj':>7} {'Status'}")
                    print("  " + "-" * 100)
                    for r in relievers:
                        status_label = r.get("rostered_by_team_name") or r.get("roster_status", "FA")
                        print(
                            f"  {r['rank']:>3}  {r['name']:<22} {r['role_type']:<14} {r['risk_tier']:<12} "
                            f"{r['proj_sv']:>5.1f} {r['proj_hld']:>5.1f} {r['proj_bs']:>5.1f} "
                            f"{r['bs_rate_pct']:>5.1f}% {r['projected_vijay']:>7.2f} "
                            f"{r['risk_adj_vijay']:>7.2f}  {status_label}"
                        )
    else:
        print(f"  status: {status} (reason: {result.get('reason', '?')})")

    return 0 if status in ("ok", "skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())
