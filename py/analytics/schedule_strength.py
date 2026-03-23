"""Schedule Strength Simulator.

Reads the full-season schedule and team win-probability data to produce:
  - Per-team remaining schedule difficulty (strength-of-schedule)
  - Expected wins from remaining matchups
  - Projected final standings (regular season)
  - Division standing projections

Win probability resolution order (best → fallback):
  1. matchup_expectations_latest.json  (CLAP v2 — team-ID-keyed)
  2. ldb_xmatchups.json                (classic xmatchups — long_abbr-keyed)
  3. 0.5 neutral                       (no data available)
"""

import math
from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
    get_ingestion_config,
    get_json_output_path,
    get_matchup_expectations_latest_path,
    get_schedule_strength_latest_path,
)

UTC = timezone.utc

# Categories used to compute overall matchup win probability via DP
SCORE_CATEGORIES = [
    "HR", "R", "OBP", "OPS", "aRBI", "aSB",
    "K", "HRA", "aWHIP", "VIJAY", "ERA", "MGS",
]
TOTAL_CATEGORIES = len(SCORE_CATEGORIES)
WIN_THRESHOLD = math.ceil(TOTAL_CATEGORIES / 2) + 1  # 7 of 12

SOS_EASY_THRESHOLD = 0.47
SOS_HARD_THRESHOLD = 0.53


class ScheduleStrengthError(RuntimeError):
    pass


class ScheduleStrengthBuilder:
    """Simulate remaining-schedule expected wins and project final standings."""

    def __init__(self):
        self.ingestion_cfg = get_ingestion_config()

    # ------------------------------------------------------------------ #
    # Public entrypoint                                                    #
    # ------------------------------------------------------------------ #

    def build(self, target_date, dry_run=False):
        output_path = get_schedule_strength_latest_path()

        schedule_path = _resolve_schedule_path()
        if schedule_path is None or not schedule_path.exists():
            return {
                "status": "skipped",
                "reason": "SCHEDULE_NOT_FOUND",
                "output_path": output_path,
            }

        schedule_raw = read_json(schedule_path)
        periods = _extract_periods(schedule_raw)
        if not periods:
            return {
                "status": "skipped",
                "reason": "SCHEDULE_EMPTY",
                "output_path": output_path,
            }

        # Build team registry from schedule data
        team_registry = _build_team_registry(periods)
        if not team_registry:
            return {
                "status": "skipped",
                "reason": "NO_TEAMS_IN_SCHEDULE",
                "output_path": output_path,
            }

        # Load current records from team weekly totals (preferred) or schedule
        current_records = _load_current_records(team_registry)

        # Determine current week (last completed period)
        current_period_id = _current_period_id(periods, target_date)

        # Build win-probability resolver
        prob_resolver = _WinProbabilityResolver(team_registry)

        # Compute per-team metrics
        team_metrics = self._compute_team_metrics(
            periods=periods,
            team_registry=team_registry,
            current_records=current_records,
            current_period_id=current_period_id,
            prob_resolver=prob_resolver,
        )

        # Build division projections
        division_projections = _build_division_projections(team_metrics, team_registry)

        now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        total_periods = len(periods)
        remaining_periods = sum(
            1 for p in periods if int(p.get("id", 0)) > current_period_id
        )

        payload = {
            "schema_version": "1.0",
            "generated_at_utc": now_utc,
            "target_date": target_date.strftime("%Y-%m-%d"),
            "current_period_id": current_period_id,
            "total_periods": total_periods,
            "remaining_periods": remaining_periods,
            "win_probability_source": prob_resolver.source_label,
            "teams": team_metrics,
            "division_projections": division_projections,
            "note": (
                "Projected records assume remaining-schedule win probabilities "
                "are accurate. Early-season projections carry high variance."
            ),
        }

        if not dry_run:
            write_json(output_path, payload)

        return {
            "status": "ok",
            "output_path": output_path,
            "summary": {
                "teams": len(team_metrics),
                "total_periods": total_periods,
                "remaining_periods": remaining_periods,
                "win_probability_source": prob_resolver.source_label,
            },
        }

    # ------------------------------------------------------------------ #
    # Core simulation                                                      #
    # ------------------------------------------------------------------ #

    def _compute_team_metrics(
        self,
        periods,
        team_registry,
        current_records,
        current_period_id,
        prob_resolver,
    ):
        """For each team, compute remaining schedule strength and projected record."""
        # Gather all matchups split by past / remaining
        past_matchups_by_team = {tid: [] for tid in team_registry}
        remaining_matchups_by_team = {tid: [] for tid in team_registry}

        for period in periods:
            period_id = int(period.get("id", 0))
            is_remaining = period_id > current_period_id
            label = period.get("label", f"Period {period_id}")
            start_date = period.get("start", "")
            end_date = period.get("end", "")

            for matchup in period.get("matchups", []):
                away = matchup.get("away_team", {})
                home = matchup.get("home_team", {})
                away_id = str(away.get("id", ""))
                home_id = str(home.get("id", ""))

                if not away_id or not home_id:
                    continue
                if away_id not in team_registry or home_id not in team_registry:
                    continue

                away_win_prob = prob_resolver.get_win_prob(away_id, home_id)
                home_win_prob = 1.0 - away_win_prob

                away_entry = {
                    "period_id": period_id,
                    "period_label": label,
                    "start_date": start_date,
                    "end_date": end_date,
                    "opponent_id": home_id,
                    "opponent_abbr": team_registry[home_id]["long_abbr"],
                    "opponent_name": team_registry[home_id]["display_name"],
                    "is_home": False,
                    "win_probability": round(away_win_prob, 4),
                }
                home_entry = {
                    "period_id": period_id,
                    "period_label": label,
                    "start_date": start_date,
                    "end_date": end_date,
                    "opponent_id": away_id,
                    "opponent_abbr": team_registry[away_id]["long_abbr"],
                    "opponent_name": team_registry[away_id]["display_name"],
                    "is_home": True,
                    "win_probability": round(home_win_prob, 4),
                }

                dest = remaining_matchups_by_team if is_remaining else past_matchups_by_team
                dest[away_id].append(away_entry)
                dest[home_id].append(home_entry)

        result = {}
        for team_id, info in team_registry.items():
            record = current_records.get(team_id, {"w": 0, "l": 0, "t": 0})
            current_w = record.get("w", 0)
            current_l = record.get("l", 0)
            current_t = record.get("t", 0)

            remaining = remaining_matchups_by_team[team_id]
            past = past_matchups_by_team[team_id]

            expected_remaining_wins = sum(m["win_probability"] for m in remaining)
            expected_remaining_losses = len(remaining) - expected_remaining_wins

            sos = (
                (sum(m["win_probability"] for m in remaining) / len(remaining))
                if remaining else 0.5
            )
            # SOS from opponent perspective: avg prob that opponent beats YOU
            sos_opponent = (
                sum(1.0 - m["win_probability"] for m in remaining) / len(remaining)
                if remaining else 0.5
            )

            if sos_opponent <= SOS_EASY_THRESHOLD:
                sos_label = "Easy"
            elif sos_opponent >= SOS_HARD_THRESHOLD:
                sos_label = "Hard"
            else:
                sos_label = "Average"

            projected_w = current_w + expected_remaining_wins
            projected_l = current_l + expected_remaining_losses
            total_games = len(past) + len(remaining)

            result[team_id] = {
                "team_id": team_id,
                "display_name": info["display_name"],
                "long_abbr": info["long_abbr"],
                "division": info["division"],
                "current_record": record,
                "games_played": len(past),
                "games_remaining": len(remaining),
                "total_games": total_games,
                "remaining_matchups": remaining,
                "expected_remaining_wins": round(expected_remaining_wins, 2),
                "expected_remaining_losses": round(expected_remaining_losses, 2),
                "projected_wins": round(projected_w, 2),
                "projected_losses": round(projected_l, 2),
                "projected_win_pct": round(
                    projected_w / max(projected_w + projected_l, 1), 4
                ),
                "sos": round(sos_opponent, 4),
                "sos_label": sos_label,
            }

        return result


# ------------------------------------------------------------------ #
# Win-probability resolver                                             #
# ------------------------------------------------------------------ #

class _WinProbabilityResolver:
    """Resolves win probability for a given team-vs-team matchup.

    Tries sources in order:
      1. matchup_expectations_latest.json (CLAP v2, most accurate)
      2. ldb_xmatchups.json (classic, long_abbr keyed)
      3. Neutral 0.5
    """

    def __init__(self, team_registry):
        self.team_registry = team_registry
        self._clap_v2 = {}        # team_id → team_id → float
        self._xmatchups = {}      # long_abbr → long_abbr → float (category-level)
        self.source_label = "neutral_0.5"
        self._load_sources()

    def _load_sources(self):
        # Source 1: CLAP v2 matchup expectations
        expectations_path = get_matchup_expectations_latest_path()
        if expectations_path.exists():
            try:
                payload = read_json(expectations_path)
                for matchup in payload.get("matchups", []):
                    aid = str(matchup.get("away_team_id", ""))
                    hid = str(matchup.get("home_team_id", ""))
                    prob = matchup.get("away_win_probability")
                    if aid and hid and prob is not None:
                        self._clap_v2.setdefault(aid, {})[hid] = float(prob)
                        self._clap_v2.setdefault(hid, {})[aid] = 1.0 - float(prob)
                if self._clap_v2:
                    self.source_label = "clap_v2_matchup_expectations"
            except Exception:
                pass

        # Source 2: classic xmatchups (category-level → compute overall)
        if not self._clap_v2:
            xmatchups_path = _resolve_xmatchups_path()
            if xmatchups_path and xmatchups_path.exists():
                try:
                    raw = read_json(xmatchups_path)
                    self._xmatchups = raw
                    if self._xmatchups:
                        self.source_label = "xmatchups_category_dp"
                except Exception:
                    pass

    def get_win_prob(self, team_id: str, opponent_id: str) -> float:
        """Return P(team wins matchup vs opponent). Falls back gracefully."""
        # Try CLAP v2 first
        if self._clap_v2:
            p = self._clap_v2.get(team_id, {}).get(opponent_id)
            if p is not None:
                return float(p)

        # Try xmatchups (long_abbr lookup)
        if self._xmatchups:
            abbr = self.team_registry.get(team_id, {}).get("long_abbr", "")
            opp_abbr = self.team_registry.get(opponent_id, {}).get("long_abbr", "")
            category_probs = self._xmatchups.get(abbr, {}).get(opp_abbr)
            if category_probs and isinstance(category_probs, dict):
                probs = [
                    float(category_probs[cat])
                    for cat in SCORE_CATEGORIES
                    if cat in category_probs
                ]
                if probs:
                    return _dp_win_probability(probs, WIN_THRESHOLD)

        # Fallback: neutral
        return 0.5


# ------------------------------------------------------------------ #
# Probability calculation                                              #
# ------------------------------------------------------------------ #

def _dp_win_probability(category_probs: list, win_threshold: int) -> float:
    """Compute P(win >= win_threshold categories) given per-category win probs.

    Uses dynamic programming (exact, O(n^2) in n categories).
    Assumes category outcomes are independent — same assumption as xmatchups model.
    """
    n = len(category_probs)
    # dp[k] = probability of winning exactly k categories so far
    dp = [0.0] * (n + 1)
    dp[0] = 1.0

    for p in category_probs:
        new_dp = [0.0] * (n + 1)
        for k in range(n + 1):
            if dp[k] == 0.0:
                continue
            # Win this category
            if k + 1 <= n:
                new_dp[k + 1] += dp[k] * p
            # Lose this category
            new_dp[k] += dp[k] * (1.0 - p)
        dp = new_dp

    return sum(dp[k] for k in range(win_threshold, n + 1))


# ------------------------------------------------------------------ #
# Division projections                                                 #
# ------------------------------------------------------------------ #

def _build_division_projections(team_metrics: dict, team_registry: dict) -> dict:
    """Group teams by division, sort by projected wins descending."""
    divisions = {}
    for team_id, metrics in team_metrics.items():
        div = metrics["division"]
        divisions.setdefault(div, [])
        divisions[div].append({
            "team_id": team_id,
            "long_abbr": metrics["long_abbr"],
            "display_name": metrics["display_name"],
            "current_record": metrics["current_record"],
            "projected_wins": metrics["projected_wins"],
            "projected_losses": metrics["projected_losses"],
            "projected_win_pct": metrics["projected_win_pct"],
            "sos_label": metrics["sos_label"],
            "projected_division_rank": None,  # filled below
        })

    for div, teams in divisions.items():
        teams.sort(key=lambda t: t["projected_wins"], reverse=True)
        for rank, team in enumerate(teams, start=1):
            team["projected_division_rank"] = rank

    return divisions


# ------------------------------------------------------------------ #
# Schedule / data helpers                                              #
# ------------------------------------------------------------------ #

def _extract_periods(schedule_raw: dict) -> list:
    """Pull periods list from CBS schedule JSON."""
    try:
        return schedule_raw["body"]["schedule"]["periods"]
    except (KeyError, TypeError):
        return []


def _build_team_registry(periods: list) -> dict:
    """Build id → {display_name, long_abbr, division} from schedule data."""
    registry = {}
    for period in periods:
        for matchup in period.get("matchups", []):
            for side in ("away_team", "home_team"):
                team = matchup.get(side, {})
                team_id = str(team.get("id", ""))
                if not team_id:
                    continue
                registry[team_id] = {
                    "team_id": team_id,
                    "display_name": team.get("name", team.get("short_name", team_id)),
                    "long_abbr": team.get("long_abbr", team.get("abbr", team_id)),
                    "short_name": team.get("short_name", ""),
                    "division": team.get("division", "Unknown"),
                    "logo": team.get("logo", ""),
                }
    return registry


def _load_current_records(team_registry: dict) -> dict:
    """Load current W-L records from team_weekly_totals or parse from schedule."""
    from project_config import get_team_weekly_totals_latest_path

    records = {}
    path = get_team_weekly_totals_latest_path()
    if path.exists():
        try:
            payload = read_json(path)
            for matchup in payload.get("period", {}).get("matchups", []):
                for side in ("away_team", "home_team"):
                    team = matchup.get(side, {})
                    team_id = str(team.get("id", ""))
                    record_str = team.get("record", "0-0-0")
                    records[team_id] = _parse_record(record_str)
        except Exception:
            pass

    # Fill any missing teams with 0-0-0
    for team_id in team_registry:
        if team_id not in records:
            records[team_id] = {"w": 0, "l": 0, "t": 0}
    return records


def _parse_record(record_str: str) -> dict:
    """Parse '7-5-0' or '7-5' into {w, l, t}."""
    parts = str(record_str).split("-")
    try:
        w = int(parts[0]) if len(parts) > 0 else 0
        l = int(parts[1]) if len(parts) > 1 else 0
        t = int(parts[2]) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        w, l, t = 0, 0, 0
    return {"w": w, "l": l, "t": t}


def _current_period_id(periods: list, target_date) -> int:
    """Determine the most recently completed period based on target_date."""
    from datetime import date

    if isinstance(target_date, date):
        td = target_date
    else:
        td = target_date.date() if hasattr(target_date, "date") else target_date

    current_id = 0
    for period in periods:
        try:
            end_str = period.get("end", "")
            # CBS format: "3/29/26" or "3/29/2026"
            end_date = _parse_cbs_date(end_str)
            if end_date and end_date <= td:
                pid = int(period.get("id", 0))
                if pid > current_id:
                    current_id = pid
        except Exception:
            pass

    return current_id


def _parse_cbs_date(date_str: str):
    """Parse CBS date strings like '3/29/26' or '3/29/2026'."""
    from datetime import date

    if not date_str:
        return None
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def _resolve_schedule_path():
    """Return path to schedule.json using project config resolver."""
    from project_config import get_schedule_path
    try:
        return get_schedule_path()
    except Exception:
        return None


def _resolve_xmatchups_path():
    """Return path to ldb_xmatchups.json."""
    from project_config import get_json_output_path
    p = get_json_output_path("ldb_xmatchups.json")
    if p.exists():
        return p
    return None
