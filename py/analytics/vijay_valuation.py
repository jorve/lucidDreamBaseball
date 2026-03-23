"""VIJAY Valuation — Risk-Adjusted Reliever Rankings.

Reads the RP preseason priors CSV (which contains component-level projections:
SV, HLD, BS, IP, GS, G, L) and computes each reliever's projected VIJAY score
using the league's canonical formula, then applies a risk adjustment that
penalizes pitchers with high blown-save rates and/or volatile roles.

VIJAY Formula (from league rules):
    VIJAY = (((INN - (INNdGS × GS)) + (S × 3) + (HD × 3)) / 4) - ((BS + RL) × 2)

Where:
    INN     = total innings pitched (IP)
    INNdGS  = innings per game start (IP / GS, or 0 if no starts)
    GS      = games started
    S       = saves
    HD      = holds
    BS      = blown saves
    RL      = reliever losses (estimated as L × relief_fraction)

Risk-Adjusted VIJAY:
    The core risk for VIJAY production is the blown save. A pitcher projecting
    a high BS rate is more likely to exceed their BS projection than to beat it.
    We estimate an "exposure premium" — extra expected blown saves beyond the
    baseline BS rate — and subtract the VIJAY cost of those surprises.

    excess_bs_rate  = max(0, bs_rate - BS_RATE_FLOOR)
    surprise_bs     = excess_bs_rate × SV_projected  (more saves = more exposure)
    risk_penalty    = surprise_bs × 2.0               (each BS costs -2 VIJAY)
    risk_adj_vijay  = projected_vijay - risk_penalty

Role classification (for context, not used in the formula):
    Closer      : SV > 15, GS < 3
    Co-Closer   : SV 8–15, GS < 3  (shared/committee closer situation)
    Elite Setup : HLD > 15, SV < 8
    Multi-Inning: IP > 75, SV + HLD < 15
    Mixed       : everything else

Risk tiers:
    Locked In   : bs_rate < 10%  AND (SV > 15 OR HLD > 15)
    Solid       : bs_rate < 15%  AND (SV > 8  OR HLD > 10)
    Volatile    : bs_rate 15–22% OR role is ambiguous
    High Risk   : bs_rate >= 22%
"""

import csv
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from analytics.io import read_json, write_json
from project_config import (
    get_ingestion_config,
    get_player_eligibility_latest_path,
    get_vijay_valuation_latest_path,
)

UTC = timezone.utc

# ── Tuneable thresholds ──────────────────────────────────────────────── #
BS_RATE_FLOOR = 0.10          # below this, no risk penalty applied
BS_RISK_SENSITIVITY = 2.0     # VIJAY points lost per surprise blown save

CLOSER_SV_THRESHOLD = 15.0
COCLOSER_SV_THRESHOLD = 8.0
ELITE_SETUP_HLD_THRESHOLD = 15.0
MULTI_INNING_IP_THRESHOLD = 75.0

RISK_LOCKED_IN_BS_RATE = 0.10
RISK_SOLID_BS_RATE = 0.15
RISK_HIGH_RISK_BS_RATE = 0.22

# Minimum projected VIJAY to include in output (filters out fringe arms)
MIN_VIJAY_THRESHOLD = 1.5

# How many players to include in output (by raw VIJAY rank)
MAX_CANDIDATES = 150


class VijayValuationError(RuntimeError):
    pass


class VijayValuationBuilder:
    """Build the VIJAY valuation artifact from RP preseason priors."""

    def __init__(self):
        self.ingestion_cfg = get_ingestion_config()

    def build(self, target_date, dry_run=False):
        output_path = get_vijay_valuation_latest_path()

        rp_csv_path = self._resolve_rp_priors_path()
        if rp_csv_path is None or not rp_csv_path.exists():
            return {
                "status": "skipped",
                "reason": "RP_PRIORS_CSV_NOT_FOUND",
                "output_path": output_path,
            }

        # Load roster state for "rostered by" lookup
        rostered_map = self._build_rostered_map()

        # Parse RP CSV and compute valuations
        rows = _parse_rp_csv(rp_csv_path)
        if not rows:
            return {
                "status": "skipped",
                "reason": "RP_PRIORS_CSV_EMPTY",
                "output_path": output_path,
            }

        valuations = []
        for row in rows:
            v = self._compute_valuation(row, rostered_map)
            if v is not None:
                valuations.append(v)

        # Sort by risk-adjusted VIJAY descending, then by raw VIJAY
        valuations.sort(key=lambda x: (-x["risk_adj_vijay"], -x["projected_vijay"]))

        # Assign overall rank
        for rank, v in enumerate(valuations, start=1):
            v["rank"] = rank

        # Split into rostered / free agent sublists for convenience
        rostered = [v for v in valuations if v["roster_status"] != "Free Agent"]
        free_agents = [v for v in valuations if v["roster_status"] == "Free Agent"]

        now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = {
            "schema_version": "1.0",
            "generated_at_utc": now_utc,
            "target_date": target_date.strftime("%Y-%m-%d"),
            "total_relievers": len(valuations),
            "rostered_count": len(rostered),
            "free_agent_count": len(free_agents),
            "thresholds": {
                "bs_rate_floor": BS_RATE_FLOOR,
                "min_vijay_threshold": MIN_VIJAY_THRESHOLD,
                "max_candidates": MAX_CANDIDATES,
            },
            "relievers": valuations,
            "note": (
                "Projected VIJAY computed from rp_priors.csv components "
                "(SV, HLD, BS, IP, GS, L). Risk-adjusted VIJAY penalizes "
                "high blown-save rates by estimating surprise BS exposure "
                "beyond the 10% baseline."
            ),
        }

        if not dry_run:
            write_json(output_path, payload)

        return {
            "status": "ok",
            "output_path": output_path,
            "summary": {
                "total": len(valuations),
                "rostered": len(rostered),
                "free_agents": len(free_agents),
                "top_5": [
                    f"{v['name']} ({v['role_type']}, {v['risk_tier']})"
                    for v in valuations[:5]
                ],
            },
        }

    # ------------------------------------------------------------------ #
    # VIJAY calculation                                                    #
    # ------------------------------------------------------------------ #

    def _compute_valuation(self, row: dict, rostered_map: dict) -> dict | None:
        name = row.get("Name", "").strip()
        if not name:
            return None

        ip    = _flt(row.get("IP", 0))
        gs    = _flt(row.get("GS", 0))
        g     = max(_flt(row.get("G", 1)), 1)
        sv    = _flt(row.get("SV", 0))
        hld   = _flt(row.get("HLD", 0))
        bs    = _flt(row.get("BS", 0))
        w     = _flt(row.get("W", 0))
        l     = _flt(row.get("L", 0))
        era   = _flt(row.get("ERA", 0))
        whip  = _flt(row.get("WHIP", 0))
        vol   = _flt(row.get("Vol", 0))
        player_id = row.get("PlayerId", "").strip()
        mlbam_id  = row.get("MLBAMID", "").strip()
        mlb_team  = row.get("Team", "").strip()

        # Reliever loss estimate: all losses from relief appearances
        relief_fraction = max(0.0, 1.0 - (gs / g)) if g > 0 else 1.0
        rl = l * relief_fraction

        # VIJAY formula
        inndgs = (ip / gs) if gs >= 1.0 else 0.0
        relief_innings = ip - (inndgs * gs)
        vijay = ((relief_innings + sv * 3.0 + hld * 3.0) / 4.0) - ((bs + rl) * 2.0)

        # Filter fringe arms
        if vijay < MIN_VIJAY_THRESHOLD and sv < 5 and hld < 8:
            return None

        # ── Risk metrics ────────────────────────────────────────────── #
        # Blown save rate (BS relative to opportunities)
        opp = sv + hld + bs
        bs_rate = bs / opp if opp > 0 else 0.0

        # Risk-adjusted VIJAY
        excess_bs_rate = max(0.0, bs_rate - BS_RATE_FLOOR)
        surprise_bs = excess_bs_rate * sv           # more save opps = more exposure
        risk_penalty = surprise_bs * BS_RISK_SENSITIVITY
        risk_adj_vijay = vijay - risk_penalty

        # ── Role classification ─────────────────────────────────────── #
        role_type = _classify_role(sv, hld, ip, gs)

        # ── Risk tier ───────────────────────────────────────────────── #
        risk_tier = _classify_risk(bs_rate, sv, hld)

        # ── Roster status ───────────────────────────────────────────── #
        roster_info = self._lookup_roster(name, player_id, rostered_map)

        return {
            "name": name,
            "mlb_team": mlb_team,
            "player_id": player_id,
            "mlbam_id": mlbam_id,
            "role_type": role_type,
            "risk_tier": risk_tier,
            "roster_status": roster_info["status"],
            "rostered_by_team_id": roster_info["team_id"],
            "rostered_by_team_name": roster_info["team_name"],
            # Projected components
            "proj_sv": round(sv, 1),
            "proj_hld": round(hld, 1),
            "proj_bs": round(bs, 1),
            "proj_ip": round(ip, 1),
            "proj_gs": round(gs, 1),
            "proj_rl": round(rl, 2),
            "proj_era": round(era, 2),
            "proj_whip": round(whip, 3),
            # VIJAY outputs
            "projected_vijay": round(vijay, 2),
            "risk_adj_vijay": round(risk_adj_vijay, 2),
            "risk_penalty": round(risk_penalty, 2),
            # Risk detail
            "bs_rate": round(bs_rate, 3),
            "bs_rate_pct": round(bs_rate * 100, 1),
            "excess_bs_rate": round(excess_bs_rate, 3),
            "surprise_bs": round(surprise_bs, 2),
            "vol": round(vol, 2),
            # Rank placeholder (filled after sort)
            "rank": 0,
        }

    # ------------------------------------------------------------------ #
    # Roster lookup                                                        #
    # ------------------------------------------------------------------ #

    def _build_rostered_map(self) -> dict:
        """Build name → {status, team_id, team_name} from roster_state."""
        from project_config import get_roster_state_latest_path
        path = get_roster_state_latest_path()
        if not path.exists():
            return {}

        try:
            payload = read_json(path)
        except Exception:
            return {}

        rostered = {}
        teams = payload.get("teams", [])
        for team in teams:
            team_id = str(team.get("team_id", ""))
            team_name = team.get("team_name", team_id)
            for player in team.get("players", []):
                pname = player.get("player_name", "")
                if pname:
                    key = _normalize_name(pname)
                    rostered[key] = {
                        "status": f"Rostered",
                        "team_id": team_id,
                        "team_name": team_name,
                    }
        return rostered

    def _lookup_roster(self, name: str, player_id: str, rostered_map: dict) -> dict:
        key = _normalize_name(name)
        if key in rostered_map:
            return rostered_map[key]
        return {"status": "Free Agent", "team_id": None, "team_name": None}

    # ------------------------------------------------------------------ #
    # Path resolution                                                      #
    # ------------------------------------------------------------------ #

    def _resolve_rp_priors_path(self) -> Path | None:
        from project_config import BASE_DIR, CURRENT_YEAR

        player_blend = self.ingestion_cfg.get("player_blend", {})
        preseason_csvs = player_blend.get("preseason_csvs", {})
        rp_template = preseason_csvs.get("rp", "data/{year}/preseason/rp_priors.csv")

        # Try configured year first
        rp_path = BASE_DIR / rp_template.replace("{year}", str(CURRENT_YEAR))
        if rp_path.exists():
            return rp_path

        # Scan data/ for the most recent year that has an rp_priors.csv
        # (handles the common case where project_config.json still points at a
        #  prior season while the preseason CSVs are already staged for the new year)
        data_dir = BASE_DIR / "data"
        if data_dir.exists():
            candidates = sorted(
                (d for d in data_dir.iterdir() if d.is_dir() and d.name.isdigit()),
                key=lambda d: int(d.name),
                reverse=True,
            )
            for year_dir in candidates:
                candidate = year_dir / "preseason" / "rp_priors.csv"
                if candidate.exists():
                    return candidate

        return None


# ------------------------------------------------------------------ #
# CSV parsing                                                          #
# ------------------------------------------------------------------ #

def _parse_rp_csv(path: Path) -> list:
    rows = []
    with path.open(encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


# ------------------------------------------------------------------ #
# Classification helpers                                               #
# ------------------------------------------------------------------ #

def _classify_role(sv: float, hld: float, ip: float, gs: float) -> str:
    if sv >= CLOSER_SV_THRESHOLD and gs < 3:
        return "Closer"
    if sv >= COCLOSER_SV_THRESHOLD and gs < 3:
        return "Co-Closer"
    if hld >= ELITE_SETUP_HLD_THRESHOLD and sv < 8:
        return "Elite Setup"
    if ip >= MULTI_INNING_IP_THRESHOLD and (sv + hld) < 15:
        return "Multi-Inning"
    if hld >= 8:
        return "Setup"
    return "Mixed"


def _classify_risk(bs_rate: float, sv: float, hld: float) -> str:
    if bs_rate < RISK_LOCKED_IN_BS_RATE and (sv > CLOSER_SV_THRESHOLD or hld > ELITE_SETUP_HLD_THRESHOLD):
        return "Locked In"
    if bs_rate < RISK_SOLID_BS_RATE and (sv > COCLOSER_SV_THRESHOLD or hld > 10):
        return "Solid"
    if bs_rate >= RISK_HIGH_RISK_BS_RATE:
        return "High Risk"
    return "Volatile"


# ------------------------------------------------------------------ #
# Utilities                                                            #
# ------------------------------------------------------------------ #

def _flt(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _normalize_name(name: str) -> str:
    """Lowercase, strip accents, strip punctuation for fuzzy matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_name.lower().strip()
