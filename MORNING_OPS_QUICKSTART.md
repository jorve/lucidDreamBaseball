# Morning Ops Quickstart

Five checks to confirm the app is healthy each morning.

## 1) Health Check

Run:

```powershell
python py/check_ingestion_health.py
```

Pass:
- returns healthy status

## 2) Status Index Scan

Open:

- `json/ingestion_status_latest.json`

Check:
- `status` is `ok`
- `codes` does not contain new failure/stale signals
- no unexpected failed resources

## 3) Overnight Artifacts Updated

Confirm these files were refreshed:

- `json/player_projection_weekly_latest.json`
- `json/view_league_weekly_latest.json`
- `json/free_agent_candidates_latest.json`
- `json/weekly_digest_latest.txt`
- `json/artifact_history_latest.json`

## 4) Free-Agent / Replacement Sanity

Open:

- `json/free_agent_candidates_latest.json`

Check:
- `summary.candidate_count` looks reasonable
- `replacement_suggestions.summary.suggestions_count` is not unexpectedly zero
- top suggestions are plausible (no obvious bad IDs/names)

## 5) Weekly Calibration Pulse (if available)

Open:

- `json/weekly_calibration_latest.json`

Check:
- artifact exists or is intentionally skipped due to early-season data
- `metrics.overall.mae_points` is present when populated
- `metrics.trend.status` is not persistently `degrading`

---

## If Any Check Fails

1. Rerun ingestion once:

```powershell
python py/run_ingestion.py --force-auth-refresh
```

2. If lock error appears:
- confirm no active ingestion process
- remove `.state/ingestion_run.lock`
- rerun ingestion

3. Re-check health/status artifacts before making any config changes.
