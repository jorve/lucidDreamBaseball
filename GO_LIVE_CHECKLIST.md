# Go-Live Checklist

Use this checklist to launch nightly ingestion safely and validate model behavior during opening weeks.

## 1) Preflight Setup (One Time)

- [ ] Install dependencies:
  - `python -m pip install -r requirements.txt`
  - `python -m playwright install`
- [ ] Confirm credentials are set in environment:
  - `CBS_USERNAME`
  - `CBS_PASSWORD`
  - optional: `CBS_API_TOKEN`
- [ ] Confirm season-specific preseason files exist for current year:
  - `data/{year}/preseason/batter_priors.csv`
  - `data/{year}/preseason/sp_priors.csv`
  - `data/{year}/preseason/rp_priors.csv`
  - `data/{year}/preseason/CBS_batter_elig.csv`
  - `data/{year}/preseason/CBS_SP_elig.csv`
  - `data/{year}/preseason/CBS_RP_elig.csv`
- [ ] Verify `project_config.json` key settings:
  - `current_year`
  - `ingestion.cbs.league_id`
  - `ingestion.projections.scoring_week_end_weekday`
  - `ingestion.request_policy.*`
  - `ingestion.run_lock.*`

## 2) Dry-Run + First Live Validation

- [ ] Run dry-run:
  - `python py/run_ingestion.py --dry-run --force-auth-refresh`
- [ ] Run first live ingestion:
  - `python py/run_ingestion.py --force-auth-refresh`
- [ ] Confirm success output and no lock conflict.
- [ ] Check health:
  - `python py/check_ingestion_health.py`
- [ ] Check status index:
  - `json/ingestion_status_latest.json`
  - confirm no unexpected `codes`

## 3) Artifact Verification (Launch Night)

- [ ] Core ingestion artifacts present:
  - `json/transactions_latest.json`
  - `json/roster_state_latest.json`
  - `json/recompute_request_latest.json`
  - `json/ingestion_status_latest.json`
- [ ] Projection artifacts present:
  - `json/player_projection_deltas_latest.json`
  - `json/player_projection_daily_latest.json`
  - `json/player_projection_weekly_latest.json`
- [ ] Decision artifacts present:
  - `json/view_league_weekly_latest.json`
  - `json/view_gm_weekly_latest.json`
  - `json/free_agent_candidates_latest.json`
  - `json/weekly_digest_latest.json`
  - `json/weekly_email_payload_latest.json`
- [ ] Reliability artifacts present:
  - `json/weekly_calibration_latest.json` (may be skipped until enough history exists)
  - `json/artifact_history_latest.json`

## 4) Scheduler Setup (Nightly)

- [ ] Configure one nightly task (Windows Task Scheduler).
- [ ] Use a single command, no parallel runs:
  - `python py/run_ingestion.py`
- [ ] Recommended schedule: one off-peak run per night.
- [ ] Disable Task Scheduler overlapping instances (in addition to app lock).
- [ ] Keep `ingestion.run_lock.enabled=true`.

### Windows Task Scheduler (Copy/Paste)

Safe default nightly run time: `03:30` local time.

If Python is on PATH:

```powershell
schtasks /Create /TN "LDB Nightly Ingestion" /SC DAILY /ST 03:30 /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd 'C:\Users\J0RV3\Documents\Development\lucidDreamBaseball'; python py/run_ingestion.py\"" /F
```

If Python is NOT on PATH, replace `python` with full path (example):

```powershell
schtasks /Create /TN "LDB Nightly Ingestion" /SC DAILY /ST 03:30 /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd 'C:\Users\J0RV3\Documents\Development\lucidDreamBaseball'; 'C:\Python313\python.exe' py/run_ingestion.py\"" /F
```

Recommended follow-up commands:

```powershell
schtasks /Query /TN "LDB Nightly Ingestion" /V /FO LIST
schtasks /Run /TN "LDB Nightly Ingestion"
```

### Monday Morning Weekly Generation Task

This task generates weekly digest/email artifacts from current snapshots (no sending).

```powershell
schtasks /Create /TN "LDB Weekly Email Generation" /SC WEEKLY /D MON /ST 08:15 /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd 'C:\Users\J0RV3\Documents\Development\lucidDreamBaseball'; python py/run_weekly_email.py\"" /F
```

If you want Monday to force a fresh ingestion first:

```powershell
schtasks /Create /TN "LDB Weekly Email Generation" /SC WEEKLY /D MON /ST 08:15 /TR "powershell -NoProfile -ExecutionPolicy Bypass -Command \"cd 'C:\Users\J0RV3\Documents\Development\lucidDreamBaseball'; python py/run_weekly_email.py --ingest-first\"" /F
```

## 5) First 2 Weeks Monitoring Plan

- [ ] Daily check `json/ingestion_status_latest.json` for:
  - resource failures
  - stale health codes
  - repeated optional endpoint failures
- [ ] Daily check `json/free_agent_candidates_latest.json`:
  - confirm candidate list and replacement suggestions look reasonable
- [ ] Weekly check `json/weekly_calibration_latest.json`:
  - `metrics.overall.mae_points`
  - `metrics.trend.status`
- [ ] Weekly check `json/view_league_weekly_latest.json`:
  - `weekly_summary.overall_performance_counts`
  - category movers under `weekly_summary.category_summary`

## 6) Safety / Rollback

- [ ] If ingestion fails repeatedly:
  - run once manually with `--force-auth-refresh`
  - inspect `json/ingestion_status_latest.json` + logs
- [ ] If lock gets stuck:
  - verify no ingestion process is running
  - remove `.state/ingestion_run.lock`
  - rerun ingestion
- [ ] If CBS endpoints start erroring:
  - keep optional resources optional
  - validate core resources (`live_scoring`, `schedule`, `rosters`) first
  - avoid increasing request rate

## 7) Ready-for-Season Exit Criteria

- [ ] 5+ consecutive successful nightly runs
- [ ] No recurring stale health codes
- [ ] Weekly calibration artifact producing stable metrics
- [ ] Free-agent suggestions and weekly digest outputs are interpretable
- [ ] No signs of API throttling/escalation from request logs/errors
