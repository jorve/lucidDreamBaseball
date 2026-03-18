# Season Operational Checklist

Runbook for launch period operations from one week before opening day through week two.

## Day -7 to Day -3 (Pre-Launch Validation)

- [ ] Run nightly ingestion manually once per day:
  - `python py/run_ingestion.py --force-auth-refresh`
- [ ] Validate health after each run:
  - `python py/check_ingestion_health.py`
- [ ] Confirm key status file exists and is healthy:
  - `json/ingestion_status_latest.json`
- [ ] Confirm run-lock behavior is sane:
  - no repeated `INGESTION_RUN_LOCKED` unless another run is genuinely active
- [ ] Confirm request policy is conservative in `project_config.json`:
  - `ingestion.request_policy.min_interval_seconds >= 1.5`
  - retries are not overly aggressive

## Day -2 to Day -1 (Scheduler Cutover)

- [ ] Enable nightly scheduler task.
- [ ] Enable Monday weekly generation task.
- [ ] Trigger a manual scheduler run once:
  - `schtasks /Run /TN "LDB Nightly Ingestion"`
- [ ] Confirm artifacts are updated by scheduled run.
- [ ] Verify account/environment context:
  - task user can access env vars and project directory

## Day 0 (Opening Day Readiness Check)

- [ ] Freeze season config values:
  - `current_year`
  - `ingestion.projections.scoring_week_end_weekday`
  - blend thresholds and request policy
- [ ] Spot-check player parsing:
  - 10 batters + 10 pitchers in projection outputs
- [ ] Confirm these artifacts exist:
  - `json/player_projection_daily_latest.json`
  - `json/player_projection_weekly_latest.json`
  - `json/view_league_weekly_latest.json`
  - `json/view_gm_weekly_latest.json`
  - `json/free_agent_candidates_latest.json`
  - `json/weekly_digest_latest.json`
  - `json/weekly_email_payload_latest.json`
  - `json/artifact_history_latest.json`

## Day +1 to Day +7 (Week 1 Operations)

Daily:

- [ ] Confirm nightly job succeeded.
- [ ] Check:
  - `json/ingestion_status_latest.json`
  - `json/free_agent_candidates_latest.json`
  - `json/view_league_weekly_latest.json`
- [ ] Review status `codes` for new errors/warnings.
- [ ] If lock issue:
  - ensure no active ingestion process
  - remove `.state/ingestion_run.lock`
  - rerun ingestion manually

Monday:

- [ ] Run weekly generation (if not already scheduled):
  - `python py/run_weekly_email.py`
- [ ] Review:
  - `json/weekly_digest_latest.txt`
  - `json/weekly_email_latest.txt`

## Day +8 to Day +14 (Week 2 Stability Check)

- [ ] Continue daily health checks.
- [ ] Confirm history snapshots accumulate:
  - `json/history/<date>/...`
- [ ] Validate calibration output behavior:
  - `json/weekly_calibration_latest.json`
  - acceptable if sparse, but should not crash pipeline
- [ ] Review `metrics.trend.status` once enough data appears.

## Pass/Fail Criteria (First 2 Weeks)

Pass if all are true:

- [ ] >= 10 successful nightly runs out of 14
- [ ] No persistent stale health codes
- [ ] No repeated CBS hard-failure pattern (4xx loops)
- [ ] Weekly artifacts generate on schedule
- [ ] Free-agent suggestions and weekly summaries remain interpretable

Fail/Intervene if any are true:

- [ ] 2+ consecutive nightly failures
- [ ] repeated lock failures without overlapping process
- [ ] repeated auth failures requiring daily manual intervention
- [ ] calibration or weekly generation consistently missing due to data path issues

## Incident Response (Quick Actions)

- Auth issue:
  - rerun `python py/run_ingestion.py --force-auth-refresh`
- Lock issue:
  - verify process -> clear `.state/ingestion_run.lock` -> rerun
- Endpoint/API issue:
  - keep request policy conservative
  - avoid increasing run frequency
  - validate core resources first (`live_scoring`, `schedule`, `rosters`)
