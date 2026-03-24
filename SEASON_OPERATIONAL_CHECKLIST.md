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

## Thursday (First Opening Day Stats Land)

Use this sequence once real opening-day box score stats appear in CBS.

### Copy/paste (VM)

```bash
cd /srv/lucidDreamBaseball
source .venv/bin/activate
set -a && source /etc/lucidDreamBaseball.env && set +a
sudo grep -c '^CBS_API_TOKEN=' /etc/lucidDreamBaseball.env
systemctl list-timers --all | rg lucid-nightly || true
python py/run_pipeline.py --ingest-first
ls -la json/ingestion_status_latest.json json/schedule.json
tail -n 120 /var/log/lucid-nightly.log
```

### Copy/paste (Windows — token task sanity)

```powershell
schtasks /Query /TN "LucidDreamBaseball Token Push" /V /FO LIST
Get-Content "C:\Users\J0RV3\Documents\Development\lucidDreamBaseball\logs\push_token_task.log" -Tail 30
```

- [ ] Confirm token + scheduler prerequisites:
  - Windows task `LucidDreamBaseball Token Push` shows recent success (`Last Result: 0`)
  - VM timer is active: `systemctl list-timers --all | rg lucid-nightly`
  - VM env file has a single token entry:
    - `sudo grep -c '^CBS_API_TOKEN=' /etc/lucidDreamBaseball.env` returns `1`
- [ ] Run one manual VM pipeline pass after first stats appear:
  - `cd /srv/lucidDreamBaseball`
  - `source .venv/bin/activate`
  - `set -a; source /etc/lucidDreamBaseball.env; set +a`
  - `python py/run_pipeline.py --ingest-first`
- [ ] Validate first-stats ingestion outputs:
  - `json/ingestion_status_latest.json` has no critical failures
  - `json/transactions_latest.json` and `json/roster_state_latest.json` updated
  - `json/schedule.json` and `data/<year>/week<current_week>.json` updated today
- [ ] Validate projection + decision outputs are populated:
  - `json/player_projection_daily_latest.json`
  - `json/player_projection_weekly_latest.json`
  - `json/view_league_weekly_latest.json`
  - `json/view_gm_weekly_latest.json`
  - `json/free_agent_candidates_latest.json`
- [ ] Acceptable opening-day caveat:
  - sparse/incomplete first-day category coverage can produce warnings
  - treat as expected unless pipeline exits non-zero or artifacts stop updating
- [ ] If Thursday manual run succeeds:
  - leave nightly timer unchanged (no schedule edits needed)
  - monitor Friday morning run log: `tail -n 120 /var/log/lucid-nightly.log`

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
