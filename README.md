# Lucid Dream Baseball 
## Data Analysis for Head-to-Head Leagues on Top of the CBS Fantasy Sports API

This package is primarily for head-to-head leagues, and I haven't given much thought to how something similar would work for roto leagues. If you're interested in adding something for roto format, let me know and I'd be happy to think it through with you.

### Categorical League Average Performance (CLAP)
The big (and probably erroneous) assumption of this model is that weekly scores in each category will be normally distributed around a true population mean. As of right now, `scoring.py` loops through each week's scores and calculates this mean per category. The CLAP is then the benchmark for two calculations: (1) the expected results for the upcoming week's matchups and (2) league-specific Wins Above Replacement values by player.
1. _Expected results of upcoming weekly matchups_
2. _League-specific player valuation (WAR-like outputs)_

### Using the Fantasy Sports API
This package primarily uses two calls to the API: Live Scoring and Schedules. Because there are some inaccuracies with the Live Scoring API, some values need to be manually changed, which requires saving & altering the resultant JSON file (as opposed to working directly from the API results). The Schedules API has no inaccuracies, but is only used once so the results are saved locally here as well.

Complicating matters further is the CBS access token. These tokens are good for about 3 days, and I usually get mine by viewing the HTML source code from a CBS fantasy sports page I am logged into. At some point I might build a Phantom.js script to grab this automatically, but given the weekly frequency of this requirement, I'll likely work on improving the Python package before automating the access token capture. If you search for `api.Token` in the source code it should be the first result.

### Current repository layout
- `data/2016/`: raw weekly data snapshots
- `json/`: intermediate JSON artifacts
- `csv/`: generated CSV outputs
- `py/`: analytics scripts
- `lottery/`: draft lottery utility

### Baseline run notes
Use the pipeline runner for end-to-end execution:

- `python py/run_pipeline.py`

Useful options:

- `python py/run_pipeline.py --list` (show step names)
- `python py/run_pipeline.py --from xwins` (resume from a step)
- `python py/run_pipeline.py --only scoring teams` (run only selected steps)
- `python py/run_pipeline.py --dry-run` (show planned commands only)

Manual sequence (if needed):

1. `python py/scoring.py`
2. `python py/teams.py`
3. `python py/xwins.py`
4. `python py/xmatchups.py`
5. `python py/weekly_xmatchup_preview.py`
6. `python py/week_preview.py`
7. `python py/batter_WAR.py`

### Runtime notes
- Recommended target for modernization: Python 3.11+
- Key dependency: `scipy`
- Install dependencies with: `python -m pip install -r requirements.txt`
- For browser automation login support, also run: `python -m playwright install chromium`
- Preferred runtime is a project-local virtualenv at `.venv/` (see setup below).
- Shared runtime config lives in `project_config.json` (for example `current_year`, `current_week`, and common data/json/csv paths).
- To switch weeks/seasons, update `project_config.json` instead of editing scripts.
- Generated JSON artifacts are written to `json/`.
- Generated CSV artifacts are written to `csv/`.
- Some scripts still assume legacy data filenames and may require local league-specific inputs (for example batter exports).

### Local virtualenv (recommended)

Create and pin a local virtualenv for this repo:

1. `python -m venv .venv`
2. `.\.venv\Scripts\python -m pip install -r requirements.txt`
3. `.\.venv\Scripts\python -m playwright install chromium`

Run scripts with the venv interpreter:

- `.\.venv\Scripts\python py/run_ingestion.py --date 2026-03-16`
- `.\.venv\Scripts\python py/run_pipeline.py`

Workspace note:

- `.vscode/settings.json` pins Cursor/VS Code Python to `.venv\Scripts\python.exe`.

### Nightly ingestion service

Run ingestion manually:

- `python py/run_ingestion.py`

Useful options:

- `python py/run_ingestion.py --date 2026-03-15`
- `python py/run_ingestion.py --dry-run`
- `python py/run_ingestion.py --skip-auth` (uses cached session)
- `python py/run_ingestion.py --skip-normalize` (raw snapshots only)

The ingestion flow:

1. Refreshes auth/session using hybrid mode (`py/ingestion/auth.py`)
2. Pulls CBS resources and writes immutable raw snapshots to `data/raw/<yyyy-mm-dd>/`
3. Normalizes required artifacts back into `data/<year>/` and `json/` for pipeline compatibility
4. Builds transaction ledger + roster state artifacts (when normalization is enabled)
5. Writes recompute trigger intent and ingestion status index artifacts

In addition to fantasy API endpoints, ingestion now also attempts league-domain optional endpoints discovered from page traffic (for example lineup/scout-team APIs). These are treated as optional and logged in the raw manifest.

Environment variables for auth:

- `CBS_USERNAME`
- `CBS_PASSWORD`
- Optional: `CBS_API_TOKEN`

Phase 1 transaction-aware artifacts now produced in `json/`:

- `transactions_latest.json`
- `transactions_quarantine_latest.json`
- `roster_state_latest.json`
- `roster_state_diagnostics_latest.json`
- `recompute_request_latest.json`
- `ingestion_status_latest.json`
- `player_eligibility_latest.json` (eligibility baseline from CBS player-field parsing)
- `player_eligibility_changes_latest.json` (added/removed/updated eligibility drift)

Phase 2 modeling artifacts:

- `preseason_player_priors.json` (manual preseason projection import from role-specific CSVs)
- `player_projection_deltas_latest.json` (Bayesian-shrinkage blended projections with under/over-performance deltas)
  - includes `performance_flag` (`overperforming`, `underperforming`, `on_track`, `insufficient_data`)
  - includes `performance_delta_pct`, plus category-level `category_delta_pct` / `category_performance_flags` when observed category stats are available
- `player_projection_daily_latest.json` (single-day player projection horizon)
- `player_projection_weekly_latest.json` (remaining-week projection horizon, `target_date -> Sunday`)
- `view_league_daily_latest.json` (compact league-facing daily leaders/movers view-model)
- `view_league_weekly_latest.json` (compact league-facing weekly leaders/movers view-model)
  - includes `weekly_summary` with league-wide over/underperformer counts and category movers (`aRBI`, `aSB`, `MGS`, `VIJAY`)
- `view_gm_daily_latest.json` (detailed GM-facing daily player view-model)
- `view_gm_weekly_latest.json` (detailed GM-facing weekly player view-model)
- `clap_player_history_latest.json` (role-aware CLAP history: component-first weekly for batters/RP, per-start for SP)
- `clap_v2_latest.json` (player-driven team/category distribution baseline for CLAP v2)
- `matchup_expectations_latest.json` (per-matchup category win probabilities from both CLAP v2 engines)
- `clap_calibration_latest.json` (historical CLAP v2 engine comparison with recommendation metadata)
- `free_agent_candidates_latest.json` (non-rostered candidate ranking with daily/weekly composite score)
  - includes `replacement_suggestions` add/drop pairs ranked by net gain vs low-end rostered players
- `weekly_digest_latest.json` (weekly machine-readable digest summary)
- `weekly_digest_latest.txt` (weekly human-readable digest render)
- `weekly_email_payload_latest.json` (weekly email payload with prior-week recap + next-week outlook)
- `weekly_email_latest.txt` (weekly email text render)
- `weekly_calibration_latest.json` (realized-vs-projected weekly calibration metrics)
- `artifact_history_latest.json` (snapshot index for latest analytics artifacts)

Notes:

- `--skip-normalize` skips downstream transaction/roster/recompute processing.
- `run_pipeline.py --ingest-first` now logs recompute intent from `recompute_request_latest.json` (informational only for now).
- If preseason priors CSVs are missing, Phase 2 priors/blend steps are marked as skipped (non-fatal).
- Weekly projection output is scoring-week aware for every run day: always projects from `target_date` through the scoring-week end day (Sunday by default).
- Configure scoring-week end day with `ingestion.projections.scoring_week_end_weekday` (Python weekday convention: Monday=`0` ... Sunday=`6`).
- Free-agent ranking tuning is under `ingestion.projections.free_agents` (`daily_weight`, `weekly_weight`, `max_candidates`, `drop_pool_size`, `max_replacement_suggestions`, `min_net_gain`).
- Over/under-performance flag thresholds are configurable via `ingestion.player_blend.overperform_threshold` and `ingestion.player_blend.underperform_threshold`.
- Percentage thresholds are configurable via `ingestion.player_blend.performance_thresholds_percent` (`overall`, `aRBI`, `aSB`, `MGS`, `VIJAY`).
- Weekly email generation tuning is under `ingestion.projections.weekly_email` (`enabled`, `top_players`, `top_swaps`).
- Weekly email metadata tuning also supports `send_day_weekday`, `send_time_local`, `subject_template`, and `recipients` placeholders (generation-only, no send yet).
- Artifact history snapshot behavior is configurable via `ingestion.history` (`enabled`, `retention_days`).
- Weekly calibration behavior is configurable via `ingestion.calibration` (`enabled`, `trend_weeks`, `degrade_mae_pct`).
- CLAP v2 behavior is configurable via `ingestion.clap_v2` (`enabled`, `selected_engine`, `monte_carlo_samples`, `player_cv`, `min_sigma`, `random_seed`, `calibration_lookback_days`, `stabilization_samples_weekly`, `stabilization_samples_starts`, `sp_start_min_outs`, `sp_two_start_threshold`).
- CLAP v2 treats `VIJAY` as an appearance-summed category in RP modeling, stored independently from component-derived categories.
- API request pacing/backoff is configurable via `ingestion.request_policy` (`min_interval_seconds`, `jitter_seconds`, `max_attempts`, `retry_backoff_seconds`, `user_agent`).
- Ingestion overlap protection is configurable via `ingestion.run_lock` (`enabled`, `stale_hours`) to avoid concurrent scheduler runs.

Weekly generation-only utility (no delivery send):

- `python py/run_weekly_email.py`
- Optional: `python py/run_weekly_email.py --ingest-first`

### Frontend MVP (read-only dashboard)

Static dashboard files live in `ui/` and read artifacts from `json/`.

Quick start:

1. From repo root, run:
   - `python -m http.server 8080`
2. Open:
   - `http://localhost:8080/ui/index.html`

MVP dashboard includes a daily/weekly horizon toggle for projection views and artifact freshness indicators.

Separate served views:

- League/public view: `http://localhost:8080/ui-league/index.html`
- GM/private view: `http://localhost:8080/ui/index.html`

Preseason priors input defaults (configurable in `project_config.json`):

- `ingestion.player_blend.preseason_csvs.batters`
- `ingestion.player_blend.preseason_csvs.sp`
- `ingestion.player_blend.preseason_csvs.rp`

Eligibility input defaults (configurable in `project_config.json`):

- `ingestion.eligibility.csvs.batters`
- `ingestion.eligibility.csvs.sp`
- `ingestion.eligibility.csvs.rp`

Eligibility parsing rules:

- Parse positions from `Player` field only (ignore stat columns).
- Batters:
  - RF slot requires `RF`
  - CF slot requires `CF`
  - OF slot accepts `LF`/`CF`/`RF`/`OF`
  - U slot accepts all batters

These defaults are year-aware using `{year}`, for example:

- `data/{year}/preseason/batter_priors.csv`
- `data/{year}/preseason/sp_priors.csv`
- `data/{year}/preseason/rp_priors.csv`

With `current_year: 2026`, paths resolve to `data/2026/preseason/...`.

Backward compatibility:

- single-file `ingestion.player_blend.preseason_csv` is still supported as fallback.

### Ingestion health check

Check whether the latest successful ingestion is fresh enough:

- `python py/check_ingestion_health.py`

Optional threshold override:

- `python py/check_ingestion_health.py --max-age-hours 36`

Health behavior now reports two dimensions:

- ingestion freshness (latest successful run age)
- transaction stream freshness (age of most recent transaction event)

Default thresholds are configured in `project_config.json`:

- `ingestion.health_max_age_hours`
- `ingestion.transaction_health_max_age_hours`

Health check reads `json/ingestion_status_latest.json` first and falls back to ingestion logs if needed.

Status index reason codes now include eligibility drift signals:

- `ELIGIBILITY_UPDATED`
- `ELIGIBILITY_ADDED`
- `ELIGIBILITY_REMOVED`

### Endpoint discovery (deprecated-doc workaround)

Discover currently live CBS fantasy API endpoints by capturing network traffic from league pages:

- `python py/discover_cbs_endpoints.py`

Options:

- `python py/discover_cbs_endpoints.py --headless`
- `python py/discover_cbs_endpoints.py --output json\\cbs_discovered_endpoints_custom.json`

This writes a dated discovery file in `json/` (for example `json/cbs_discovered_endpoints_YYYY-MM-DD.json`) including endpoint paths, query keys, and sample URLs observed while visiting:

- scoring page (`/scoring/standard`)
- teams page (`/teams`)

### Pipeline + ingestion integration

Run ingestion before analytics in one command:

- `python py/run_pipeline.py --ingest-first`

Additional pipeline options:

- `python py/run_pipeline.py --ingest-first --ingest-date 2026-03-15`
- `python py/run_pipeline.py --max-input-age-hours 48`
- `python py/run_pipeline.py --skip-input-check`

When ingestion runs in non-dry mode, pipeline prints recompute intent summary:

- `triggered` (`true`/`false`)
- recommended scope (`none`/`incremental`/`full`)
- affected team/player counts

### Windows Task Scheduler setup

Recommended nightly action:

- Program/script: `C:\\Path\\To\\python.exe`
- Add arguments: `py\\run_ingestion.py`
- Start in: `C:\\Users\\J0RV3\\Documents\\Development\\lucidDreamBaseball`

Copy-paste one-command creation (update python path if needed):

`schtasks /Create /SC DAILY /ST 02:30 /TN "LucidDreamBaseball Nightly Ingestion" /TR "\"C:\\Users\\J0RV3\\AppData\\Local\\Python\\pythoncore-3.14-64\\python.exe\" \"C:\\Users\\J0RV3\\Documents\\Development\\lucidDreamBaseball\\py\\run_ingestion.py\"" /F`

Optional chained nightly analytics job:

- Program/script: `C:\\Path\\To\\python.exe`
- Add arguments: `py\\run_pipeline.py --ingest-first`
- Start in: `C:\\Users\\J0RV3\\Documents\\Development\\lucidDreamBaseball`

Copy-paste one-command creation:

`schtasks /Create /SC DAILY /ST 02:45 /TN "LucidDreamBaseball Nightly Pipeline" /TR "\"C:\\Users\\J0RV3\\AppData\\Local\\Python\\pythoncore-3.14-64\\python.exe\" \"C:\\Users\\J0RV3\\Documents\\Development\\lucidDreamBaseball\\py\\run_pipeline.py\" --ingest-first" /F`

Optional ingestion health-check task:

`schtasks /Create /SC DAILY /ST 03:00 /TN "LucidDreamBaseball Ingestion Health Check" /TR "\"C:\\Users\\J0RV3\\AppData\\Local\\Python\\pythoncore-3.14-64\\python.exe\" \"C:\\Users\\J0RV3\\Documents\\Development\\lucidDreamBaseball\\py\\check_ingestion_health.py\"" /F`

### Tests

Run the Phase 1 guardrail suite:

- `python -m unittest discover -s tests -p "test_*.py"`

Current test coverage includes:

- deterministic event ID behavior
- transaction normalization and idempotency checks
- roster-state atomic trade application and quarantine behavior
- recompute trigger behavior
- preseason priors CSV ingestion and Bayesian blend output generation
- ingestion dry-run smoke checks and legacy artifact presence

### Failure recovery runbook

If nightly ingestion fails:

1. Check latest log in `logs/ingestion_<date>.log`
2. Validate credentials are set (`CBS_USERNAME`, `CBS_PASSWORD`)
3. Re-run once in dry-run mode, then normal mode:
   - `python py/run_ingestion.py --dry-run`
   - `python py/run_ingestion.py`
4. If auth fails repeatedly, run with visible browser by setting `project_config.json -> ingestion.auth.headless` to `false` for one bootstrap run
5. Confirm raw snapshots exist in `data/raw/<date>/` and normalized outputs are updated in `json/`



