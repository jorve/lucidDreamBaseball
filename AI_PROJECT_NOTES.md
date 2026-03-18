# AI Project Notes (Living)

This file is a running memory of inferred project goals and direction.
Update date: 2026-03-16

## What the project appears to be

- Python-based fantasy baseball analytics for a CBS head-to-head league.
- Core concept is CLAP (Categorical League Average Performance), then using category distributions to estimate:
  - team expected wins (`xWins`)
  - matchup outcome probabilities
  - player value (WAR-style) for league-specific categories
- Data workflow is file-based (JSON and CSV), not a web app/service.

## Current pipeline understanding

Primary scripts in `py/` look like a sequential pipeline:

1. `scoring.py`
   - Reads weekly scoring snapshots from `./2016/week*.json`
   - Computes league category means/stdevs (CLAP)
   - Writes `ldbClap.csv`, `weeklyScores.json`, `ldbCLAP.json`, `key_variables.json`
2. `teams.py`
   - Builds per-team weekly and season scoring/records
   - Writes `team_scores.json`, `team_CLAPS.json`
3. `xwins.py`
   - Computes team expected wins and "LUCK"
   - Writes `ldb_xwins.csv`
4. `xmatchups.py`
   - Computes pairwise category/matchup win probabilities
   - Writes `ldb_xmatchups.json`, `replacementLevel.json`
5. `weekly_xmatchup_preview.py`
   - Combines schedule + matchup model for current week pairings
   - Writes `week_matchups.json`
6. `week_preview.py`
   - Produces week preview output
   - Writes `week_preview.csv`
7. `batter_WAR.py`
   - Calculates batter WAR-like outputs from batter CSV data
   - Writes `ldb_batters.json`, `battingWAR.csv`

Separate utility:
- `lottery/ldb_lottery.py` handles draft lottery simulation and CSV output.

## Inferred near-term goals

- Preserve and improve the existing statistical model (CLAP/xWins/xMatchups/WAR).
- Make recurring weekly workflow easier and less manual.
- Improve reliability/maintainability of scripts for repeated in-season use.
- Keep league-specific flexibility (categories, replacement assumptions, custom adjustments).

## Likely medium-term goals (inferred)

- Reduce hard-coded season/week values.
- Better handling for CBS API ingestion/token lifecycle (currently semi-manual).
- Modernize runtime compatibility (some scripts appear Python-2-era).
- Potentially broaden support beyond current head-to-head-only assumptions.

## Technical debt / risks observed

- Hard-coded constants (year/week, local paths, team mappings) in multiple scripts.
- Mixed Python-era syntax/patterns:
  - `print` statement usage in `schedule.py`
  - `f.next()` and `filter(...)` behavior assumptions in `batter_WAR.py`
- No single orchestrator/CLI for running the full pipeline end-to-end.
- Manual data correction is expected for API inaccuracies (documented in `README.md`).
- Output artifact locations and naming are implicit rather than centrally configured.

## Questions to clarify as project evolves

- Is the primary target still one league, or should this become multi-league reusable?
- Which outputs are most important to trust first (xWins, week preview, batter WAR, all)?
- Preferred Python version target for modernization?
- Should this become a package/CLI with reproducible runs and config files?
- Do you want automated tests around statistical transformations before refactors?

## Suggested incremental roadmap

1. Stabilize runtime (pin Python version and dependencies).
2. Add a single driver script/CLI for the pipeline sequence.
3. Externalize config (year, week, categories, paths, league id).
4. Add validation checks for input JSON/CSV and output sanity tests.
5. Improve CBS ingestion ergonomics (token + fetch + cache workflow).
6. Refactor script-by-script with tests to preserve model behavior.

## First modernization targets (second pass)

This is the recommended order for first implementation work. Priority is based on lowest risk + highest leverage.

### Milestone 0: Baseline and safety net (Week 1)

- Goal: lock in current behavior before changing internals.
- Tasks:
  - Capture a known-good run and preserve generated artifacts as baseline references.
  - Add a short "how to run current pipeline" section to `README.md`.
  - Record required Python version and dependency install steps.
- Success criteria:
  - A teammate can reproduce outputs from raw input files using documented steps.
  - Baseline output files are available for diff checks after refactors.

### Milestone 1: Runtime compatibility cleanup (Week 1-2)

- Goal: make scripts consistently runnable on modern Python (target: Python 3.11+ unless you choose otherwise).
- Tasks:
  - Replace Python-2-era syntax/patterns (`print` statement, `f.next()`, `filter(...)` assumptions).
  - Normalize file reads/writes using context managers.
  - Remove dead variables (example: unused API URL strings in local-file workflows).
- Success criteria:
  - All scripts in `py/` and `lottery/` run without syntax/runtime issues on chosen Python 3 version.
  - No behavior change versus baseline outputs except formatting/ordering noise.

### Milestone 2: Centralized configuration (Week 2-3)

- Goal: eliminate hard-coded constants spread across scripts.
- Tasks:
  - Add one config source (e.g., `config.json` or `config.py`) for year, week, categories, paths, league metadata.
  - Update scripts to consume config instead of inline constants.
  - Add defaults + clear error messages for missing config fields.
- Success criteria:
  - Changing season/week/paths requires edits in one place only.
  - Scripts fail fast with actionable messages when config is invalid.

### Milestone 3: Pipeline orchestrator (Week 3)

- Goal: one command to run the full workflow in the right order.
- Tasks:
  - Add a driver script (for example `py/run_pipeline.py`) that runs modules in dependency order.
  - Support partial runs (`--from xwins`, `--only week_preview`) for iteration speed.
  - Emit concise run logs and non-zero exit codes on failure.
- Success criteria:
  - End-to-end pipeline can be executed with a single command.
  - Partial reruns are possible without editing scripts manually.

### Milestone 4: Data validation and model guardrails (Week 4)

- Goal: catch bad inputs and silent model drift early.
- Tasks:
  - Validate required keys and numeric fields for weekly JSON inputs.
  - Add checks for category count consistency and team presence across weeks.
  - Add simple output sanity assertions (no NaNs, probabilities in range, expected file row counts > 0).
- Success criteria:
  - Corrupt/malformed input data fails early with specific errors.
  - Pipeline does not silently emit invalid probability/stat outputs.

### Milestone 5: Thin regression tests (Week 4-5)

- Goal: make future refactors safe.
- Tasks:
  - Add lightweight tests around key transformations:
    - CLAP mean/stdev generation
    - xWins probability accumulation
    - xMatchups pairwise probability structure
    - batter WAR output shape/basic invariants
  - Compare selected outputs against baseline fixtures with tolerance for float rounding.
- Success criteria:
  - At least one test suite runs locally in under 30 seconds.
  - Core model behavior changes trigger test failures instead of silent drift.

### Milestone 6: CBS ingestion ergonomics (Later, optional early)

- Goal: reduce manual overhead when refreshing weekly data.
- Tasks:
  - Add a small ingestion utility for API pulls and local caching.
  - Standardize naming/versioning for raw snapshots (`data/<year>/week<n>.json`).
  - Keep manual patching step explicit where API inaccuracies exist.
- Success criteria:
  - Weekly refresh is predictable and documented.
  - Raw source snapshots remain auditable and reproducible.

## First implementation candidates (specific files)

- `py/schedule.py`: Python 3 compatibility fixes and clearer current-week logic.
- `py/batter_WAR.py`: replace `f.next()` usage and Python 3 `filter` list handling.
- `py/scoring.py` + `py/teams.py`: extract repeated constants into shared config.
- `py/weekly_xmatchup_preview.py` + `py/week_preview.py`: improve dependency assumptions and failure messaging.
- `README.md`: add modern run instructions + pipeline command order.

## Decision points to confirm before coding

- Target Python version (`3.10`, `3.11`, etc.).
- Dependency management preference (`requirements.txt` only vs. Poetry/Pipenv).
- Tolerance policy for output diffs (exact match vs. numeric tolerance).
- Whether to prioritize quick weekly usability (orchestrator) over refactor cleanliness first.

## Session updates

- **2026-03-16**
- **New objective/constraint:** Begin Milestone 0 + 1 immediately with safe, low-risk compatibility work.
- **Impact on roadmap:** Started runtime-compatibility and baseline documentation track before larger refactors.
- **Files/components affected:**
  - `py/schedule.py`: Python 3 print compatibility + robust schedule path resolution
  - `py/batter_WAR.py`: Python 3 compatibility updates (`next(...)`, `filter` handling), context-managed file I/O, resilient input lookup
  - `README.md`: added current layout, baseline run sequence, and runtime notes
  - Verification notes: syntax compile passed for updated scripts; `batter_WAR.py` runtime currently blocked by missing local `scipy` installation in environment

- **2026-03-16**
- **New objective/constraint:** Continue Milestone 1 by modernizing core data-processing scripts while preserving output behavior.
- **Impact on roadmap:** Completed another low-risk compatibility slice and improved environment setup repeatability.
- **Files/components affected:**
  - `py/scoring.py`: resilient week-file lookup (`data/<year>/week<n>.json` with fallback), context-managed output writes
  - `py/teams.py`: same resilient week-file lookup + context-managed JSON output writes
  - `requirements.txt`: added (`scipy`, `simplejson`) for reproducible dependency install
  - `README.md`: added explicit dependency install command
  - Verification notes: `py_compile` and runtime execution succeeded for `py/scoring.py` and `py/teams.py`

- **2026-03-16**
- **New objective/constraint:** Finish Milestone 1 compatibility updates for remaining weekly-projection scripts.
- **Impact on roadmap:** Core pipeline scripts now share the same modernized file-loading pattern and Python 3-safe output handling.
- **Files/components affected:**
  - `py/xwins.py`: path-resilient JSON input loading (`root`/`json`), context-managed CSV write
  - `py/xmatchups.py`: removed legacy/unused imports, path-resilient JSON loading, context-managed JSON writes
  - `py/weekly_xmatchup_preview.py`: path-resilient JSON loading and context-managed JSON write
  - `py/week_preview.py`: removed `simplejson` dependency usage, path-resilient JSON loading, context-managed CSV write
  - Verification notes: `py_compile` passed for all four updated scripts

- **2026-03-16**
- **New objective/constraint:** Start Milestone 2 by centralizing year/week and path configuration.
- **Impact on roadmap:** Replaced per-script hard-coded runtime constants with a shared configuration layer.
- **Files/components affected:**
  - `project_config.json`: added root-level shared config (`current_year`, `current_week`, `paths`)
  - `py/project_config.py`: added shared config loader and helpers (`get_week_file_path`, `get_schedule_path`, `load_project_json`)
  - `py/scoring.py`, `py/teams.py`, `py/schedule.py`, `py/batter_WAR.py`, `py/xwins.py`, `py/xmatchups.py`, `py/weekly_xmatchup_preview.py`, `py/week_preview.py`: switched to shared config module
  - `README.md`: documented shared config usage
  - Verification notes: full `py_compile` pass succeeded across updated scripts; `py/schedule.py` runtime check succeeded

- **2026-03-16**
- **New objective/constraint:** Standardize output destinations across pipeline scripts.
- **Impact on roadmap:** Output file locations are now consistent and controlled by config, reducing root-directory drift.
- **Files/components affected:**
  - `py/project_config.py`: added output helpers (`get_json_output_dir/path`, `get_csv_output_dir/path`) and made JSON loader prefer `json/` before root
  - `py/scoring.py`, `py/teams.py`, `py/xwins.py`, `py/xmatchups.py`, `py/weekly_xmatchup_preview.py`, `py/week_preview.py`, `py/batter_WAR.py`: switched writes from root to configured `json/` and `csv/` output paths
  - `README.md`: documented standardized output destinations

- **2026-03-16**
- **New objective/constraint:** Implement Milestone 3 pipeline orchestrator.
- **Impact on roadmap:** Added a single command entrypoint to run full or partial pipeline in dependency order.
- **Files/components affected:**
  - `py/run_pipeline.py`: added orchestrator with `--list`, `--from`, `--only`, and `--dry-run`
  - `README.md`: updated run instructions to make pipeline runner the default workflow
  - Verification notes: `py_compile` passed for runner; dry-run/list behavior validated for full and partial plans

- **2026-03-16**
- **New objective/constraint:** Scope shift to nightly CBS ingestion service (hybrid auth + API fetch) replacing manual JSON provisioning.
- **Impact on roadmap:** Data ingestion became a first-class workflow alongside analytics pipeline execution.
- **Files/components affected:**
  - `project_config.json`, `py/project_config.py`: ingestion config schema, auth/state/log/raw directory helpers
  - `py/ingestion/auth.py`: hybrid auth manager with browser refresh, keyring-backed session/token cache, env-var credential support
  - `py/ingestion/fetch_cbs_data.py`: CBS fetcher with retry/backoff, per-resource raw snapshots, metadata sidecars, manifest
  - `py/ingestion/normalize.py`: schema validation and normalization into existing pipeline contracts (`data/<year>/week<n>.json`, `json/schedule.json`, etc.)
  - `py/run_ingestion.py`: ingestion CLI (`--date`, `--dry-run`, `--skip-auth`, `--skip-normalize`, `--force-auth-refresh`)
  - `py/run_pipeline.py`: optional ingestion pre-step and required-input stale checks (`--ingest-first`, `--ingest-date`, `--max-input-age-hours`)
  - `README.md`: Task Scheduler setup + failure recovery runbook
  - `requirements.txt`: added ingestion dependencies (`requests`, `keyring`, `playwright`)

- **2026-03-16**
- **New objective/constraint:** Guided live calibration against real CBS league login and API behavior.
- **Impact on roadmap:** Nightly ingestion reached successful end-to-end execution with live auth + normalized outputs.
- **Files/components affected:**
  - `py/ingestion/auth.py`: hardened token extraction (league page candidates, runtime/global/local/session/network parsing, navigation-safe reads)
  - `project_config.json` and `py/project_config.py`: league-domain token source URL defaults
  - `py/ingestion/fetch_cbs_data.py`: schedule endpoint fallback (`schedule`/`schedules`), optional-resource handling for `player_stats`
  - Validation notes: `logs/ingestion_2026-03-16.log` shows final `status: ok`; raw manifest written to `data/raw/2026-03-16/manifest.json`; normalized summary written to `json/ingestion_summary_2026-03-16.json`

- **2026-03-16**
- **New objective/constraint:** Final operational hardening for unattended nightly runs.
- **Impact on roadmap:** Reduced long-term maintenance overhead and improved scheduler ergonomics for production-like operation.
- **Files/components affected:**
  - `py/run_ingestion.py`: added raw snapshot retention cleanup (based on `ingestion.retention_days`) with run-log reporting
  - `README.md`: added copy-paste `schtasks` one-command setup for ingestion and optional pipeline job

- **2026-03-16**
- **New objective/constraint:** Add operational health-check guard for nightly ingestion freshness.
- **Impact on roadmap:** Provides a simple machine-checkable signal for scheduler/ops monitoring.
- **Files/components affected:**
  - `py/check_ingestion_health.py`: verifies latest successful ingestion age from log records
  - `project_config.json` / `py/project_config.py`: added `ingestion.health_max_age_hours` default
  - `README.md`: added health-check usage and optional Task Scheduler command

- **2026-03-16**
- **New objective/constraint:** Add endpoint discovery workflow because CBS API docs are deprecated.
- **Impact on roadmap:** Allows iterative endpoint maintenance by observing real network traffic from league pages.
- **Files/components affected:**
  - `py/discover_cbs_endpoints.py`: Playwright-based endpoint discovery utility (captures API request/response URLs and query keys)
  - `README.md`: added endpoint discovery instructions and output location

## Milestone status snapshot

- Milestone 0 (baseline docs): in progress
- Milestone 1 (runtime compatibility): mostly complete for `py/` scripts; full runtime validation pending local dependency install + end-to-end run
- Milestone 2 (centralized config extraction): mostly complete (shared config + standardized output destinations now implemented)
- Milestone 3 (pipeline orchestrator): started and functionally complete for local script execution
- Milestone 3 extension (ingestion integration): implemented with optional pre-step + staleness guardrails
- Next suggested step: harden ingestion auth selectors and endpoint mappings against real CBS account responses, then add thin regression tests for normalization outputs

## Update protocol for future sessions

When new goals or constraints are learned, append:

- **Date**
- **New objective/constraint**
- **Impact on roadmap**
- **Files/components affected**

