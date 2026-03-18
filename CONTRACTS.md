# Data Contracts

This document defines stable JSON contracts for ingestion and view-model artifacts.

## Purpose

These files are consumed by downstream tools (CLI summaries, dashboard views, digests).  
Treat them like API responses: predictable structure, predictable types, careful changes.

## Contract Stability Rules

- Keep existing field names stable.
- Keep existing field types stable.
- Additive changes are preferred (new optional fields).
- Do not remove/rename required fields without a schema version bump.
- If behavior changes materially, document it here and in `README.md`.
- Add/adjust tests whenever contract fields are introduced or changed.

## Versioning

- Each artifact should include `schema_version`.
- Current convention is string semver-like values, e.g. `"1.0"`.
- Backward-compatible additions: keep major version the same.
- Breaking changes: increment major version and update consumers first.

## Core Artifacts

### `json/player_projection_daily_latest.json`

Daily projection horizon for `target_date`.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `window` (object)
- `summary` (object)
- `players` (array)

Required `window` fields:
- `label` (string)
- `start_date` (`YYYY-MM-DD`)
- `end_date` (`YYYY-MM-DD`)
- `days` (number; expected `1` for daily)

Required player fields:
- `player_id` (string)
- `player_name` (string)
- `player_role` (string)
- `projected_appearances_window` (number)
- `projected_points_window` (number)
- `blended_projection_season` (number)
- `performance_delta` (number or null)
- `performance_delta_pct` (number or null)
- `performance_flag` (string; `overperforming`, `underperforming`, `on_track`, `insufficient_data`)
- `category_delta_pct` (object; category -> percent delta or null)
- `category_performance_flags` (object; category -> `overperforming|underperforming|on_track|insufficient_data`)

Optional role-specific player fields:
- Batters: `aRBI_window`, `aSB_window`
- Pitchers: `MGS_window`, `VIJAY_window`

### `json/player_projection_weekly_latest.json`

Scoring-week-aware projection horizon from `target_date` to scoring-week end day.

Required top-level fields are the same as daily.

Required `window` fields:
- `label` (string; currently `weekly_remaining`)
- `start_date` (`YYYY-MM-DD`)
- `end_date` (`YYYY-MM-DD`)
- `days` (number; remaining days in scoring week)
- `week_end_weekday` (integer, Python weekday convention: Monday=`0` ... Sunday=`6`)

## CLAP v2 Artifacts

### `json/clap_player_history_latest.json`

Role-aware player history inputs used by CLAP v2 distribution building.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `summary` (object)
- `players` (array)

Required `players[]` fields:
- `player_id` (string)
- `player_name` (string)
- `role` (string; `batters|rp|sp`)
- `weekly_samples` (object)
- `weekly_component_samples` (object)
- `vijay_appearance_values` (array)
- `vijay_weekly_sum_samples` (object)
- `per_start_samples` (object)
- `weekly_start_count_signal` (object)

Role storage semantics:
- `batters`/`rp`: CLAP uses component-first weekly storage (`weekly_component_samples`) with exposure-aware aggregation.
- `rp` `VIJAY`: per-appearance values are stored in `vijay_appearance_values` and summarized in `vijay_weekly_sum_samples`;
  this category is treated as `appearance_summed` (not reconstructed from end-of-week component rollups).
- `sp`: CLAP uses `per_start_samples` (`mu_start`, `sigma_start`, `n_starts`) and
  `weekly_start_count_signal.expected_starts_week` to aggregate per-start signal into weekly expectation.

### `json/clap_v2_latest.json`

Player-driven team/category distribution snapshot used by CLAP v2 probability engines.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `period_key` (string)
- `summary` (object)
- `config` (object)
- `league_baseline` (object keyed by scoring category)
- `teams` (array)

Required `league_baseline[category]` fields:
- `mu` (number)
- `sigma` (number)
- `sample_size` (number)
- `stability` (number in `[0,1]`)

Required `teams[]` fields:
- `team_id` (string)
- `team_name` (string or null)
- `projected_points_window` (number)
- `players_projected` (number)
- `expected_sp_starts_week` (number)
- `categories` (object keyed by scoring category)
- `role_categories` (object keyed by `batters|rp|sp`)

Required `teams[].categories[category]` fields:
- `mu` (number)
- `sigma` (number)
- `contributors` (number)

### `json/matchup_expectations_latest.json`

Per-matchup CLAP v2 category win probabilities for both engines.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `period_key` (string)
- `selected_engine` (string; `analytic_normal|monte_carlo|auto`)
- `summary` (object)
- `matchups` (array)

Required `matchups[]` fields:
- `matchup_id` (string)
- `away_team_id` (string)
- `home_team_id` (string)
- `engines` (object with `analytic_normal` and `monte_carlo`)
- `selected_engine` (string)
- `selected` (object)

Each engine object includes:
- `categories` (object keyed by scoring category)
- `expected_score` (object with `away`, `home`)

Each category probability row includes:
- `away_win_prob` (number in `[0,1]`)
- `home_win_prob` (number in `[0,1]`)
- `away_mu`, `away_sigma`, `home_mu`, `home_sigma` (numbers)
- `away_sp_expected_starts`, `home_sp_expected_starts` (numbers)
- `provenance` (object; role -> `component_derived|per_start_aggregated`)
- `category_source` (string; `component_derived|appearance_summed`)
- `role_contributions` (object; away/home role contribution snapshot)
- `dominant_role` (string; `batters|rp|sp`)

### `json/clap_calibration_latest.json`

Historical calibration comparison between analytic-normal and Monte Carlo CLAP v2 engines.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `window` (object)
- `metrics` (object with per-engine snapshots)
- `engine_recommendation` (object)

Required `metrics.<engine>` fields:
- `samples` (number)
- `brier_score` (number or null)
- `mae` (number or null)

Additional required metric segments:
- `metrics.role_segments` (object keyed by `batters|rp|sp`, each containing per-engine snapshots)
- `metrics.sp_start_buckets` (object keyed by `1-start|2-start`, each containing per-engine snapshots)
- `metrics.category_source_diagnostics` with:
  - `component_derived_categories` (array)
  - `appearance_summed_categories` (array; includes `VIJAY`)
  - `source_segments` (per-engine snapshots keyed by `component_derived|appearance_summed`)

Required `engine_recommendation` fields:
- `recommended` (string)
- `selected` (string)
- `selection_mode` (string; `analytic_normal|monte_carlo|auto`)

## View-Model Artifacts

### `json/view_league_daily_latest.json`
### `json/view_league_weekly_latest.json`

Compact league-facing payloads for leaders/movers.

Required top-level fields:
- `schema_version` (string)
- `view_type` (string; `league`)
- `horizon` (string; `daily` or `weekly`)
- `target_date` (`YYYY-MM-DD`)
- `generated_at_utc` (ISO-8601 UTC string)
- `window` (object)
- `summary` (object)
- `leaders` (object)

Required `summary` fields:
- `player_count` (number)
- `projected_points_total` (number)

Required `leaders` keys:
- `projected_points` (array)
- `overperformers` (array)
- `underperformers` (array)

`view_league_weekly_latest.json` additionally includes:
- `weekly_summary.overall_performance_counts` (object of flag -> count)
- `weekly_summary.category_summary` (object keyed by `aRBI|aSB|MGS|VIJAY`), each with:
  - `overperforming_count` (number)
  - `underperforming_count` (number)
  - `top_overperformers` (array)
  - `top_underperformers` (array)

Leader row contract:
- `player_id` (string)
- `player_name` (string)
- `player_role` (string)
- `projected_points_window` (number)
- `performance_delta` (number or null; present on over/under lists)

### `json/view_gm_daily_latest.json`
### `json/view_gm_weekly_latest.json`

Detailed GM-facing payloads for deeper evaluation.

Required top-level fields:
- `schema_version` (string)
- `view_type` (string; `gm`)
- `horizon` (string; `daily` or `weekly`)
- `target_date` (`YYYY-MM-DD`)
- `generated_at_utc` (ISO-8601 UTC string)
- `window` (object)
- `summary` (object)
- `players` (array)

Required `summary` fields:
- `player_count` (number)
- `role_counts` (object of role -> count)
- `projected_points_total` (number)

`players` rows should follow the player projection row contract.

## Free-Agent Candidates Artifact

### `json/free_agent_candidates_latest.json`

Ranked non-rostered player universe for add/drop scanning.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `assignment_snapshot` (object)
- `scoring` (object)
- `summary` (object)
- `candidates` (array)

Required `assignment_snapshot` fields:
- `source` (string; `roster_state_latest` or `raw_rosters`)
- `as_of_utc` (ISO-8601 UTC string or null)
- `teams_count` (number)

Required `scoring` fields:
- `daily_weight` (number)
- `weekly_weight` (number)

Required `summary` fields:
- `candidate_count` (number)
- `rostered_player_count` (number)
- `universe_player_count` (number)

Candidate row contract:
- `player_id` (string)
- `player_name` (string)
- `player_role` (string)
- `projected_points_daily` (number)
- `projected_points_weekly` (number)
- `composite_score` (number)
- `performance_delta` (number or null)
- `performance_flag` (string)
- `slot_positions` (array of strings; can be empty)

`replacement_suggestions` contract:
- `replacement_suggestions.summary` (object)
  - `teams_considered` (number)
  - `drop_pool_players_considered` (number)
  - `suggestions_count` (number)
  - `min_net_gain` (number)
- `replacement_suggestions.suggestions` (array), each row includes:
  - `team_id` (string)
  - `team_name` (string)
  - `add_player` (object: `player_id`, `player_name`, `player_role`, `slot_positions`)
  - `drop_player` (object: `player_id`, `player_name`, `player_role`, `slot_positions`)
  - `net_points_daily` (number)
  - `net_points_weekly` (number)
  - `net_composite_score` (number)

## Weekly Digest Artifacts

### `json/weekly_digest_latest.json`

Machine-readable weekly summary built from weekly view-model and replacement signals.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `window` (object)
- `summary` (object)
- `top_overperformers` (array)
- `top_underperformers` (array)
- `category_spotlight` (object)
- `recommended_swaps` (array)

Required `summary` fields:
- `league_players` (number)
- `gm_players` (number)
- `overperforming_count` (number)
- `underperforming_count` (number)
- `replacement_candidates` (number)

### `json/weekly_digest_latest.txt`

Human-readable weekly text render intended for console/email style delivery.

## Weekly Email Artifacts

### `json/weekly_email_payload_latest.json`

Weekly email payload with:
- `lookback_week` (previous scoring week recap)
- `lookahead_week` (next scoring week outlook)

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `lookback_week` (object)
- `lookahead_week` (object)
- `delivery_metadata` (object)
- `source_snapshot` (object)

`lookback_week` may be:
- `status: ok` when prior-week history snapshot exists
- `status: insufficient_history` when history has not been captured yet

### `json/weekly_email_latest.txt`

Human-readable weekly email body render.

`delivery_metadata` contract:
- `subject` (string)
- `send_schedule` (object):
  - `send_day_weekday` (integer, Monday=`0` ... Sunday=`6`)
  - `send_time_local` (`HH:MM`)
  - `next_recommended_send_local` (`YYYY-MM-DD HH:MM`)
- `recipients` (array of strings, optional placeholders)
- `delivery_mode` (string; currently `generation_only`)

## Artifact History Index

### `json/artifact_history_latest.json`

Index of per-run snapshots copied to `json/history/<target_date>/`.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `history_root` (string path)
- `retention_days` (number)
- `artifacts` (array)
- `cleanup` (object)

Each `artifacts` row:
- `artifact` (string key)
- `status` (`snapshotted` or `missing`)
- `source` (string path)
- `snapshot` (string path; empty when missing)

## Weekly Calibration

### `json/weekly_calibration_latest.json`

Realized vs projected calibration for the previous completed scoring week.

Required top-level fields:
- `schema_version` (string)
- `generated_at_utc` (ISO-8601 UTC string)
- `target_date` (`YYYY-MM-DD`)
- `calibration_week` (object; `start_date`, `end_date`)
- `summary` (object)
- `metrics` (object)
- `players` (array)

Required metric fields:
- `metrics.overall.count`
- `metrics.overall.mae_points`
- `metrics.overall.bias_points`
- `metrics.by_role` (object)
- `metrics.trend` (object with `status`, recent MAE context)

## Ingestion Status Contract (Operational)

`json/ingestion_status_latest.json` is the machine-readable health/status contract.

It must include:
- run status and freshness
- per-resource status rows
- `codes` array with concise reason/status signals

When new resources are added to ingestion (example: `view_models`), include a resource row so monitoring remains complete.

## Change Checklist (Before Merge)

- Update this file for any contract change.
- Update `README.md` artifact list if outputs changed.
- Add/adjust tests under `tests/` for shape and behavior.
- Run: `python -m unittest discover -s tests -p "test_*.py"`.
