# Backend Storage Scope

## Purpose

Define the canonical storage boundaries for migrating from JSON-only artifacts to database-backed persistence while preserving existing JSON contracts.

## Canonical Entities

- `ingestion_runs`: one row per ingestion execution, including status and timestamps.
- `raw_resources`: raw CBS payload snapshots by `target_date` and `resource_name`.
- `normalized_resources`: normalized bridge payloads produced by ingestion normalization.
- `transaction_ledger`: normalized transaction events and run-level summary metadata.
- `roster_state`: canonical team rosters and integrity diagnostics snapshots.
- `team_weekly_totals_state`: persistent weekly accumulation state used for idempotent rollups.
- `team_season_player_totals`: flattened per-team, per-player season category totals.
- `clap_player_history`: player history distributions used by CLAP v2.
- `clap_matchup_expectations`: matchup/category probability outputs and provenance.
- `clap_calibration`: calibration summaries and diagnostics by engine/segment/source.
- `artifact_writes`: generic latest/artifact payload store for parity and troubleshooting.
- `run_events`: operational audit events emitted during orchestration.

## Source-of-Truth Boundaries

### Phase A (json_only / dual_write)

- JSON artifacts remain source of truth for:
  - downstream pipeline dependencies,
  - UI reads,
  - existing contracts documented in `CONTRACTS.md`.
- Database is best-effort mirror in `dual_write` mode.

### Phase B (db_primary for selected domains)

- Database becomes source of truth for selected high-value domains:
  - season player totals,
  - weekly totals state,
  - roster state + transaction ledger,
  - CLAP outputs + calibration.
- JSON is still generated from DB-backed rows to preserve contracts.

## Retention Windows

- `raw_resources`: 45 days (aligns with current raw retention policy).
- `normalized_resources`: 90 days.
- `ingestion_runs` and `run_events`: 365 days.
- `artifact_writes`: 180 days for full payload copies, then keep only recent hashes/metadata.
- `transaction_ledger`, `roster_state`, `team_weekly_totals_state`, `team_season_player_totals`, `clap_*`:
  - keep full in-season year,
  - keep previous season summary snapshots for trend diagnostics.

## Operational Rules

- DB failures must not block JSON writes in `dual_write`.
- Every DB write stores:
  - `artifact_path`,
  - `target_date` if available,
  - `payload_hash`,
  - `recorded_at_utc`,
  - `write_source`.
- Parity compares JSON vs DB by hash first, then selected semantic fields.

## Cutover Gates

- Consecutive successful nightly runs with dual-write enabled.
- Stable parity pass rate for targeted artifacts.
- No regressions in ingestion health/status and UI outputs.
