# Storage Cutover Runbook

## Objective

Move safely from JSON-only state to database-backed storage while preserving nightly reliability and artifact contract compatibility.

## Modes

- `json_only`: default safe mode; no DB writes required.
- `dual_write`: JSON remains primary; DB writes happen in parallel.
- `db_primary`: DB is primary for selected domains; JSON contracts are still exported.

Configured in `project_config.json` under `storage`.

## Readiness Criteria

Before enabling `db_primary` for any domain:

- At least 7 consecutive nightly runs with successful DB writes in `dual_write`.
- No ingestion lock/idempotency regressions.
- Parity check reports:
  - `mismatched = 0`
  - `missing_in_db = 0`
  - only known `skipped_invalid_json` entries (if any).
- `check_ingestion_health.py` remains healthy with `storage_parity=ok`.
- Critical pilot domains populated in DB:
  - roster state
  - transaction ledger
  - weekly totals state
  - team season player totals
  - CLAP artifacts (when present)

## Cutover Sequence

1. **Enable dual-write**
   - Set `storage.mode = "dual_write"`, `storage.enabled = true`.
   - Keep JSON readers unchanged.

2. **Backfill historical artifacts**
   - Run:
     - `python py/storage_backfill_and_parity.py --mode backfill --force-write`

3. **Run parity**
   - Run:
     - `python py/storage_backfill_and_parity.py --mode parity`
   - Confirm parity report at `json/storage_parity_latest.json`.

4. **Observe nightly operations**
   - Continue normal ingestion.
   - Verify `json/ingestion_status_latest.json` includes healthy storage signals.

5. **Promote selected domains**
   - Move one domain at a time to DB-backed reads.
   - Keep JSON export outputs unchanged for UI and downstream compatibility.

6. **Expand to broader DB-primary usage**
   - Only after stable domain-by-domain rollout.

## Rollback

Rollback is immediate and config-driven:

1. Set `storage.mode = "json_only"` and/or `storage.enabled = false`.
2. Re-run ingestion.
3. Confirm:
   - JSON artifacts are still produced as expected.
   - Health check is green.
4. Preserve DB data for forensic comparison (do not delete DB during rollback).

## Operational Checks

- Daily:
  - `python py/check_ingestion_health.py`
- On migration/pilot days:
  - `python py/storage_backfill_and_parity.py --mode parity`
- Weekly:
  - inspect pilot table row counts and parity trend.
