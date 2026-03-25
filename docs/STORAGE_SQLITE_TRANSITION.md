# SQLite transition (JSON today → DB-backed later)

This repo already has a **storage layer** designed for gradual cutover: JSON artifacts remain the contract for UIs and scripts; SQLite is an optional **parallel sink** and future **read path**.

## Current state (as implemented)

- **`project_config.json` → `storage`**
  - `mode`: `json_only` | `dual_write` | `db_primary` (see `py/project_config.py` defaults).
  - `enabled`: master switch for `StorageRecorder`.
  - `sqlite_path`: default `.state/lucid_storage.db`.
- **`py/storage/db.py` → `StorageRecorder`**
  - On enabled modes, writes normalized rows / snapshot JSON into SQLite (`artifact_writes`, `run_events`, roster/transaction/totals snapshots, CLAP snapshots, etc.).
- **`py/storage_backfill_and_parity.py`**
  - Backfill and parity checks vs on-disk JSON.
- **Ingestion / analytics** call `StorageRecorder` at shared write points when storage is on (`py/run_ingestion.py`, normalizers, some analytics I/O).

So “moving to SQLite” is not a rewrite: it is **turning on and widening** this path, then **optionally** serving reads from DB.

## Recommended phases

### Phase 1 — Dual-write only (safe, reversible)

1. Set `storage.mode` to `dual_write` and `storage.enabled` to `true` on a **staging** clone or VM snapshot.
2. Run nightly ingestion + pipeline as today; confirm:
   - JSON `*_latest.json` unchanged for consumers.
   - DB grows; `storage_parity_latest.json` / parity script reports **no mismatches** (or only known tolerances).
3. Keep **UIs and batch scripts reading JSON** until parity is boring for 1–2 weeks.

**Rollback:** set `enabled: false` or `mode: json_only`; drop or ignore the DB file.

### Phase 2 — Read path pilots (narrow)

Pick **one** read surface to try DB-first behind a flag, e.g.:

- Internal tooling / diagnostics that already tolerate schema drift, or
- A single artifact type with a stable mapping (e.g. latest `artifact_writes` row for `transactions_latest`).

Implement **read-through**: try DB → fallback to JSON file → log which path won.

### Phase 3 — `db_primary` for selected flows

- Set `mode` to `db_primary` only when **writers** always persist to DB and **readers** are updated.
- JSON exports become **exports** (nightly snapshot files for git/archive/UI) rather than source of truth.

### Phase 4 — Retention and VM ops

- DB file: keep on disk under `.state/` (already gitignored when using recommended ignores).
- Back up `lucid_storage.db` on the VM (cron `sqlite3 .backup` or file copy when ingestion is idle).
- Tune `storage.retention_days` in config when you add pruning jobs.

## What stays stable

- **Public contract** for `ui/` and `ui-league/`: loading `/json/*_latest.json` (or static paths) until you explicitly add an API or build step that emits JSON from DB.
- **Ingestion auth cache**, locks, and logs: filesystem under `.state/` / `logs/`.

## Next engineering tasks (concrete)

1. **Parity dashboard**: one command that prints green/red per artifact name from `storage_backfill_and_parity` output.
2. **Feature flag** in `project_config.json` for any experimental DB read (avoid branching logic scattered in call sites).
3. **Document** which tables map to which JSON filenames (single table in `db.py` + `CONTRACTS.md` cross-link).
4. **CI**: run parity on a fixture snapshot in `json/` (small) when storage code changes.

## VM note

After enabling dual-write, ensure `.state/lucid_storage.db` is **not** committed (see root `.gitignore`). Use `git rm -r --cached` once if files were tracked historically.

## Migrating an existing clone (stop tracking generated files)

If `json/*_latest.json`, `data/raw/`, or `.state/` were previously committed, `.gitignore` alone will not remove them from the index. On each machine (or once on the canonical repo), run something like:

```bash
git rm -r --cached .state 2>/dev/null || true
git rm -r --cached data/raw 2>/dev/null || true
git rm -r --cached json/history 2>/dev/null || true
git rm --cached json/*_latest.json 2>/dev/null || true
git rm --cached json/ingestion_summary_*.json 2>/dev/null || true
```

Then commit the “stop tracking” change. Developers pull fresh; VMs `git pull` without merge conflicts on generated blobs.
