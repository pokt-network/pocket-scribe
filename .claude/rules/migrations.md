---
paths:
  - "schema/migrations/**/*.sql"
---

# SQL migration rules

When editing files under `schema/migrations/`:

1. **Forward-only.** Never `DROP COLUMN`, `RENAME COLUMN`, `DROP TABLE`. Schema is additive.
2. **Always nullable** when adding columns: `ADD COLUMN x TYPE NULL`. Old rows must remain queryable.
3. **Use goose pragmas**: `-- +goose Up` / `-- +goose Down` blocks with `StatementBegin` / `StatementEnd`.
4. **Filename**: numbered `NNNN_<snake_case_description>.sql` (zero-padded 4 digits).
5. **For history tables** (entities with lifecycle): PK must be `(<id_column>, block_height)`. NO `valid_to_height`.
6. **For event hypertables**: include `block_height BIGINT NOT NULL` and `block_time TIMESTAMPTZ NOT NULL`. Create with `SELECT create_hypertable(...)`.
7. **Down migration** for shipped migrations: no-op or `RAISE EXCEPTION 'irreversible'`. Down is for local dev only.
8. **Comments**: add `COMMENT ON TABLE` / `COMMENT ON COLUMN` for non-obvious fields. Hasura + PostgREST surface these as auto-docs.
9. **Compression policies**: do NOT enable in initial migration. Add as a separate post-backfill migration.

Anti-patterns:
- ❌ `ALTER TABLE supplier_history DROP COLUMN x;` — never.
- ❌ Adding a NOT NULL column without DEFAULT — breaks history.
- ❌ Renaming an existing column — additive only.
- ❌ Inline data migrations using arbitrary SQL — use goose Go migration for complex data work.

See `docs/architecture/03-data-model.md` and `CLAUDE.md` invariants 1, 2.
