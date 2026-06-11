# internal/store

Single gateway for all Postgres access in PocketScribe (ADR-016). Wraps a `pgx/v5` connection pool; every query in the indexer goes through `Store`. Migrations are managed by goose with SQL files embedded from `schema/migrations/`. sqlc-generated queries are not yet used in Slice 1 but the pattern is established here.

## Invariants honored

- **Invariant 2** — all history-table writes are append-only INSERTs; no UPDATE methods exist except on cursor/metadata rows (`consumer_consolidation.consolidated_up_to`, `bucket_seal.sealed_at`).
- **Invariant 4** — every INSERT is `ON CONFLICT … DO UPDATE/NOTHING`; the same input replayed N times leaves the same DB state.
- **Invariant 5** — `ProcessHeight` owns the transaction: `BEGIN` → handler writes → `INSERT processed_heights` → advance consolidation → `COMMIT`. Ack fires after this returns nil.
- **ADR-016** — `database/sql` is used only for the goose migration boundary; all runtime queries use `pgxpool`.
- **ADR-024** — `FlushOnly` runs a write transaction without touching cursors or `processed_heights`; used by `BatchRuntime` partial flushes.

## Entry points

- `New(ctx, dsn) (*Store, error)` — opens and pings a pgxpool; the only constructor.
- `ProcessHeight(ctx, consumer, height, writeFn) (int64, error)` — ack-after-commit transaction body; returns the new `consolidated_up_to`.
- `FlushOnly(ctx, writeFn) error` — partial-flush transaction (no cursor advance); ADR-024 triggers 2-3.
- `IsSealed(ctx, height, genesisVersion) (bool, error)` — derives seal status from consolidation cursors at query time; no materialized seal row.
- `Migrate(ctx, dsn, command) error` — applies goose migrations ("up"/"down"/"status").

## Testing

- **Unit** — `internal/store/versiongate_test.go` for dormancy/first-valid-height logic with a fake pool.
- **Integration** — `test/integration/cursor_test.go`, `store_supplier_test.go`, `seal_test.go`, `migrations_test.go`, `store_error_paths_test.go` run against a real Postgres (testcontainers) and validate consolidation, sealing, upsert idempotency, and migration ordering.
