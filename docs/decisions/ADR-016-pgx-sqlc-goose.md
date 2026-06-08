# ADR-016: pgx v5 + sqlc + goose (no ORM)

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

PocketScribe is a write-heavy, query-fast Postgres application with:
- Bulk inserts during backfill (millions per hour).
- COPY-style batch operations.
- Streaming replication awareness.
- Native Postgres types (NUMERIC(78,0), JSONB, TIMESTAMPTZ, arrays).
- Multi-version handling (each consumer may use slightly different queries).

Three approaches considered:
1. **ORM** (GORM, sqlboiler, ent): high-level, less SQL boilerplate.
2. **`database/sql` + raw SQL**: low-level, every query hand-written.
3. **`pgx v5` + `sqlc`**: typed queries generated from SQL files; pgx for hot paths and bulk operations.

## Decision

Use **pgx v5** as the driver and **sqlc** for query type generation. **goose** for migrations.

```
.sql files in internal/store/queries/
       ↓ sqlc
Generated Go in internal/store/gen/  (committed)
       ↓ used by
internal/store/postgres.go (pgx pool wrapper)
```

Migrations: SQL in `schema/migrations/`, runner is `goose`, embedded in the binary via `embed.FS`.

## Consequences

### Positive

- **pgx v5** is the most performant Postgres driver in Go. Supports COPY, LISTEN/NOTIFY, prepared statements, custom types — none of this is lossy through `database/sql`.
- **sqlc generates type-safe Go code** from SQL. Compile errors when query signatures change.
- **Queries are real SQL** (in `.sql` files). DBAs can read them. Optimizer hints work. EXPLAIN ANALYZE on them as-is.
- **goose** supports both `.sql` and Go migrations (escape hatch for complex data migrations).
- **Embedded migrations** via `embed.FS` — the binary self-migrates. CLI works too.

### Negative

- **No magical lazy-loading or eager-loading** (which ORMs offer). Acceptable: explicit JOINs are more readable anyway.
- **No automatic CRUD generation** — write `INSERT`, `SELECT`, etc. manually. Acceptable for an indexer; queries are not generic CRUD.
- **Generated code in repo** — `internal/store/gen/` is committed. Mitigated by `make gen-check` blocking stale generation.

### Neutral

- Learning curve for sqlc is shallow (one day).

## Alternatives considered

### Option A: GORM
- Pro: rapid prototyping; rich ecosystem.
- Con: opinionated; obscures the query plan.
- Con: slow path for bulk operations; doesn't expose COPY.
- Con: N+1 query problems common with relationship loading.
- **Rejected**: ORM overhead doesn't pay rent in a high-throughput indexer.

### Option B: sqlboiler
- Pro: code generation from DB schema (similar to sqlc but reverse).
- Con: less control over query shape; generates one-size-fits-all CRUD.
- Con: schema-driven (regenerates on every schema change).
- **Rejected**: sqlc's query-driven approach fits our explicit queries better.

### Option C: ent (entgo)
- Pro: Go-first, schema-as-code.
- Con: yet another DSL; generated types are complex.
- **Rejected**: similar to GORM concerns + Facebook backing doesn't make it the right choice for this project.

### Option D: `database/sql` + manual struct scanning
- Pro: no codegen step.
- Con: lots of error-prone repetitive `rows.Scan(&...)` code.
- Con: easy to miss columns in scan / get type mismatches at runtime.
- **Rejected**: sqlc eliminates this with compile-time safety.

### Option E: golang-migrate (instead of goose)
- Pro: language-agnostic CLI.
- Con: no Go-migration escape hatch.
- **Rejected**: goose's `func Up(tx *sql.Tx)` lets us write code-based migrations for complex data backfills.

## Implementation notes

### sqlc config (`sqlc.yaml`)

```yaml
version: "2"
sql:
  - engine: postgresql
    queries: internal/store/queries
    schema: schema/migrations
    gen:
      go:
        package: gen
        out: internal/store/gen
        sql_package: pgx/v5
        emit_pointers_for_null_types: true
        emit_db_tags: true
        emit_json_tags: true
```

### Query example (`internal/store/queries/supplier.sql`)

```sql
-- name: GetSupplierAt :one
SELECT * FROM supplier_history
WHERE address = $1 AND block_height <= $2
ORDER BY block_height DESC
LIMIT 1;

-- name: InsertSupplierSnapshot :exec
INSERT INTO supplier_history (
    address, block_height, block_time,
    owner_address, stake_upokt, services, ...,
    snapshot_method, proto_version
) VALUES (
    $1, $2, $3, $4, $5, $6, ..., $N, $N+1
) ON CONFLICT (address, block_height) DO UPDATE SET
    snapshot_method = EXCLUDED.snapshot_method;
```

### Generated Go in `internal/store/gen/supplier.sql.go`

```go
func (q *Queries) GetSupplierAt(ctx context.Context, address string, blockHeight int64) (SupplierHistory, error) { ... }
func (q *Queries) InsertSupplierSnapshot(ctx context.Context, arg InsertSupplierSnapshotParams) error { ... }
```

Consumer code uses these typed methods, never inline SQL strings.

### Pool wrapper (`internal/store/postgres.go`)

Wraps `pgxpool.Pool` with our own helpers (transactions, metrics, retry on transient errors).

### Migration pattern (`schema/migrations/`)

```sql
-- +goose Up
-- +goose StatementBegin
ALTER TABLE supplier_history ADD COLUMN rev_share JSONB NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- (omitted; this migration is forward-only after merge)
-- +goose StatementEnd
```

Run: `goose -dir schema/migrations postgres "$DATABASE_URL" up` (wrapped as `make db-migrate`).

In Go: migrations are embedded via `embed.FS`; the `ps migrate up` subcommand applies them.

## Bulk insert (backfill) pattern

For backfill / large batches, bypass sqlc and use pgx directly:

```go
_, err := tx.CopyFrom(ctx, pgx.Identifier{"supplier_history"},
    []string{"address", "block_height", "block_time", ...},
    pgx.CopyFromRows(rows))
```

10-100x faster than parameterized INSERTs.

## References

- User request: latest stable versions of everything.
- `docs/research/go-project-layout.md` — chose pgx + sqlc + goose explicitly.
- ADR-002 (Postgres + Timescale) — the database we're using.
- CLAUDE.md banned list (no GORM/sqlboiler/ent).
