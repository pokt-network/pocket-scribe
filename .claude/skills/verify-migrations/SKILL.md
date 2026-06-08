---
name: verify-migrations
description: Apply all goose migrations against a disposable TimescaleDB container to validate SQL correctness. Spins up timescale/timescaledb:latest-pg18, runs `goose up`, reports the first failure (if any) with the exact PostgreSQL error and the migration filename. Idempotent — re-runs reset the DB and apply from scratch. Use after editing any migration or skill that emits migrations.
allowed-tools: Read, Bash
---

# /verify-migrations

End-to-end validation of `schema/migrations/`. The single source of truth for "does our SQL actually work."

## Why this exists

Skill output (from `/generate-migration-from-diff` and friends) is *syntactically* sane but PostgreSQL + TimescaleDB have their own rules: reserved words must be quoted, hypertable PKs must contain the partition column, constraint names truncate at 63 bytes, etc. Catching these only at production-deploy time is brutal. This skill catches them in <30 seconds.

## Inputs

Optional flags:
- `--dir <path>` — migrations directory (default: `schema/migrations`)
- `--keep-db` — leave the container running after success (default: stops and removes)
- `--target <version>` — apply up to a specific migration version (default: all)

## Steps

### 1. Ensure docker is available

```bash
docker --version
```

If absent: abort with instructions.

### 2. Bring up (or reuse) the TimescaleDB container

Container name: `ps-verify-db`. Image: `timescale/timescaledb:latest-pg18`. Port: `15432` (host) → `5432` (container). Password: `verify`.

If the container already exists, drop its schema; otherwise launch.

### 3. Wait for readiness

```bash
for i in $(seq 1 15); do
  docker exec ps-verify-db pg_isready -U postgres && break
  sleep 1
done
```

### 4. Reset the public schema

```sql
DROP SCHEMA IF EXISTS public CASCADE;
DROP SCHEMA IF EXISTS _timescaledb_internal CASCADE;
CREATE SCHEMA public;
DROP TABLE IF EXISTS goose_db_version;
```

This guarantees we apply migrations from scratch.

### 5. Run goose up

```bash
go run github.com/pressly/goose/v3/cmd/goose@latest \
    -dir <MIGRATIONS_DIR> \
    postgres "host=localhost port=15432 user=postgres password=verify dbname=postgres sslmode=disable" \
    up
```

Capture stdout + stderr.

### 6. Report

On success:

```
✅ All N migrations applied OK.
   Tables created: <count>
   Hypertables:    <count>
   Schema bytes:   <pg_database_size>
```

On failure: print the first failing migration, the PG error code (SQLSTATE), and the relevant snippet of the migration that triggered it (heuristic: grep for known reserved words, long identifiers, hypertable PK mismatches).

### 7. Cleanup

By default: stop and remove the container. With `--keep-db`: leave it running, print the connection string for manual inspection.

## Common errors recognized

| SQLSTATE | Likely cause | Suggested fix |
|---|---|---|
| 42601 syntax error at or near `<word>` | Reserved keyword used as identifier | Quote in script's PG_RESERVED set |
| 42710 constraint already exists | Identifier truncated to 63 bytes, collision with sibling | Shorten table or constraint name |
| 42703 column does not exist | CREATE TABLE was IF NOT EXISTS no-op (table pre-existed with different shape) | Drop legacy table or rename new one |
| TS103 cannot create unique index without partition column | Hypertable PK missing `block_time` | Include partition column in PK |
| 42P07 relation already exists | Two migrations creating same table | Audit migration ordering |

## Out of scope

- Data validation (this skill only proves SQL applies; not that rows roundtrip).
- Performance benchmarks.
- Down-migration testing — separate concern.

## References

- ADR-016 — pgx + sqlc + goose (toolchain).
- ADR-028 — schema versioning strategy.
- `.claude/skills/generate-migration-from-diff/` — generates the migrations we verify here.
