# MVP Replay Runbook — genesis → v0.1.30 on local kind

This runbook walks through feeding 30 eras of captured FilePlugin data into the
local PocketScribe stack managed by Tilt, populating the `block` and `supplier`
tables from genesis through v0.1.30.

## Prerequisites

| Requirement | Check |
|---|---|
| kind cluster `pocketscribe-dev` running | `kubectl cluster-info --context kind-pocketscribe-dev` |
| Tarballs present | `ls /tmp/v0.1.*-fileplugin.tar.xz \| wc -l` → 30 |
| `nats-cli` installed (optional, improves drain detection) | `nats --version` |
| `go` in PATH | `go version` |
| `goose` installed (for `make migrate-dev`) | `goose --version` |
| Free disk under `/tmp` | ~2–4 GB per extracted era; script cleans up after each era |

## 1 — Bring up the stack

```bash
cd /path/to/pocketscribe
tilt up
```

Tilt starts: `postgres` → `nats` → `migrations`.
Watch the Tilt UI until all three resources show green.

Port-forwards active after `tilt up`:
- Postgres: `localhost:5432`
- NATS client: `localhost:4222`
- NATS monitor: `localhost:8222`

## 2 — Run migrations

`migrations` runs automatically when `postgres` is ready (triggered by
`schema/migrations` changes). Verify:

```bash
make migrate-dev-status
```

All migrations should show `OK`.

## 3 — Populate upgrade registry (sync-upgrades)

This is a **manual trigger** in Tilt (it queries Sauron over the network).
Run it once after migrations complete:

```bash
# From terminal (preferred):
go run ./cmd/ps sync-upgrades \
  --config configs/networks/mainnet.yaml \
  --dsn "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

# Or click "sync-upgrades" in Tilt UI → trigger.
```

Verify the upgrades table was populated:

```sql
-- psql or any Postgres client
SELECT version, applied_height FROM upgrades ORDER BY applied_height;
```

Expected: 33 rows (v0.1.2 through v0.1.34).

## 4 — Start consumers

`consumer-block` and `consumer-supplier` start automatically after `migrations`
and `sync-upgrades` in the Tilt dependency graph.  They run as host processes
connecting to port-forwarded NATS and Postgres.

Verify they are running in the Tilt UI (both should show green / serving).

## 5 — Replay eras in order

Run one era at a time, or batch them. The script extracts the tarball to
`/tmp/ps-replay/<version>/`, feeds it to `ps fileplugin --bootstrap`, waits for
the NATS stream to drain, then cleans up.

```bash
# Single era
./scripts/replay_era.sh v0.1.0

# Full genesis → v0.1.30 (sequential, ~hours depending on era size)
./scripts/replay_era.sh \
  v0.1.0 v0.1.2 v0.1.3 v0.1.4 v0.1.5 v0.1.6 v0.1.7 v0.1.8 v0.1.9 \
  v0.1.10 v0.1.11 v0.1.12 v0.1.13 v0.1.14 v0.1.15 v0.1.16 v0.1.17 \
  v0.1.18 v0.1.19 v0.1.20 v0.1.21 v0.1.22 v0.1.23 v0.1.24 v0.1.25 \
  v0.1.26 v0.1.27 v0.1.28 v0.1.29 v0.1.30
```

Note: v0.1.1 has no tarball (was never applied on mainnet — see `docs/research/`).

Environment overrides:
```bash
PS_TARBALL_DIR=/mnt/data/tarballs  # if tarballs moved from /tmp
PS_NATS_URL=nats://localhost:4222  # default
PS_REPLAY_WORKDIR=/tmp/ps-replay   # extraction scratch space
```

## 6 — Verify pipeline health

```bash
# Cursor state — both consumers should show recent heights
go run ./cmd/ps inspect cursors

# NATS stream state — Messages should drop toward 0 between eras
go run ./cmd/ps inspect streams

# System health check
go run ./cmd/ps doctor
```

Example verification queries:

```sql
-- Block count and latest indexed height
SELECT COUNT(*) AS block_count, MAX(block_height) AS tip FROM block;

-- Supplier snapshot count (append-only history)
SELECT COUNT(*) AS snapshot_count, MAX(block_height) AS tip FROM supplier_history;

-- Supplier count at latest height
SELECT COUNT(DISTINCT address) AS active_suppliers
FROM (
    SELECT DISTINCT ON (address) address, block_height
    FROM supplier_history
    ORDER BY address, block_height DESC
) latest;

-- Cross-check: upgrades vs block coverage
SELECT u.version, u.applied_height,
       (SELECT MAX(block_height) FROM block) AS indexed_tip
FROM upgrades u
ORDER BY u.applied_height;
```

## 7 — Tear down

```bash
tilt down
```

The kind cluster is preserved (Postgres and NATS data survive PVC re-mounts on
next `tilt up`).  To reset data completely:

```bash
make cluster-down && make cluster-up
tilt up
# then repeat from step 2
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `consumer-supplier` exits immediately | `sync-upgrades` not run yet | Trigger sync-upgrades first |
| `fileplugin --bootstrap` exits with "no files" | Wrong `--input-dir` | Check tarball extraction path |
| NATS drain hangs | Consumer crashed | Check consumer-block / consumer-supplier in Tilt UI |
| `pending` count stuck | Consumer restart loop | `go run ./cmd/ps inspect streams` to identify which subject |
| Postgres connection refused | Port-forward dropped | `tilt up` re-establishes; or `kubectl port-forward svc/postgres 5432:5432` |
| Out of disk during extraction | Era is large | Free space on `/tmp` or set `PS_REPLAY_WORKDIR` to a larger mount |
