# PocketScribe 🪶

> The faithful chronicler of Pocket Network.

A Go-native indexer for Pocket Network's Shannon protocol. Stream-first ingestion via ABCI FilePlugin, append-only state snapshots with chain-as-source-of-truth, version-aware decoders, and bucket-sealed analytics. Built for owned infrastructure. No cloud lock-in.

## Status

**🚧 Pre-spike — schema foundation complete.** The full chain data model (244 tables covering poktroll + cosmos-sdk core) is generated and validated end-to-end. No Go runtime code yet — the first spike (see [ROADMAP.md](./ROADMAP.md)) wires consumers + decoders on top of this foundation.

**Visual overview**: [`docs/architecture/00-system-flow.md`](./docs/architecture/00-system-flow.md) — 4 mermaid diagrams covering live ingestion, schema generation pipeline, decoder routing, and archeology substrate.

What exists today: skills + schema + archeology. See [STATUS.md](./STATUS.md) for the precise breakdown.

## Why another indexer

| Past pain (legacy indexers) | PocketScribe answer |
|---|---|
| Single-thread / per-block tx (Node.js) | Native Go, per-module consumers, parallel processing |
| Math bugs causing accumulated drift | Snapshots from chain — never computed locally |
| Reindex = days of downtime | Append-only history + per-module reindex in minutes |
| Silent indexer/chain drift | Reconciler with bulk gRPC comparison + auto-heal |
| Replication pain on legacy analytics DBs | Postgres + TimescaleDB OSS, single DB |
| Subscriptions tied to write primary | NATS WebSocket bridge; queries hit replicas |
| Schema changes = full rewrite | Versioned proto decoders per upgrade height |

## Stack (latest stable always)

- **Language**: Go 1.26+
- **Chain**: poktroll (Cosmos SDK v0.53.0 + CometBFT fork)
- **Ingestion**: official Cosmos SDK ABCI `FilePlugin`
- **Bus**: NATS JetStream 2.10+ (3-replica cluster, file storage)
- **Storage**: PostgreSQL 18+ with TimescaleDB OSS 2.18+
- **Migrations**: goose
- **Codegen**: buf (protos), sqlc (queries)
- **DB driver**: pgx v5
- **Local dev**: Tilt + kind/k3d
- **Testing**: testcontainers-go + sebdah/goldie/v2
- **CLI**: cobra + viper, single `ps` binary with subcommands

## Architecture in 60 seconds

```
poktroll archive node (no-prune)
   │  official Cosmos SDK FilePlugin
   ▼
/var/lib/poktroll/streaming/block-{H}-data + block-{H}-meta
   │  ps fileplugin (sidecar)
   ▼
NATS JetStream (3 replicas, dedup by Msg-Id, file storage)
   │
   ├──► ps consumer supplier ──┐
   ├──► ps consumer application ┤
   ├──► ps consumer gateway     ┤──► PostgreSQL 18 + TimescaleDB
   ├──► ps consumer tokenomics  ┘    ├─ entity history (append-only)
   │                                  └─ event hypertables (time-series)
   └──► ps sealing ─────────────► continuous aggregates
                                   (bucket-sealed, gap-aware)

   ps reconciler ───► periodic bulk gRPC list ──► drift detection + auto-heal

Downstream (NOT part of PocketScribe):
   ├──► Hasura          → GraphQL
   ├──► PostgREST       → REST + OpenAPI
   └──► NATS WS bridge  → real-time push
```

Full design in [`docs/architecture/`](./docs/architecture/).

## CLI overview

Single binary `ps`, cobra subcommands:

```bash
# Long-running services
ps fileplugin                  # sidecar: tails FilePlugin dir → NATS
ps consumer <module>           # run one module consumer
ps indexer                     # run all enabled consumers in one process
ps reconciler                  # periodic drift detection
ps sealing                     # bucket sealing loop

# Admin
ps migrate up | down | status
ps inspect streams | cursors | seals
ps replay --module=X --from=H1 --to=H2
ps backfill --from-genesis
ps reconcile --module=X
ps doctor                      # health check: DB, NATS, node
ps version
```

Full CLI in [`docs/architecture/02-ingestion.md`](./docs/architecture/02-ingestion.md) and [`CLAUDE.md`](./CLAUDE.md).

## Local development

### Prerequisites

- **Go 1.26+** (`go version`)
- **Docker** (with at least 8 GB RAM allocated)
- **kind** (`kind version`) — installs via `go install sigs.k8s.io/kind@latest`
- **Tilt** (`tilt version`) — see https://docs.tilt.dev/install.html
- **kubectl** (`kubectl version --client`)
- **golangci-lint v2+** (`golangci-lint --version`) — install: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
- **goose** (`goose --version`) — install: `go install github.com/pressly/goose/v3/cmd/goose@latest`

### Bringing up the local stack

```bash
make cluster-up   # one-time per session: create the kind cluster
tilt up           # brings up postgres + nats; applies migrations
                  # press space in the terminal to open the Tilt UI;
                  # ctrl-C stops it (resources keep running)

# When done:
tilt down         # remove the deployed resources (cluster stays)
make cluster-down # delete the kind cluster entirely
```

`make cluster-up` is idempotent — re-running it is a no-op if the cluster already exists.

After `tilt up` shows all resources green:
- Postgres reachable at `localhost:5432` (user `pocketscribe`, password `dev_only_password`, db `pocketscribe`).
- NATS reachable at `localhost:4222` (client) and `localhost:8222` (monitor).
- The `pocketscribe` database has the full 244-table schema applied via goose.

### CI checks locally

```bash
make ci         # vet + fmt-check + lint + test
make ci-race    # same, with the race detector
make fmt        # apply gofmt to the tree
```

### Resetting the dev stack

```bash
tilt down
tilt up    # fresh start; migrations re-run automatically
```

Or for a full reset (incl. the cluster itself):

```bash
make cluster-down
make cluster-up
tilt up
```


## Documentation

- [`CLAUDE.md`](./CLAUDE.md) — operating rules for Claude Code working on this repo (also valuable for humans)
- [`docs/architecture/`](./docs/architecture/) — full system design (10 documents)
- [`docs/decisions/`](./docs/decisions/) — ADRs for every major call
- [`docs/operations/`](./docs/operations/) — deployment, monitoring, runbooks
- [`docs/research/`](./docs/research/) — focused technical research
- [`ROADMAP.md`](./ROADMAP.md) — phased plan from spike to production
- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — how to add module / aggregate / version

## Hard rules (short version)

1. Every row carries `(block_height, block_time)` from the chain header. **Never** indexer write time.
2. State entities are append-only. No `UPDATE`. No `valid_to_*`. Ranges via `LEAD()`.
3. The chain is the source of truth. We never compute state — we mirror it.
4. Ack NATS messages **after** Postgres commit. Never before.
5. No cloud-managed services. No Node.js framework indexers.
6. Test-driven by default. DRY across the codebase.
7. Local dev runs in Kubernetes (kind/k3d) via Tilt — same as production.

Full rules + rationale: [`CLAUDE.md`](./CLAUDE.md).

## License

TBD (likely MIT or Apache-2.0 to match the broader Pocket ecosystem).

## Credits

