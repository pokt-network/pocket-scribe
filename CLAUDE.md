# PocketScribe — Claude Project Rules

> A Go-native indexer for Pocket Network's Shannon protocol. Stream-first, append-only, version-aware. Self-hosted, OSS-only.

This file is your operating manual when working on PocketScribe. **Read it before touching code or proposing designs.**

For deeper context:
- `docs/architecture/` — full system design with rationales
- `docs/decisions/` — ADRs for every major call
- `docs/operations/` — runbooks, deployment, monitoring
- `docs/research/` — focused technical research notes

---

## Project mission

Replace [Pocketdex](https://github.com/pokt-network/pocketdex) (SubQuery + Node.js) with a Go-native indexer that:

1. Survives the failure modes that have bitten prior indexer implementations.
2. Runs on owned infrastructure — no cloud lock-in.
3. Scales horizontally with predictable cost.
4. Is approachable for new contributors (mid/senior Go developers).
5. Acts as a **producer** of indexed data — exposition (Hasura, PostgREST, NATS WebSocket bridge) lives downstream as separate, swappable components.

---

## Version pins (always use the latest stable)

| Tool | Version policy |
|---|---|
| **Go** | Latest stable (**Go 1.26** at project start, Feb 2026). Bump minor deliberately. |
| **PostgreSQL** | Latest stable major (**18.4** at project start; see `docs/research/poktroll-versions.md`). |
| **TimescaleDB** | Latest OSS (2.18+). |
| **NATS Server** | Latest stable (2.10+). |
| **NATS Go client** | Latest `nats.go/jetstream`. |
| **pgx** | v5 latest. |
| **goose, sqlc, buf, cobra, golangci-lint** | Latest stable, pinned in `Makefile` / config. |
| **Tilt, kind/k3d** | Latest stable. |

**Policy**: never use an outdated version of a dependency without a documented reason. Bump majors on a feature branch, test, then merge.

---

## Hard invariants (NEVER violate)

### 1. Every row carries `(block_height, block_time)` from the chain consensus header

- `block_height` (BIGINT NOT NULL) — cursors, gap detection, exact state joins.
- `block_time` (TIMESTAMPTZ NOT NULL) — `time_bucket()` and time-range queries.
- **NEVER** use `now()`, `clock_timestamp()`, or indexer-side timestamps as a queryable axis. `indexed_at` can exist as audit metadata; never in `WHERE`/`GROUP BY`/aggregates.

### 2. Append-only pure for state entities. No `UPDATE`.

- Entities with lifecycle live in `*_history` tables with PK `(address_or_id, block_height)`.
- **No `valid_to_height` column.** Validity computed at query time with `LEAD()`.
- Commutative inserts → order-independent → safe under late arrivals, reorgs, parallel backfill, reconciler corrections.
- "Current" exposed via `DISTINCT ON (address) ... ORDER BY block_height DESC` view.

### 3. Chain is the source of truth. Indexer never computes derived state.

- Never `stake = stake + delta` style mutations from events.
- Snapshots come from chain KV writes (ABCI StreamingService) or bulk gRPC queries — whole entity state as the chain wrote it.
- Events are **triggers** indicating "this entity changed at this height" — the value comes from the chain.
- If your code computes a value the chain already computed (stake totals, mint amounts), the design is wrong.

### 4. Idempotency via deterministic IDs

- All writes use upsert with deterministic PKs: `{block_height}-{tx_index}-{event_index}` or `{tx_hash}-{msg_index}`.
- `INSERT ... ON CONFLICT (pk) DO UPDATE` or `DO NOTHING`.
- NATS messages carry `Nats-Msg-Id` for dedup.
- Same input replayed N times → same output.

### 5. Ack after commit. Never before.

- Consumer pattern:
  1. `BEGIN` Postgres tx
  2. Upsert data rows
  3. Insert cursor advance (`processed_heights`) in same tx
  4. `COMMIT`
  5. **Then** ack NATS message
- Crash between commit and ack → redelivery → idempotent upsert → no-op. Effectively-once.

---

## Process invariants (HOW we work)

### DRY (Don't Repeat Yourself)

- Subject naming: ONE source of truth in `internal/nats/subjects.go`.
- Metric names: ONE place in `internal/metrics/metrics.go`.
- Configuration loading: ONE pattern in `internal/config/`.
- Per-module logic (handlers) is templated — adding a module copies a scaffold, not reinventing wheels.
- Aggregate definitions live in DB (`aggregate_registry`), not hardcoded across files.
- If you find yourself writing similar code in two places, ask first whether the abstraction should exist — but **don't introduce premature abstractions for things that look similar but aren't**.

### TDD (Test-Driven Development) — write tests first

- Default working mode: write the test, see it fail, write the code, see it pass.
- The 5 testing layers (`docs/architecture/10-testing.md`):
  1. **Unit** — pure functions, `_test.go` next to code. <10s total. Run on every commit.
  2. **Component** — testcontainers (NATS, Postgres). One subsystem at a time. ~seconds.
  3. **Golden / contract** — fixtures of real captured chain data, decode and assert. Per proto version. CI on every PR.
  4. **Integration** — full stack in testcontainers (sidecar + NATS + consumers + DB). No real node. ~minutes.
  5. **E2E** — local poktroll node + full stack. Slowest; nightly + pre-release.
- Coverage target: 90% on `internal/`, 100% on decoders (mandatory because correctness is verifiable from chain).
- **Never** merge a bug fix without a test that reproduces the bug.
- **Never** merge a new feature without tests for happy path + at least one edge case.

### Local development with Tilt + kind/k3d

- **Tilt** orchestrates the local dev loop: hot-reload Go binaries, redeploy K8s resources on file save, log aggregation.
- **kind** or **k3d** runs a local Kubernetes cluster (preferred over `docker-compose` for parity with prod).
- **Tiltfile** (root) defines:
  - poktroll archive node (testnet or dev chain)
  - NATS JetStream (single node for dev, cluster mode flag for HA tests)
  - PostgreSQL + TimescaleDB
  - `ps fileplugin`, `ps consumer <module>` (hot-reloaded)
  - Hasura + PostgREST for verification (optional)
- `tilt up` brings the full stack online in <60s.
- `tilt down` tears it down cleanly.
- **No raw `docker run` in `make dev`** — go through Tilt for parity.

### Code review hygiene

- Every PR: lint clean, tests pass, no invariant violations.
- Reviewer asks: "Does this respect the 5 hard invariants?"
- Reviewer asks: "Is the test layer correct? (don't put an E2E test where a unit test would work.)"
- Reviewer asks: "Does this DRY violate `internal/nats/subjects.go` or `internal/metrics/`?"
- Self-review before requesting human review: `make ci` clean.

---

## Banned tools and patterns

| Banned | Reason | Recourse |
|---|---|---|
| ClickHouse | Self-hosted replication is slow; best version is cloud-only. | Use TimescaleDB hypertables. |
| BigQuery, Snowflake, any cloud-managed analytics DB | Cost, lock-in, no sovereignty. | Postgres + Timescale on owned infra. |
| Managed Kafka (Confluent Cloud, MSK) | Cost, vendor lock. | NATS JetStream self-hosted. |
| GraphQL subscriptions via LISTEN/NOTIFY | Won't scale; locks subs to primary. | NATS WebSocket bridge. |
| SubQuery, Subsquid, any Node.js indexer framework | Single-thread, per-block tx model is the root cause of the rewrite. | Native Go consumers. |
| Custom plugin Go code compiled into poktroll node | Risk of crashing consensus path. | Official `FilePlugin` + Go sidecar. |
| `valid_to_height` materialized column on state history | Non-commutative; breaks late-arrival safety. | Append-only pure; `LEAD()`. |
| Mutating state from events (`UPDATE supplier SET stake = stake + X`) | Math bugs cause permanent drift. | Snapshot the entire entity from chain. |
| Polling RPC per-entity in hot path | Doesn't scale (3k suppliers restaking = 3k RPC calls). | StreamingService for live; bulk gRPC for reconciliation/backfill. |
| `time_bucket()` on indexer write time | Breaks reproducibility across replays. | Always `time_bucket(..., block_time)`. |
| `docker-compose` for dev | Loses K8s parity. | Tilt + kind/k3d. |
| `pkg/util`, `internal/common`, "helpers" packages | Anti-pattern; helpers belong to their domain. | Name packages by concern. |
| GORM, sqlboiler, ent | ORM overhead, loss of query control. | pgx v5 + sqlc. |
| `database/sql` directly | Too lossy for an indexer (bulk copy, type fidelity). | pgx v5. |

---

## Stack commitments

- **Language**: Go (latest stable)
- **Ingestion**: poktroll archive node → official Cosmos SDK `FilePlugin` (`plugin = "file"`) → `ps fileplugin` sidecar → NATS JetStream
- **Bus**: NATS JetStream, 3-replica cluster, file storage, dedup by `Nats-Msg-Id`, 30-day retention default
- **Storage**: PostgreSQL (latest) + TimescaleDB OSS. Single DB.
- **Replication**: Postgres streaming replication.
- **Migrations**: goose (Go-migration escape hatch). SQL in `schema/`.
- **Codegen**: buf (protos), sqlc (queries), mockery (mocks).
- **Logging**: `log/slog` (stdlib).
- **Metrics**: prometheus/client_golang.
- **Tracing**: OpenTelemetry.
- **CLI**: cobra + viper. Single binary `ps` with subcommands.
- **Build/release**: goreleaser; ko for images.
- **Local dev**: Tilt + kind/k3d.
- **Testing**: testcontainers-go for integration; sebdah/goldie/v2 for golden files.
- **Downstream APIs (NOT in PocketScribe scope but supported by schema)**:
  - Hasura (GraphQL)
  - PostgREST (REST + OpenAPI)
  - Custom NATS WebSocket bridge (realtime)

---

## CLI (`ps`)

Single binary, subcommand-driven.

```
# Run subcommands (long-running services)
ps fileplugin                           # sidecar: tails FilePlugin dir → NATS
ps consumer supplier                    # run one module consumer
ps consumer application
ps consumer gateway
ps consumer service
ps consumer session
ps consumer tokenomics
ps consumer bank
ps consumer authz
ps indexer                              # run all enabled consumers in one process
ps reconciler                           # periodic drift detection (entities + upgrades)
ps sealing                              # bucket sealing loop
ps sync-upgrades --config <network.yaml>  # populate `upgrades` table from chain (ADR-018)
ps ws-bridge                            # NATS WebSocket bridge (real-time push)

# Admin subcommands (one-shot)
ps migrate up [--steps=N]
ps migrate down [--steps=N]
ps migrate status
ps inspect streams                      # NATS streams + lag
ps inspect cursors                      # consumer_consolidation
ps inspect seals [--aggregate=NAME]     # bucket_seal status
ps replay --module=X --from=H1 --to=H2  # reindex a module range
ps backfill --from-genesis              # bulk historical load
ps reconcile --module=X [--at-height=H] # one-shot reconcile check
ps version
ps doctor                               # health check: DB, NATS, node
ps help
```

---

## Design principles

When facing a decision not covered above:

1. **Fidelity over performance.** Correct + slow is fixable; fast + drifting is not.
2. **Commutativity wins.** Designs that produce identical results regardless of arrival order beat ordered ones.
3. **Local recovery over global recovery.** Bug in one module → reindex that module's height range. Not the whole chain.
4. **Stateless over stateful where possible.** Consumers restartable from cursor state alone.
5. **One source of truth per fact.** Chain = truth for entity state. Tables = truth for cursors. Never both.
6. **Explicit over implicit.** No magic refreshes. Sealing gatekeeped by gap-free consolidation. Aggregates listed in registry.
7. **Tests reflect production failure modes.** Late arrivals, gaps, version boundaries, reconciler races — all have tests.
8. **No exposition logic in the indexer.** Hasura/PostgREST/WS-bridge are separate.
9. **DRY across the codebase**, but not at the cost of premature abstraction.
10. **TDD by default.** Test first; if you can't write the test, you don't understand the spec.

---

## Vocabulary (canonical terms)

Use these exact terms in code, docs, commits, conversation:

- **Consumer**: a Go process (or subcommand) that reads a NATS subject and writes to Postgres.
- **Snapshot**: a row in a `*_history` table representing an entity's full state at a specific `block_height`. Append-only.
- **Fileplugin / sidecar**: the `ps fileplugin` binary that tails FilePlugin output and publishes to NATS.
- **Sealing**: act of materializing a continuous aggregate bucket once consumers have confirmed processing past the bucket's last height.
- **Bucket seal**: row in `bucket_seal` table confirming materialization is complete and gap-free.
- **Consolidation**: per-consumer cursor advancing through height ranges known to be gap-free.
- **Aggregate registry**: `aggregate_registry` table where named aggregates declare metadata.
- **Reconciler**: periodic job that bulk-queries chain and compares against indexed state.
- **Decoder version**: snapshot of poktroll protobuf at a specific git tag, vendored into `internal/decoders/v{X}_{Y}_{Z}/`.
- **Router**: component picking the correct decoder for a `block_height` based on `upgrades` table.
- **Backfill**: replay from genesis (or range) using the same code path as live ingestion.
- **Shadow aggregate**: registered aggregate that is materialized but not exposed via downstream API.

---

## How to work on this project (Claude-specific)

### Before writing code

1. Confirm the change doesn't violate an invariant.
2. If touching state-history tables: append-only, no `valid_to_*`, write a test for out-of-order insert.
3. If touching decoders: identify the version(s). New version → new directory under `internal/decoders/`, not modification.
4. If touching aggregates: add to `aggregate_registry`, don't hardcode sealing logic.
5. **Write the test first** unless you have a strong reason not to.

### Before suggesting an external library

- Go-native? (C/C++ bindings need justification.)
- OSS with healthy maintenance?
- Self-hosted story without enterprise-tier features locked away?
- Latest stable version compatible with the rest of the stack?

### When you find yourself wanting to UPDATE a row

- Stop. Re-read invariants 2 and 3.
- Exceptions: `consumer_consolidation.consolidated_up_to`, `bucket_seal.sealed_at`, `aggregate_registry.status`. These are cursors/metadata, not chain data.

### When you want to query "what was the value 3 days ago"

- First: convert time → height range using `block` table.
- Then: bound query by `block_height BETWEEN min AND max`.
- Never `WHERE block_time BETWEEN ...` directly on a hot query path.

### Specialized agents available

See `.claude/agents/`:
- `pocketscribe-architect` — discuss architectural changes against invariants
- `pocketscribe-schema-designer` — design new entity tables that respect append-only + invariants
- `pocketscribe-proto-versioner` — onboard a new poktroll version's protos
- `pocketscribe-aggregate-designer` — design a new aggregate against registry pattern
- `pocketscribe-test-author` — author tests across the 5 testing layers
- `pocketscribe-reviewer` — review PRs / changes for invariant violations, coverage, ADR alignment

### Specialized skills / slash commands

See `.claude/skills/` and `.claude/commands/`:
- `/scaffold-consumer <module>` — generate consumer skeleton
- `/scaffold-aggregate <name> <bucket>` — generate continuous aggregate + registry entry
- `/generate-decoder <version_tag>` — vendor protos at a poktroll tag and codegen decoders
- `/invariant-check` — scan changed files for invariant violations
- `/tilt-up` — bring local dev stack online

---

## Communication conventions

- Code, comments, commits, docs: **English**.
- PR descriptions: Spanish or English (team preference).
- Commit messages: imperative mood, English, reference ADR/issue.
- Architectural decisions: ADR in `docs/decisions/` **before** implementing.
- Surprises during implementation: document in commit + open an issue.

---

## Quick reference

```
/CLAUDE.md                              # this file
/README.md                              # human intro
/ROADMAP.md                             # phased plan
/CONTRIBUTING.md                        # how to add module / aggregate / version
/Tiltfile                               # local dev orchestration
/Makefile                               # canonical commands (also what CI runs)

/docs/architecture/                     # full system design
/docs/decisions/                        # ADRs (one per major call)
/docs/operations/                       # runbooks, deployment, monitoring
/docs/research/                         # focused technical research notes

/.claude/agents/                        # project-specific subagent definitions
/.claude/skills/                        # project-specific skills
/.claude/commands/                      # project-specific slash commands
/.claude/settings.json                  # hooks, permissions

/cmd/ps/                                # single binary entrypoint
   main.go                              # thin: build cobra root → internal/app/

/internal/                              # private packages (Go convention)
   app/                                 # composition root per subcommand
     fileplugin/, consumer/, indexer/, reconciler/, migrate/, inspect/
   config/                              # viper/koanf loaders
   fileplugin/                          # sidecar logic
   consumer/                            # generic consumer + modules/
   reconciler/                          # drift detection
   sealing/                             # bucket sealing loop
   registry/                            # aggregate registry helpers
   nats/                                # JetStream wrappers (subjects.go = single source)
   store/                               # pgx + sqlc + migrations
   decoders/v{X}_{Y}_{Z}/               # versioned proto decoders
   router/                              # height → decoder dispatch
   proto/                               # OUR protos (envelope)
   chain/                               # cosmos helpers
   types/                               # canonical entity types
   metrics/                             # prometheus registry (single source)
   log/                                 # slog setup
   version/                             # build-time injection

/pkg/scribeclient/                      # ONLY public package (Go SDK)

/schema/migrations/                     # SQL migrations (numbered, forward-only)

/third_party/proto/                     # vendored, not-ours protos
   poktroll/v{X}_{Y}_{Z}/
   cosmos-sdk/v0.53/

/test/
   integration/                         # testcontainers-based
   e2e/                                 # full-stack with real poktroll
   load/                                # k6/vegeta

/configs/                               # app.toml example, NATS config, etc.
/scripts/                               # bash helpers (sync-protos, etc.)
/deploy/                                # docker, docker-compose, helm
```

---

## When in doubt

- Read the relevant ADR in `docs/decisions/`.
- Read the relevant architecture doc in `docs/architecture/`.
- Ask the human before assuming.
