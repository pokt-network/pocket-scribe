# Project Status

> Real-time view of what's in this repo. Read this before chasing a doc link.

**Phase**: Phase 1 / Slice 1 complete. Production Go runtime (consumers, decoders, sidecar, reconciler) shipped and green. Slices 2–4 are next per [ROADMAP.md](./ROADMAP.md).

Last updated: 2026-06-11 (Slice 1 done — phases A–G, exit criterion §15 met).

## What exists today

### Production Go runtime (Slice 1, main=226245e)

- ✅ **`cmd/ps`** — single binary, cobra subcommands: `fileplugin --bootstrap`, `consumer block`, `consumer supplier`, `sync-upgrades`, `reconciler`, `migrate`, `deregister-consumer`, `version` (`indexer`, `inspect`, `doctor` and fileplugin live-tail are deferred — see `docs/TECH-DEBT.md`)
- ✅ **`internal/fileplugin`** — sidecar: reads FilePlugin output, fans out per-tx/event/KV to NATS (ADR-022), stamps `Pocket-Block-Time` header, publishes `pokt.block.{H}` envelope last as the per-height completeness fence
- ✅ **`internal/consumer`** — generic `BatchRuntime`: buffers messages per height, flushes in one Postgres tx on the fence, ADR-024 triggers 1–3 (fence / size-5000 / time-5s valves), partial-flush via `store.FlushOnly` (no cursor advance), orphan eviction with seen-count protocol; ack-after-commit invariant intact
- ✅ **`internal/consumer/block`** and **`internal/consumer/supplier`** — module consumers (full supplier lifecycle: `MsgStakeSupplier`, `MsgUnstakeSupplier`, 5 event types, Supplier/ServiceConfigUpdate KV across versions)
- ✅ **`internal/decoders/`** — 8 versioned decoder packages covering all 31 mainnet-applied protocol versions (shape-range strategy: `v0_1_0`, `v0_1_8`, `v0_1_10`, `v0_1_20`, `v0_1_27`, `v0_1_28`, `v0_1_29`, `v0_1_30`)
- ✅ **`internal/router`** — DB-driven height→decoder dispatch; `NewStaticRouter` for unit tests; `TestDecoderForAllMainnetBoundaries` pins all 31 boundaries
- ✅ **`internal/store`** — pgx v5 + sqlc; `ProcessHeight`, `FlushOnly`, cursor, AND-seal, `RequiredSet`, `IsSealed`, `FirstValidHeights`
- ✅ **`internal/reconciler`** — upgrades-table refresh loop (immediate first sync, `pocketscribe_reconciler_syncs_total` / `sync_errors_total`, signal-aware shutdown)
- ✅ **`ps sync-upgrades`** — populates `upgrades` table from mainnet LCD (golden-tested)

### Test suite

- ✅ **27 spec test scenarios** (§11.1) green — including valves + eviction end-to-end, sidecar 256KiB/1MiB caps, migrate-down round-trip, rollback after partial write, upgrade boundary `v0_1_26→v0_1_27`, partial simultaneous restart
- ✅ **~80 fixture triplets** — 9 fixture heights with multi-version golden blobs (real mainnet data: v0.1.0/8/10/20/27/28/29 eras; supplier activity confirmed at 7 heights)
- ✅ **`make ci`** — vet + fmt-check + lint + lint-integration + test-race (fast, no containers)
- ✅ **`make ci-full`** — ci + integration + coverage gate (100% decoders / ≥90% internal/, composition roots `internal/app/*` excluded)
- ✅ **`.github/workflows/ci.yml`** — lint (both tag modes) + race + integration/coverage jobs

### Skills + tooling pipeline

- ✅ **4 Claude Code skills** in `.claude/skills/`: `generate-decoder`, `generate-migration-from-diff`, `verify-migrations`, `add-decoder-version`
- ✅ **40 schema migrations** in `schema/migrations/` — validated end-to-end via TimescaleDB+goose
- ✅ **33 proto-shape snapshots** in `docs/research/.shapes/` — one per poktroll release v0.1.0 → v0.1.33
- ✅ **Archeology run** in `archeology/` — 32 patched binaries (via Git LFS), scripts (with tip-mode orchestrator), 3 patches, and consolidated docs (README, FINDINGS, VERSIONS)

### Documentation

- ✅ `CLAUDE.md`, `README.md`, `ROADMAP.md`, `STATUS.md`, `CONTRIBUTING.md`
- ✅ `docs/architecture/` (12 documents)
- ✅ `docs/decisions/` — 28 ADRs (ADR-001 through ADR-028)
- ✅ `docs/research/` — investigations (poktroll-sync-from-genesis, poktroll-versions, etc.)

### Config

- ✅ `configs/networks/` — `mainnet.yaml`, `beta.yaml`, `localnet.yaml`
- ✅ `configs/{dev,downstream,observability}/` — templates
- ✅ `buf.yaml`, `buf.gen.yaml`, `sqlc.yaml`, `go.mod` (toolchain configs)
- ✅ `Makefile` — `ci`, `ci-full`, `coverage`, `test`, `test-race`, `test-integration`, `verify-migrations`, and more

### What does NOT exist yet (Slices 2–4)

- ❌ Continuous aggregates + bucket sealing loop (`ps sealing`) — Slice 2
- ❌ Hasura + PostgREST deployment + `COMMENT ON` pass — Slice 3
- ❌ NATS WebSocket bridge (`ps ws-bridge`) + full reconciler entity drift — Slice 4
- ❌ `Tiltfile` fully wired — currently a stub (`fail("not yet implemented")`)
- ❌ Deploy manifests — `deploy/{docker,k8s}` will be added in Phase 3

## Schema highlights (244 tables)

The schema generation skill produces:

- **Framework tables** — `decoder_version`, `block`, `processed_heights`, `consumer_consolidation`, `consumer_registry`, `aggregate_registry`, `bucket_seal`, `cagg_dirty_buckets`, `param_history`, `upgrades`
- **Poktroll state entities** — Supplier, Application, Gateway, Service, Session, Claim, Proof, SessionSMT, RelayMiningDifficulty, PendingApplicationTransfer + Morse migration accounts
- **Cosmos-sdk state entities** — BaseAccount, ModuleAccount, Bank Metadata/Balance, Staking Validator/Delegation/Redelegation/Pool/HistoricalInfo, Distribution FeePool + per-validator rewards, Gov Proposal/Vote/Deposit, Upgrade Plan, Slashing ValidatorSigningInfo, Feegrant Grant
- **~80 stateless event/msg hypertables** — auto-included via FQN regex rules
- **Module Params** — one stateful table per poktroll module + per cosmos module
- **Genesis state** — one table per module (forensic record)

See [ADR-028](./docs/decisions/ADR-028-schema-versioning-strategy.md) for the design rationale.

## Schema versioning across 33 poktroll versions

The skill generates *additive only* migrations (CREATE TABLE IF NOT EXISTS / ALTER ADD COLUMN IF NOT EXISTS). Total = 33 decoder migrations (one per version) covering both poktroll-owned and cosmos-sdk-owned shape changes.

Two cosmos-sdk versions are vendored (deduped):

- `cosmos-sdk v0.50.13` for poktroll v0.1.0 → v0.1.11
- `cosmos-sdk v0.53.0` for poktroll v0.1.12 → v0.1.33

The cosmos-sdk version is detected automatically from each poktroll release's `go.mod`.

## Roadmap next steps

See [ROADMAP.md](./ROADMAP.md). Slice 1 foundation is proven. Slice 2 (aggregates + sealing) is next.
