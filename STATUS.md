# Project Status

> Real-time view of what's in this repo. Read this before chasing a doc link.

**Phase**: 0 → 1 transition. Schema generation pipeline complete; production Go code (consumers, decoders) not yet written. Spike (Phase 1) is up next per [ROADMAP.md](./ROADMAP.md).

Last updated: 2026-06-06 (session 6 — schema generation + skill pipeline + archeology cleanup).

## What exists today

### Working pipelines

- ✅ **4 Claude Code skills** in `.claude/skills/`:
  - `generate-decoder` — vendors poktroll + cosmos-sdk protos, extracts shape snapshots with field comments
  - `generate-migration-from-diff` — produces idempotent goose migrations from snapshots (244 tables covering full chain data model)
  - `verify-migrations` — applies all migrations against a disposable TimescaleDB container and reports first failure
  - `add-decoder-version` (legacy, kept for reference)
- ✅ **38 schema migrations** in `schema/migrations/` — validated end-to-end via TimescaleDB+goose (`make verify-migrations`)
- ✅ **33 proto-shape snapshots** in `docs/research/.shapes/` — one per poktroll release v0.1.0 → v0.1.33
- ✅ **Archeology run** in `archeology/` — 32 patched binaries (via Git LFS), scripts (with tip-mode orchestrator), 3 patches, and consolidated docs (README, FINDINGS, VERSIONS)

### Documentation

- ✅ `CLAUDE.md`, `README.md`, `ROADMAP.md`, `STATUS.md`, `CONTRIBUTING.md`
- ✅ `docs/architecture/` (10 documents)
- ✅ `docs/decisions/` — 22 ADRs (ADR-001 through ADR-028)
- ✅ `docs/research/` — investigations (poktroll-sync-from-genesis, poktroll-versions, etc.)

### Config

- ✅ `configs/networks/` — `mainnet.yaml`, `beta.yaml`, `localnet.yaml`
- ✅ `configs/{dev,downstream,observability}/` — templates
- ✅ `buf.yaml`, `buf.gen.yaml`, `sqlc.yaml`, `go.mod` (toolchain configs)
- ✅ `Makefile` — only **tested** targets: `verify-migrations`, `regenerate-snapshots`, `regenerate-migrations`, `clean`, `help`

### What does NOT exist yet

- ❌ Production Go code — no `cmd/`, no `internal/` (deleted after honest assessment; will be added when Phase 1 spike begins)
- ❌ `Tiltfile` — currently a stub (`fail("not yet implemented")`)
- ❌ Tests — no Go tests today (the validation today is via `make verify-migrations` against the SQL output)
- ❌ Deploy manifests — `deploy/{docker,k8s}` removed (will return when Phase 3 lands)
- ❌ Live indexing — no consumer, no decoder, no router runtime

## Schema highlights (244 tables)

The schema generation skill produces:

- **3 framework tables** — `decoder_version`, `block`, `processed_heights`, `consumer_consolidation`, `aggregate_registry`, `bucket_seal`, `cagg_dirty_buckets`, `param_history`, `upgrades`
- **Poktroll state entities** — Supplier, Application, Gateway, Service, Session, Claim, Proof, SessionSMT, RelayMiningDifficulty, PendingApplicationTransfer + Morse migration accounts
- **Cosmos-sdk state entities** — BaseAccount, ModuleAccount, Bank Metadata/Balance, Staking Validator/Delegation/Redelegation/Pool/HistoricalInfo, Distribution FeePool + per-validator rewards, Gov Proposal/Vote/Deposit, Upgrade Plan, Slashing ValidatorSigningInfo, Feegrant Grant, plus the 4 distribution per-validator rewards entities with synthetic id_fields
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

See [ROADMAP.md](./ROADMAP.md). The immediate next phase wires the decoders + consumer runtime in Go on top of this schema foundation.
