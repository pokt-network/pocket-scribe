# Slice 1 — Orchestration foundation + multi-version decoder lib + block & supplier consumers

**Status**: Draft (awaiting user review)
**Date**: 2026-06-08
**Authors**: Jorge Cuesta, Claude
**Scope**: Phase 1 / Slice 1 (the first of four vertical slices that comprise Phase 1)
**Cadence**: Spare-time pacing. No timeline. The build order is a strict dependency DAG; each phase ships when the prior is green.

---

## 1. Context and goal

Phase 0 produced the design corpus (22 ADRs, 10 architecture docs, 244-table schema validated against TimescaleDB, 32-version proto vendoring, archeology pipeline). No Go runtime code exists yet — `internal/`, `cmd/`, `test/` are intentionally empty.

Phase 1 (per `ROADMAP.md`) is the spike that proves the architecture end-to-end. Its full scope (~10 components) is too large for a single design and implementation cycle. It is decomposed into **four vertical slices**, each independently demoable.

**Slice 1 (this document) is the foundation.** It establishes the orchestration layer that every future module consumer will sit on top of, validates the multi-version decoder pipeline as the central architectural risk, and ships the first two consumers (block and supplier) end-to-end. Subsequent slices build on top of this without rework.

---

## 2. Phase 1 decomposition (where Slice 1 fits)

| Slice | Scope | Depends on |
|---|---|---|
| **1 (this doc)** | Orchestration framework (cursor, gaps, self-heal, per-height seal) + multi-version decoder lib (32 vendored versions) + `ps fileplugin` sidecar + `ps consumer block` + `ps consumer supplier` + `ps sync-upgrades` + `ps reconciler` (upgrades refresh only) | none |
| **2** | Aggregates (`rewards_hourly`) + bucket sealing loop (`ps sealing`) + aggregate dependency declarations | Slice 1's per-height seal state |
| **3** | Hasura + PostgREST + `COMMENT ON` pass + docs landing page (the schema-driven API value proposition) | Slice 1's populated tables |
| **4** | NATS WebSocket bridge (`ps ws-bridge`) + reconciler entity drift detection (full reconciler) + golden tests for supplier decoder + E2E test + active self-heal | Slice 1's data + Slice 2's aggregates + Slice 3's APIs |

Slices 2, 3, 4 each get their own brainstorm → spec → plan cycle after Slice 1 ships.

---

## 3. Architectural model

Slice 1 introduces a layered model that subsequent slices extend. The layering is conceptual; it dictates what depends on what, not how files are organized.

```
LAYER 0 — Orchestration framework (no business logic, no chain knowledge)
  Generic consumer runtime. Cursor tracking, gap detection, ack-after-commit,
  per-height seal logic, restart safety, NATS/Postgres reconnection.
  Testable with NoOpHandlers and synthetic fixtures alone.

LAYER 1 — Consumers in parallel (each owns its module's tables)
  block consumer:    writes the `block` table (header, hash, time, proposer).
  supplier consumer: writes msg_stake_supplier, event_supplier_*,
                     supplier_history (KV state snapshots), supplier_params_history.
  Phase 2: + application, gateway, service, session, bank, authz...

  Each consumer:
    - Has its own cursor in consumer_consolidation.
    - Subscribes to NATS subjects relevant to its module.
    - Decodes only what it owns. Writes only its tables.
    - Advances cursor after processing height H, even if H contained
      nothing for the module ("looked at H, nothing for me, moving on").

LAYER 2 — Per-height seal (AND of all required consumers)
  A height H is sealed when all consumers in required_set(H) have advanced
  past H. required_set(H) is computed dynamically per height and per network
  from consumer_registry, consumer FirstValidVersion declarations, and the
  upgrades table.

LAYER 3 — Aggregates consume sealed heights (Slice 2 territory)
  Each aggregate declares its dependencies on specific consumers. A bucket
  seals when all heights in its range are sealed AND the aggregate has
  materialized.
```

**Key principle**: a height is sealed only when ALL relevant module consumers have signed off. A module consumer added later contributes to sealing from its FirstValidVersion onward; it never invalidates past seals.

---

## 4. Slice 1 components

### 4.1 `ps fileplugin` sidecar

Reads per-block FilePlugin output (`block-{H}-meta` + `block-{H}-data`), parses the per-block proto in-process, fans out per-event to NATS subjects per ADR-022. Operates in two modes:

- **Live**: tails a FilePlugin output directory, publishing as new files appear.
- **`--bootstrap`**: reads from a finite local replica of the archive (downloaded from the archeology Hetzner bucket), publishes at max rate, refuses to exceed `--max-height`.

Same publishing logic, identical NATS payloads, same `Nats-Msg-Id` derivation. Consumers do not distinguish bootstrap from live.

NATS subjects (per ADR-022):
- `pokt.block.{H}` — block envelope (header + hash + tx_count + event_count + chain_id)
- `pokt.tx.{H}.{idx}` — one tx with its result section
- `pokt.events.{eventType}.{H}` — one event
- `pokt.kv.{store}.{H}` — one StoreKVPair

Per-message size ≤ 256 KiB soft cap; ≤ 1 MiB hard cap.

The sidecar requires the decoder library to parse blocks; it queries the router for the appropriate decoder version per block height.

### 4.2 Decoder library (multi-version)

`internal/decoders/v{X_Y_Z}/` packages — one per vendored proto version. Each implements a common `Decoder` interface:

```go
type Decoder interface {
    DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)
    DecodeTx(txBytes []byte) (*types.Tx, error)              // tx + decoded msgs + events
    DecodeStateEntity(kv *types.StoreKVPair) (types.Entity, error)  // typed entity snapshot
    DecodeEvent(ev *types.Event) (types.DecodedEvent, error)
}
```

Each version's package is generated by extending the existing `generate-decoder` skill:
1. `buf generate` against `third_party/proto/poktroll/v{X_Y_Z}/` produces Go protobuf bindings.
2. Hand-written (or scaffolded) version-specific adapter binds the bindings to canonical `types/` representations.
3. Per-version golden tests against curated archeology fixtures verify decode correctness.

All 32 vendored versions get a package. Coverage of the 4 categories per version expands week by week in build order.

### 4.3 Router (DB-driven)

`internal/router/` — implements `DecoderFor(blockHeight uint64) Decoder` by reading the `upgrades` table on startup and on periodic refresh. Per ADR-018, the router NEVER contains hardcoded heights. `internal/router/upgrades.go` is a doc stub.

Tests use `NewStaticRouter([]Upgrade{...})` to avoid DB dependency in unit tests.

### 4.4 `ps sync-upgrades` subcommand

Per ADR-018, this is the canonical mechanism for populating the `upgrades` table:

1. Reads `configs/networks/<name>.yaml` for RPC endpoint and known upgrade names.
2. For each name, queries `/cosmos/upgrade/v1beta1/applied_plan/{name}`.
3. Upserts `(name, height, info)` into `upgrades` table.
4. Returns count of new/updated upgrades.

Reconciler invokes this on a periodic schedule (every N minutes) so new upgrades surface automatically.

### 4.5 `ps reconciler` (upgrades refresh ONLY in Slice 1)

The full reconciler (entity drift detection, gap auto-replay, bulk gRPC verification) is Slice 4. Slice 1 ships only the upgrades-refresh slice of the reconciler — a periodic loop that calls `ps sync-upgrades` to keep the router fresh.

### 4.6 Consumer runtime (generic framework)

`internal/consumer/` — generic runtime that any module consumer embeds. Provides:

- NATS JetStream subscription with `Nats-Msg-Id` dedup.
- Cursor tracking via `consumer_consolidation` table.
- Per-message Handler invocation.
- Ack-after-commit pattern: `BEGIN tx → handler writes data + processed_heights + advances consolidation → COMMIT → ack NATS msg`.
- Restart safety: read own cursor on startup, subscribe from max(cursor)+1.
- Gap detection (passive in Slice 1): if a message for height H+1 arrives and the consumer's cursor is at H-1 (gap at H), the consumer processes H+1 (writes processed_heights row), but consolidation does not advance past H-1. The gap is logged and metric'd.
- Self-registration in `consumer_registry` on startup.

```go
type Consumer interface {
    ID() string                     // unique identifier, e.g., "block", "supplier"
    FirstValidVersion() string      // e.g., "v0.1.0" (genesis-valid) or "v0.1.30"
    Handle(msg *NATSMessage, decoder Decoder) error
}
```

### 4.7 `ps consumer block`

Subscribes to `pokt.block.{H}`. Decodes block header. Writes a row to the `block` table per ADR-010 (height + time + hash + proposer + chain_id). `FirstValidVersion = "v0.1.0"` (block headers exist from genesis on every chain).

### 4.8 `ps consumer supplier`

Subscribes to:
- `pokt.tx.{H}.*` — filters internally for supplier-related msgs (`MsgStakeSupplier`, `MsgUnstakeSupplier`, `MsgUpdateParam` for supplier module, etc.).
- `pokt.events.event_supplier_*.{H}` — supplier-emitted events.
- `pokt.kv.supplier.{H}` (or whatever the store key is for the supplier module) — KV writes for state snapshots.

Writes to:
- `msg_stake_supplier`, `msg_unstake_supplier`, etc.
- `event_supplier_staked`, `event_supplier_unbonding_*`, etc.
- `supplier_history` (full entity snapshot from KV writes; append-only per ADR-005).
- `supplier_params_history` (for `MsgUpdateParam` on supplier module).

`FirstValidVersion = "v0.1.0"` (supplier module has existed since mainnet genesis).

### 4.9 `consumer_registry` table (NEW, Slice 1)

Registration of all consumer instances. Column naming aligns with the existing schema (`consumer_consolidation.consumer_name`, `processed_heights.consumer_name`).

```sql
CREATE TABLE IF NOT EXISTS consumer_registry (
    consumer_name        TEXT PRIMARY KEY,
    first_valid_version  TEXT NOT NULL,      -- e.g., "v0.1.0"
    active               BOOLEAN NOT NULL DEFAULT true,
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    deregistered_at      TIMESTAMPTZ
);

COMMENT ON TABLE consumer_registry IS
    'Self-registered consumer instances. Used to derive required_set(H) for per-height sealing. Consumers INSERT on startup (idempotent via ON CONFLICT DO NOTHING). Operators flip active=false via ps deregister-consumer for explicit decommission.';
```

Consumers `INSERT ... ON CONFLICT DO NOTHING` on startup. The orchestration layer queries `WHERE active=true` to derive REQUIRED_SET.

### 4.10 Per-height seal logic

`required_set(H)` is computed at query time (no materialization in Slice 1):

```
required_set(H, network) = {
    c ∈ consumer_registry WHERE active = true
    AND consumer_first_valid_height(c, network) ≤ H
}

consumer_first_valid_height(c, network):
    let V = c.first_valid_version
    if V ≤ network.genesis_decoder_version:
        return 1
    if V ∈ upgrades(network):
        return upgrades[network][V].height
    return INFINITY  -- dormant on this network

is_sealed(H, network) =
    ∀ c ∈ required_set(H, network):
        consumer_consolidation[c].consolidated_up_to ≥ H
```

`network.genesis_decoder_version` comes from `configs/networks/<name>.yaml`. The upgrades table is populated by `ps sync-upgrades` per ADR-018. `consumer_consolidation.consolidated_up_to` is the per-consumer high-water mark of contiguous processing (existing schema).

**Version comparison is semver, not lexicographic.** "v0.1.10" > "v0.1.9" (lexicographic gets this wrong). All version comparisons in `required_set` resolution go through a semver parser (`golang.org/x/mod/semver` or equivalent). Versions are normalized to canonical form (`vMAJOR.MINOR.PATCH`) at the system boundary; internal code never compares version strings directly.

A materialized `block_seal` table is deliberately deferred to Slice 2 (when aggregates need fast `is_sealed` lookups). Slice 1 uses derived queries.

### 4.11 Per-network config

```yaml
# configs/networks/mainnet.yaml (excerpt)
network:
  id: pocket-mainnet
  chain_id: pocket
  genesis_decoder_version: v0_1_0
endpoints:
  rpc: [https://sauron-rpc.infra.pocket.network]

# configs/networks/localnet.yaml (excerpt)
network:
  id: pocket-localnet
  chain_id: poktroll
  genesis_decoder_version: v0_1_33
endpoints:
  rpc: [http://localhost:26657]
```

The `genesis_decoder_version` is an invariant of the network (the version the chain started at). It does not change after the network exists. Other fields (RPC, chain_id) describe how to reach the chain.

---

## 5. Data flow

```
Chain                    Sidecar                    NATS                     Consumers                  Postgres
─────                    ───────                    ────                     ─────────                  ────────
FilePlugin output dir
  block-H-meta      ──→  ps fileplugin
  block-H-data           parses proto
                         (via router→
                          decoder for H)
                         fan-out:
                         pokt.block.H        ──→   subject "block"     ──→   block consumer        ──→  block (1 row)
                         pokt.tx.H.0         ──→   subject "tx"        ──→   supplier consumer
                         pokt.tx.H.1               ...                       (filters internally)   ──→  msg_stake_supplier (0+ rows)
                         pokt.events.X.H     ──→   subject "events"    ──→   supplier consumer     ──→  event_supplier_* (0+ rows)
                         pokt.kv.supplier.H  ──→   subject "kv"        ──→   supplier consumer     ──→  supplier_history (0+ rows)

                                                                              Each consumer atomically:
                                                                                BEGIN tx
                                                                                INSERT data rows
                                                                                INSERT processed_heights(c, H)
                                                                                UPSERT consumer_consolidation(c, range)
                                                                                COMMIT
                                                                                ack NATS msg

                                                                              When ALL c ∈ required_set(H) crossed:
                                                                                H is "sealed" (derived query, no row written)
```

### 5.1 Startup orchestration order

To avoid chicken-and-egg between sidecar (needs router → needs upgrades) and `ps sync-upgrades` (needs DB + network), the bootstrap sequence is strict:

```
1. ps migrate up                                  ← schema present
2. ps sync-upgrades --config configs/networks/<n>.yaml
                                                  ← upgrades table populated for this network
3. ps fileplugin --bootstrap --input-dir <path>   ← sidecar starts; router can resolve heights
4. ps consumer block (and supplier, etc.)         ← consumers join; self-register; advance cursors
5. ps reconciler                                  ← begins periodic sync-upgrades refresh
```

In Tilt, these are wired as ordered resources with `resource_deps`. The sidecar refuses to start if the upgrades table is empty for the configured network (configurable bypass for tests via `--allow-empty-upgrades`, which makes the router return a single hardcoded fallback decoder — useful only for orchestration-skeleton tests with synthetic fixtures).

---

## 6. Self-heal scope (Slice 1: passive only)

When a consumer detects a gap (receives H+2 having consolidation at H), Slice 1 behavior:

1. The H+2 message is processed normally (data + processed_heights row written).
2. consumer_consolidation does NOT advance past H — it stays at H, waiting for H+1.
3. A metric is incremented (`consumer.gap_count`) and the gap is logged.
4. The consumer continues processing newer messages, expecting H+1 to arrive later via NATS replay or sidecar re-publish.

**Alert threshold**: if a gap persists for more than 5 minutes without filling, the consumer emits a structured WARN log line with the gap range. After 30 minutes unfilled, escalates to ERROR. These thresholds are configurable via env (`PS_GAP_WARN_AFTER`, `PS_GAP_ERROR_AFTER`). Slice 1 does NOT page on its own — it produces signal that operator alerting can subscribe to.

Active self-heal (querying chain RPC for missing heights, injecting into NATS) is Slice 4 / reconciler responsibility. Slice 1 trusts NATS retention and idempotency to eventually close gaps.

---

## 7. Multi-version handling

### 7.1 Codegen

`buf generate` runs against `third_party/proto/poktroll/v{X_Y_Z}/` for each of the 32 vendored versions. Generated bindings live under `internal/decoders/v{X_Y_Z}/gen/`. Hand-written adapter code in `internal/decoders/v{X_Y_Z}/decoder.go` binds bindings to canonical types.

The existing `generate-decoder` skill is extended to also scaffold the adapter file (currently only produces shape snapshots and migrations).

### 7.2 Router

Sidecar AND consumers query `router.DecoderFor(H)` for the version applicable at height H. The router reads `upgrades` table on startup and refreshes periodically.

### 7.3 Schema shifts between versions

Surfaced empirically by per-version golden tests. When v(X+1) adds a field to a struct or changes serialization semantics, the corresponding fixture for v(X+1) fails the v(X) decoder, forcing implementation of the v(X+1) adapter. Schema shifts in the SQL tables themselves are handled by existing migrations (the 38 migrations already validated).

### 7.4 Versions where a module did not exist

A consumer with `FirstValidVersion = "v0.1.30"` running against mainnet sees blocks from v0.1.0 onward. For blocks before v0.1.30's applied_plan height, `required_set` does not include this consumer (per Section 4.10), so the consumer's absence does not block sealing for those blocks. The consumer simply does not subscribe to messages for those heights, or if it does, advances cursor without writing (configurable per consumer).

---

## 8. Fixture structure

Real archeology data, curated per version, ~2-3 representative blocks per version. Total ~60-100 fixtures across 32 versions. Fixture curation is the dominant manual-effort line item in this slice; it is interleaved with each phase that touches a new version set.

```
test/fixtures/
  v0_1_0/
    block-1-data.bin            # raw bytes from FilePlugin output
    block-1-meta.bin            # raw bytes
    block-1-expected.json       # expected rows in each table
    block-78620-data.bin        # last block of v0.1.0 (boundary)
    block-78620-meta.bin
    block-78620-expected.json
  v0_1_30/
    block-635505-data.bin       # first block of v0.1.30 (boundary)
    block-635505-meta.bin
    block-635505-expected.json
    block-XXXXXX-...            # one block with MsgStakeSupplier
    block-YYYYYY-...            # one block with multiple events
  ...
  sync-upgrades/
    mainnet-applied-plans.json  # captured snapshot of RPC response
    beta-applied-plans.json
```

`expected.json` is curated by hand when each fixture is added — the team decides what rows are "correct" for that block. Tests compare DB state against expected.json after replay.

### 8.1 Fixture curation criteria

For each version, fixtures should collectively cover:
- Block AT the upgrade boundary (height where the version was applied).
- Block with a successful `MsgStakeSupplier`.
- Block with diverse events (supplier events + claim events if applicable).
- Block with a KV write to the supplier store (state snapshot trigger).
- Block that is "empty" (no transactions, only BeginBlock/EndBlock events) as an edge case.

The fixture set grows additively. Each new version onboarded brings its own fixtures.

---

## 9. Build order (DAG of phases)

No timeline. Each phase ships when the previous is green. Phases are sequential where dependencies require it; sub-tasks within a phase can be parallelized opportunistically. Test scenarios are numbered 1-27 to match Section 11.1; they accumulate (each phase keeps earlier tests green).

```
┌─────────────────┐
│ A. Prerequisites│
└────────┬────────┘
         │
┌────────▼────────┐
│ B. Layer 0      │   (tests 1-13)
└────────┬────────┘
         │
┌────────▼────────────────────────┐
│ C. Codegen pipeline validated   │
│    on ONE version (v0.1.30)     │
└────────┬────────────────────────┘
         │
┌────────▼────────────────────────┐
│ D. Block consumer + 5 versions  │   (tests 14-17)
└────────┬────────────────────────┘
         │
┌────────▼────────────────────────┐
│ E. Supplier consumer + 5 vers   │   (tests 18-21)
└────────┬────────────────────────┘
         │
┌────────▼────────────────────────┐
│ F. Multi-version expansion (32) │   (tests 22-27)
└────────┬────────────────────────┘
         │
┌────────▼────────────────────────┐
│ G. Hardening + reconciler       │
└─────────────────────────────────┘
```

### Phase A — Prerequisites

**Depends on**: nothing (this is the floor).

Deliverables before any Slice 1 work begins:
- Tilt-based local dev environment that brings up NATS JetStream (1-node) + Postgres + TimescaleDB. Tiltfile fleshed out from current stub.
- Localnet poktroll node bringup procedure (cosmos-sdk localnet pattern) documented. Can be deferred if early phases use only `--bootstrap` mode (recommended path).
- `make ci` baseline: lint, vet, test infrastructure works on an empty internal/ tree (so it stays clean as code arrives).

**Exit**: `tilt up` brings the stack online; `make ci` passes on empty tree.

### Phase B — Layer 0 orchestration skeleton (synthetic data only)

**Depends on**: Phase A.

Implementation:
- `0039_consumer_registry.sql` migration adds the new table.
- Verify existing migrations (`consumer_consolidation`, `processed_heights`) apply cleanly via existing `verify-migrations` skill.
- `internal/consumer/` generic runtime: cursor tracking via `consolidated_up_to`, processed_heights write, ack-after-commit, restart safety, passive gap detection, self-registration into `consumer_registry`.
- Two `NoOpHandler` instances wired in parallel for testing the AND-seal logic.
- testcontainers harness for NATS + Postgres in tests.
- Synthetic fixtures: bash-generated `block-{H}-meta` and `block-{H}-data` with marker bytes (no real proto needed).
- `ps deregister-consumer <name>` subcommand.

**Tests (green to exit)**: 1-13 (Section 11.1).

**Exit**: pipeline plumbing validated with zero chain code.

### Phase C — Codegen pipeline validated end-to-end on ONE version

**Depends on**: Phase B (need the consumer runtime stable to wire codegen output into).

This phase exists to derisk the codegen extension BEFORE we scale to 32 versions. If buf+scaffold doesn't produce usable Go code, we learn here, not at Phase F.

Implementation:
- Extend the existing `generate-decoder` skill to also emit a Go decoder adapter scaffold (currently emits only shape snapshots and SQL migrations).
- Run codegen against `third_party/proto/poktroll/v0_1_30/` → `internal/decoders/v0_1_30/`.
- Hand-fill the adapter for block header decoding only (sufficient to drive Phase D's block consumer; other categories filled in subsequent phases).
- Unit tests for the codegen output: shape comparison against a captured snapshot of the proto fields.

**Tests (green to exit)**: unit-level decoder tests for v0.1.30 block header only. No test number — this is sub-Phase D infrastructure.

**Exit**: `internal/decoders/v0_1_30/` compiles, block header decode passes unit test, codegen pattern proven repeatable.

### Phase D — Block consumer + sidecar + first real fixtures (5 versions)

**Depends on**: Phase C (needs codegen working for at least v0.1.30).

Implementation:
- `ps fileplugin` sidecar in `--bootstrap` mode (live mode hardening can wait).
- Router DB-driven (loads from `upgrades` table, refreshes periodically).
- `ps sync-upgrades` subcommand against mainnet RPC (real network, golden-snapshot for tests).
- `ps consumer block`: subscribes to `pokt.block.{H}`, writes block table.
- Real fixtures curated from the archeology Hetzner bucket for 5 representative versions (v0.1.0, v0.1.10, v0.1.20, v0.1.28, v0.1.29). Per-version curation criteria per Section 8.1.
- NoOp #2 stays wired (it becomes the supplier consumer in Phase E).

**Tests (added)**: 14-17 (Section 11.1).

**Exit**: real chain data flows end-to-end through orchestration; multi-version dispatch validated for block-header category.

### Phase E — Supplier consumer joins (5 versions, all 4 categories)

**Depends on**: Phase D.

Implementation:
- Replace NoOp #2 with `ps consumer supplier`.
- Extend decoder lib: tx + msg_stake_supplier + event_supplier_* + supplier KV state (the 3 remaining categories beyond block header).
- Fixtures: supplier-specific blocks (MsgStakeSupplier, supplier events, supplier KV writes) for the same 5 versions.
- Per-height seal now requires both block AND supplier consumers.

**Tests (added)**: 18-21.

**Exit**: supplier lifecycle end-to-end across 5 versions.

### Phase F — Multi-version expansion (the remaining 27 versions)

**Depends on**: Phase E (the codegen + adapter pattern + fixture format must be proven before scaling).

This is the BIGGEST phase in raw work volume. Fixture curation alone is the dominant cost — ~3 fixtures per version × 27 versions = ~80 fixtures to curate by hand.

Implementation:
- Curate fixtures for the 27 remaining versions (drives the rest of this phase).
- Run codegen against the 27 remaining proto dirs.
- Implement adapters per version, surfacing any schema shifts as they appear.
- Update `ps sync-upgrades` known-names list to cover all 32 versions.

**Tests (added)**: 22-27.

**Exit**: 32 versions × 4 categories × 2 consumers all green.

### Phase G — Hardening + reconciler refresh

**Depends on**: Phase F.

Implementation:
- `ps reconciler` minimal: periodic loop calling `ps sync-upgrades`. (Full reconciler is Slice 4.)
- Edge cases: empty blocks, upgrade boundary corners, large blocks (256 KiB soft cap → 1 MiB hard cap behavior), simultaneous consumer restarts.
- Per-component package READMEs.
- `make ci` clean: lint, vet, test (race detector), coverage targets (80% on internal/, 100% on decoders).

**Exit**: full Slice 1 exit criterion (Section 15) met.

---

## 10. Error handling

| Failure | Handler responsibility | Slice 1 behavior |
|---|---|---|
| NATS msg deserialization fails | Sidecar | Sidecar refuses to publish, alerts. (Sidecar parsing must be correct; bug → fix sidecar, not consumer.) |
| Decoder crashes on bytes | Consumer | Log error with fixture context, do NOT advance cursor, do NOT ack NATS. Message retries until handled (or dead-letter after N attempts — Slice 4). |
| Postgres tx fails (constraint violation, etc.) | Consumer runtime | Rollback tx, do NOT ack NATS. Message retries. If persistent: alert. |
| NATS disconnected | Consumer runtime | Reconnect with exponential backoff. Resume subscription from last ack. |
| Postgres restarted / connection lost | Consumer runtime | Reconnect (pgx pool handles), retry tx. |
| Gap in NATS sequence | Consumer runtime | Process newer messages, do not advance consolidation past gap. Log + metric. |
| Consumer crashed mid-tx | OS | On restart, the partial tx was rolled back by Postgres. NATS replays the unacked message. Idempotency keys prevent double-write. |
| `ps sync-upgrades` fails to reach chain | Reconciler | Log, alert. Router uses existing (cached) upgrades table state until next successful sync. |

---

## 11. Testing strategy

Per CLAUDE.md and ADR-012 (testing strategy):

- **Layer 1 (Unit, <10s total)**: pure functions, table-driven, no external deps. `_test.go` next to code. Examples: cursor advancement logic, required_set computation, FirstValidHeight resolution.
- **Layer 2 (Component, ~seconds)**: per-subsystem with testcontainers. Examples: sidecar publishing to NATS, single consumer against Postgres.
- **Layer 3 (Golden / contract)**: per-version fixtures from archeology bucket. Decode → assert canonical types match expected.json. This is the multi-version validation backbone.
- **Layer 4 (Integration, ~minutes)**: full Slice 1 stack in testcontainers (sidecar + NATS + two consumers + Postgres). Replays curated fixtures, asserts DB state.
- **Layer 5 (E2E with real poktroll node)**: deferred to Slice 4 / Phase 2.

**TDD discipline (CLAUDE.md invariant)**: every feature is preceded by a failing test. For decoder work, the workflow per version is:

1. Curate the fixture (download from Hetzner, identify representative blocks).
2. Write the golden test asserting expected.json — it fails (no decoder).
3. Implement the decoder adapter for that version until the test passes.
4. Repeat for next path / next version.

Nothing reaches "verified" status without a repeatable test backing it. No manual verification claims.

### 11.1 Test scenarios (all 27)

**Orchestration-pure (Phase B, synthetic):**
1. Cursor advances contiguously over N synthetic messages.
2. Forced gap: cursor stops at N-1, gap recorded, no further advance until gap fills.
3. Kill + restart: consumer resumes from cursor; no duplicate rows.
4. Crash mid-tx: no duplicate, no skip (ack-after-commit holds).
5. NATS disconnect + reconnect: consumer resumes.
6. Postgres restart: consumer waits, recovers.
7. Per-height seal with 1 NoOp: H sealed when NoOp crosses H.
8. Per-height seal with 2 NoOps: H sealed only when BOTH cross H.
9. Self-registration: consumer startup is idempotent (re-startup does not duplicate row).
10. Out-of-order delivery: H+1 arrives before H, processed_heights records both, consolidation stays at H-1 until H arrives.
11. Duplicate Nats-Msg-Id: same message twice, no duplicate row.
12. Multiple consumers crash simultaneously: independent restart, sealing eventually recovers.
13. Decommission: `ps deregister-consumer <name>` flips active=false, required_set no longer includes it, sealing no longer waits for it.

**Block consumer + initial multi-version (Phase D, real fixtures):**
14. `ps sync-upgrades` against real mainnet RPC: response captured as golden, replay matches bit-for-bit.
15. `router.DecoderFor(H)` returns correct decoder at upgrade boundaries (5 fixtures).
16. Block consumer + 5 fixtures: each produces correct row in `block` table + per-height seal triggers when block + NoOp #2 both cross H.
17. Self-heal with real gap: force gap between two fixtures, verify consumer pauses consolidation, recovers when gap fills.

**Supplier consumer (Phase E):**
18. `MsgStakeSupplier` decoded correctly across 5 versions, row written to msg_stake_supplier.
19. event_supplier_staked and other supplier events populated correctly.
20. supplier_history snapshots written from KV writes, append-only (multiple snapshots per address as state changes).
21. Per-height seal: H sealed only when BOTH block and supplier have crossed H (verified with intentional supplier lag).

**Version-aware orchestration (Phase F):**
22. Dormant consumer: declare a fictitious consumer with `FirstValidVersion = "v0.2.0"` (never applied on mainnet), verify it registers as dormant, does not affect required_set, exits cleanly.
23. Dynamic required_set per height: for H ∈ [0, first_valid-1] the consumer is not in required_set, H seals without it; for H ≥ first_valid, the consumer is required.
24. Consumer wakeup: fixture where sync-upgrades adds a new version → consumer wakes → enters required_set for heights ≥ that version (test uses different router states between runs).
25. Multi-network correctness: same consumer code run against mainnet.yaml vs localnet.yaml, required_set computed differently per equivalent fixtures.
26. Backfill semantics: a consumer added "after the fact" on mainnet, cursor starts at first_valid_height, seals from genesis to that height are unaffected, seals after pause until backfill catches up.
27. Block exceeding 256 KiB soft cap: sidecar logs WARN, continues publishing up to 1 MiB; block exceeding 1 MiB: sidecar refuses, alerts.

These 27 are the floor. Additional tests will be added during implementation as edge cases emerge.

---

## 12. CLI surface (Slice 1)

```
ps fileplugin --bootstrap --input-dir <path> --max-height <H>
ps fileplugin                                  # live mode (Phase 2 polish)
ps consumer block
ps consumer supplier
ps indexer                                     # runs all enabled consumers in one process
ps reconciler                                  # upgrades refresh only in Slice 1
ps sync-upgrades --config configs/networks/<name>.yaml
ps deregister-consumer <id>                    # admin: flip consumer_registry.active=false
ps migrate up
ps inspect cursors                             # show consumer_consolidation state
ps inspect streams                             # show NATS JetStream state
ps doctor                                      # health check
ps version
```

Each subcommand is implemented in `internal/app/<name>/`. The `cmd/ps/main.go` is thin — just wires Cobra to the app packages.

---

## 13. Dependencies and ADRs honored

- **ADR-001** Go over Rust
- **ADR-003** FilePlugin + sidecar
- **ADR-004** NATS JetStream
- **ADR-005** Append-only pure
- **ADR-006** Chain as source of truth
- **ADR-007** Per-module consumers
- **ADR-008** Versioned decoders (per-version Go packages)
- **ADR-010** (block_height, block_time) invariant
- **ADR-012** Testing strategy
- **ADR-013** Single binary CLI
- **ADR-015** TDD and DRY
- **ADR-016** pgx + sqlc + goose
- **ADR-018** No hardcoded upgrades (DB-driven router)
- **ADR-022** NATS payload discipline (per-event fan-out)
- **ADR-023** Live vs bootstrap boundary
- **ADR-024** Consumer batching (single-msg in Slice 1; batched in Phase 2)
- **ADR-025** Indexer coordination

---

## 14. Open questions deferred to Slice 2+

These are flagged for future slices, not for Slice 1:

- How aggregates use `aggregate_registry.consumers_needed` (already in schema) to declare their per-consumer dependencies, and how this is enforced at materialization time (Slice 2).
- Materialized `block_seal` table for perf vs derived `is_sealed(H)` query (Slice 2 decides based on perf).
- How `bucket_seal.sealed_by_consumers` (already in schema) gets populated when an aggregate bucket seals (Slice 2).
- `processed_heights` at scale (current schema is a regular table; mainnet alone is ~635k rows per consumer, growing). Hypertable conversion or partition pruning becomes relevant in Phase 2 when 8+ consumers run; decision can wait until perf surfaces in Slice 2 sealing queries.
- Per-block-tx batching (ADR-024) vs single-msg-tx in Slice 1. Single-msg-tx is correctness-correct but may be too slow to replay long block ranges in integration tests. If Phase D/E tests reveal it as a bottleneck, evaluate minimal batching here; otherwise defer the full feature to Phase 2.
- Active self-heal via chain RPC injection (Slice 4).
- NATS stream sizing and retention policy for production (Slice 4 / Phase 3).
- Backpressure tuning if consumers lag (Slice 4).
- Reconciler full feature set (entity drift detection) (Slice 4).

---

## 15. Exit criterion summary

Slice 1 is "done" when:

1. All 27 test scenarios (Section 9) are green.
2. 32 decoder packages compile and pass per-version golden tests against curated fixtures.
3. Block + supplier consumers running in parallel against real archeology data.
4. Per-height seal computed correctly via dynamic `required_set(H)` query, validated multi-network (mainnet config + localnet config tests).
5. `ps sync-upgrades` populates `upgrades` table from mainnet RPC; reconciler refreshes periodically.
6. Passive self-heal: gap injection scenario closes correctly when the missing height arrives.
7. `make ci` clean; coverage ≥80% on internal/, 100% on decoders.

When Slice 1 is done, Slice 2 (aggregates) can begin — the orchestration foundation, the data layer, and the multi-version mechanism will all be proven.

---

**Phase A complete**: branch slice-1/phase-a — Tilt stack green (postgres + nats), 244-table schema applied via goose, timescaledb extension live, NATS JetStream healthy, make ci green. Ready for Phase B (Layer 0 orchestration skeleton).
