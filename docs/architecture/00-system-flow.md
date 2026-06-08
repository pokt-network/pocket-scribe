# PocketScribe — System Flow

> The high-level view. What PocketScribe is, what it produces, and how the
> pieces talk to each other. Each diagram below is a different lens on the
> same system.

## What is PocketScribe?

**A Go-native indexer for Pocket Network's Shannon protocol.** It turns the
firehose of chain events into a queryable Postgres + TimescaleDB store, with
GraphQL (Hasura), REST (PostgREST), and realtime (NATS WebSocket bridge)
endpoints hanging off it. It replaces the SubQuery-based Pocketdex with
something that is:

- **Stream-first** — every block flows through NATS JetStream, never polled.
- **Append-only** — entity history is a sequence of snapshots; never UPDATE,
  never `valid_to_height`.
- **Version-aware** — one decoder per poktroll release, dispatched per block
  by an upgrades table.
- **Self-hosted** — no cloud lock-in, no managed analytics DB.
- **Reproducible** — replay any height range from the canonical FilePlugin
  archive without re-syncing the chain.

---

## Diagram 1 — Live ingestion (the runtime path)

```mermaid
flowchart LR
    subgraph chain ["Pocket Network (Shannon)"]
        Node["poktroll archive node<br/>(FilePlugin enabled)"]
    end

    subgraph ingest ["PocketScribe ingestion"]
        Files["fileplugin-output/<br/>block-N-data<br/>block-N-meta"]
        Sidecar["ps fileplugin<br/>(sidecar)"]
        NATS["NATS JetStream<br/>(dedup by Nats-Msg-Id,<br/>30-day retention)"]
    end

    subgraph process ["Per-module consumers (Go)"]
        Router["Decoder router<br/>(reads upgrades table,<br/>picks decoder per height)"]
        ConSupplier["ps consumer supplier"]
        ConTokenomics["ps consumer tokenomics"]
        ConOther["ps consumer ..."]
    end

    subgraph storage ["PostgreSQL + TimescaleDB"]
        State["supplier_history<br/>application_history<br/>(append-only)"]
        Events["event_claim_settled<br/>(hypertables)"]
        Aggs["claims_hourly<br/>(continuous aggregates,<br/>gap-aware sealing)"]
    end

    subgraph expose ["Downstream APIs"]
        Hasura["Hasura<br/>(GraphQL)"]
        PostgREST["PostgREST<br/>(REST + OpenAPI)"]
        WS["NATS WS bridge<br/>(realtime push)"]
    end

    subgraph clients ["Consumers"]
        UI["Apps / dashboards"]
        Bots["Bots / scripts"]
    end

    Node -- "writes per-block files" --> Files
    Files -- "tail + publish" --> Sidecar
    Sidecar -- "pokt.chain.blocks" --> NATS
    NATS -- "pokt.events.&lt;module&gt;.&gt;" --> Router
    Router --> ConSupplier
    Router --> ConTokenomics
    Router --> ConOther
    ConSupplier -- "upsert + cursor advance<br/>in same tx" --> State
    ConTokenomics --> Events
    ConOther --> State
    Events -. "sealing loop" .-> Aggs
    State --> Hasura
    State --> PostgREST
    Events --> Hasura
    Events --> PostgREST
    NATS --> WS
    Hasura --> UI
    PostgREST --> UI
    PostgREST --> Bots
    WS --> UI
```

**Key invariants** (see [ADR-005](../decisions/ADR-005-append-only-pure.md),
[ADR-006](../decisions/ADR-006-chain-as-source-of-truth.md),
[ADR-010](../decisions/ADR-010-height-and-time-invariant.md)):

- Every row carries `(block_height, block_time)` from the chain consensus header.
- Consumer pattern: BEGIN tx → upsert → cursor advance → COMMIT → THEN ack NATS.
  Crash anywhere → idempotent replay → no duplicates, no gaps.
- "Current" = `DISTINCT ON (id) ... ORDER BY block_height DESC` view over
  the history table. No materialized "current" column.

---

## Diagram 2 — Schema generation pipeline (the unique part)

PocketScribe's schema is **generated from poktroll's protobuf shapes**, not
hand-authored. When poktroll releases a new version, three skills produce
the new SQL — no human writes a CREATE TABLE.

```mermaid
flowchart TB
    subgraph upstream ["Upstream"]
        Pokt["pokt-network/poktroll<br/>vX.Y.Z release"]
        Cosmos["cosmos/cosmos-sdk<br/>(version detected from poktroll's go.mod)"]
    end

    subgraph vendor ["Vendor (one-time per version)"]
        VendorPokt["third_party/proto/poktroll/vX_Y_Z/"]
        VendorCosmos["third_party/proto/cosmos-sdk/v0_53_0/<br/>(dedup across poktroll releases)"]
    end

    subgraph extract ["Extract (per version)"]
        Snapshot["docs/research/.shapes/vX_Y_Z.json<br/>(every message, every field,<br/>+ proto docstring comments)"]
    end

    subgraph diff ["Diff (vs previous version)"]
        Evolution["docs/research/spine-shape-evolution.md<br/>(curated human-readable log)"]
        DiffJSON["(diff computed on demand<br/>from snapshots, not persisted)"]
    end

    subgraph migrate ["Migrate"]
        Config["config.yaml<br/>(entities + auto_include rules)"]
        Migration["schema/migrations/00NN_decoder_vX_Y_Z.sql<br/>(idempotent goose migration)"]
    end

    subgraph verify ["Verify"]
        DB[("TimescaleDB<br/>(disposable)")]
    end

    Pokt -- "/generate-decoder vX.Y.Z" --> VendorPokt
    Pokt -- "reads go.mod" --> Cosmos
    Cosmos -- "vendor if not yet" --> VendorCosmos
    VendorPokt --> Snapshot
    VendorCosmos --> Snapshot
    Snapshot -- "diff vs prev snapshot" --> Evolution
    Snapshot --> DiffJSON
    DiffJSON -- "/generate-migration-from-diff vX.Y.Z" --> Migration
    Config -- "entities, denom aliases,<br/>id_fields, patterns" --> Migration
    Migration -- "/verify-migrations" --> DB
    DB -- "OK | error" --> Migration
```

**Result**: schema covers 244 tables across both poktroll-specific entities
(Supplier, Application, EventClaimSettled, …) and cosmos-sdk core (BaseAccount,
Validator, Proposal, …). All idempotent. All validated against TimescaleDB.

See [ADR-028](../decisions/ADR-028-schema-versioning-strategy.md) for the
full design.

---

## Diagram 3 — Decoder version routing (runtime)

Every block lives in exactly one version range. The router reads the
`upgrades` table (chain-driven, see [ADR-018](../decisions/ADR-018-no-hardcoded-upgrades.md))
and dispatches the block's bytes to the right per-version decoder package.

```mermaid
flowchart LR
    BlockBytes["Block bytes<br/>(from NATS)"] --> Router

    subgraph router ["Decoder router"]
        Router["Read upgrades table:<br/>height → decoder version"]
    end

    Router -- "h &lt; 78621" --> Dec_v0_1_0["internal/decoders/v0_1_0/<br/>(Go package, vendored protos)"]
    Router -- "78621 ≤ h ≤ 80509" --> Dec_v0_1_2["internal/decoders/v0_1_2/<br/>... v0.1.11"]
    Router -- "h ≥ 78697 ≤ 102141" --> Dec_v0_1_12["internal/decoders/v0_1_12/<br/>... v0.1.16 (cosmos-sdk v0.53 bump)"]
    Router -- "h ≥ 247893 ≤ 287931" --> Dec_v0_1_27["internal/decoders/v0_1_27/<br/>(major EventClaimSettled refactor)"]
    Router -- "h ≥ 703870" --> Dec_v0_1_33["internal/decoders/v0_1_33/<br/>(current live binary)"]

    Dec_v0_1_0 -- "writes" --> Rows
    Dec_v0_1_2 -- "writes" --> Rows
    Dec_v0_1_12 -- "writes" --> Rows
    Dec_v0_1_27 -- "writes" --> Rows
    Dec_v0_1_33 -- "writes" --> Rows
    Rows[("supplier_history<br/>event_claim_settled<br/>(append-only,<br/>decoded_by_version per row)")]
```

The `decoded_by_version` column on every row provides indelible audit:
**"this row was interpreted by this version's Go code"**. Debugging a NULL
field reduces to "did this version's shape even have it?" — answer in O(1).

---

## Diagram 4 — Archeology pipeline (how we got the initial substrate)

A one-shot effort: capture per-version FilePlugin output for every poktroll
release that ran on mainnet. The output is uploaded to a Hetzner bucket
and is the canonical input PocketScribe replays — there's no way to
re-derive it from genesis because of the known historical replay
discontinuity (see [ADR-021](../decisions/ADR-021-shannon-history-discontinuity.md)).

```mermaid
flowchart LR
    subgraph build ["Build (one-time per version)"]
        Source["pokt-network/poktroll<br/>vX.Y.Z source"]
        Patches["archeology/patches/<br/>(FilePlugin streaming-file fix,<br/>+ MorseClaimableAccount shim<br/>for v0.1.15/v0.1.16)"]
        Binary["archeology/binaries/vX.Y.Z/pocketd<br/>(patched, Git LFS)"]
    end

    subgraph capture ["Capture (orchestrator loop)"]
        Orch["archeology/scripts/orchestrator.sh<br/>(idempotent, tip-mode aware)"]
        Run["Run pocketd vX.Y.Z<br/>until runs_until height (or tip)"]
        Snapshot["snapshot datadir<br/>+ fileplugin-output"]
    end

    subgraph distribute ["Distribute"]
        Bucket[("Hetzner Object Storage<br/>pocketscribe-mainnet-archeology<br/>per-version: datadir.tar.xz,<br/>fileplugin.tar.xz, sha256")]
    end

    subgraph reuse ["Reuse (PocketScribe consumers)"]
        Download["Download<br/>fileplugin.tar.xz"]
        Replay["ps fileplugin sidecar<br/>replays into NATS"]
        Indexer["ps consumer &lt;module&gt;<br/>indexes into Postgres<br/>(same code path as live)"]
    end

    Source --> Patches
    Patches --> Binary
    Binary --> Run
    Orch -- "drives loop" --> Run
    Run --> Snapshot
    Snapshot -- "rclone upload" --> Bucket
    Bucket --> Download
    Download --> Replay
    Replay --> Indexer
```

After v0.1.34 goes live, future captures switch to the **stock** poktroll
binary (Otto's FilePlugin fix landed in mainline); the patched-binary
archeology layer becomes obsolete and we move to live-tail capture.

See [archeology/README.md](../../archeology/README.md) and
[archeology/FINDINGS.md](../../archeology/FINDINGS.md) for the operational
detail.

---

## How to read the rest of the docs

| Topic | Doc |
|---|---|
| Why these choices? | [`docs/decisions/`](../decisions/) — 22 ADRs |
| Detailed subsystem design | [`docs/architecture/`](.) — 10 documents |
| Investigation notes | [`docs/research/`](../research/) |
| What exists today (vs planned) | [`STATUS.md`](../../STATUS.md) |
| How a contributor adds a module | [`CONTRIBUTING.md`](../../CONTRIBUTING.md) |
| The full plan | [`ROADMAP.md`](../../ROADMAP.md) |
| Schema artifacts | [`schema/migrations/`](../../schema/migrations/) — 38 files |
| Skill internals | [`.claude/skills/`](../../.claude/skills/) — 4 skills |
| Archeology run output | [`archeology/`](../../archeology/) — patches, binaries (LFS), scripts |
