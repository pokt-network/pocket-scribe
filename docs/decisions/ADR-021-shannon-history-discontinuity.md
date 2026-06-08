# ADR-021: Shannon mainnet history is non-deterministic — snapshot-bootstrap is mandatory

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude
**Supersedes**: amends ADR-006, ADR-019; deprecates the "raw genesis sync" backfill path documented in `docs/architecture/09-backfill.md`.

## Context

Otto (PNF protocol lead) flagged that poktroll Shannon mainnet (chain_id `pocket`) cannot be full-synced from genesis with any released binary. Empirical investigation confirmed the report.

Root cause is documented in upstream issue [pokt-network/poktroll#1481](https://github.com/pokt-network/poktroll/issues/1481):

- Between releases **v0.1.15** (applied at height 94370) and **v0.1.16** (applied at 99293), `MorseClaimableAccount.GetEstimatedUnbondingEndHeight` used `time.Until(m.UnstakingTime)` — wall-clock time on the validator, not `sdkCtx.BlockTime()`.
- That code path was exercised by `MsgClaimMorseSupplier` in tx `07F9F26146FDCAA701BAF7743854D89540A6187363C8E218E541D7F789114FFD` at **height 96610**. The resulting `EventSupplierUnbondingEnd` was recorded into chain state with values that depend on the proposing validator's wall clock at the moment of original execution.
- Replaying that block from genesis on any released binary produces a different `LastResultsHash` than the canonical chain:

  ```
  wrong Block.Header.LastResultsHash.
  Expected 411B9F82AC965528EE3D9A9D4CF1B28D8038A5B8C90648EBF631E69D91053344,
  got      C8D12F54E800AB34DAAEE36A6AFD84A7D308023AA667C6EF3659A179AB65A6DB
  ```

- The non-determinism was fixed in **v0.1.17** (applied at 102142) via [poktroll PR #1436](https://github.com/pokt-network/poktroll/pull/1436) — it now uses `sdkCtx.BlockTime()`. However the bad historical blocks remain on-chain and **cannot be deterministically reproduced** by any released binary.
- A proposed "blockchain shim" (recompile v0.1.15/v0.1.16 with a hardcoded `LegacyShimFixHeight` lookup table of recorded historical outcomes) exists in the issue but is **not implemented**.
- Official PNF mitigation, per @Olshansk 2025-06-16: distribute **state snapshots** past the bad window. There is no plan to enable raw genesis sync.

Empirical probes against Sauron mainnet RPC on 2026-05-22 confirm that **the canonical chain history is fully queryable** — `/block?height=H` returns valid responses for every probed height from 1 to the tip (764939), including 96610. Sauron itself is a full archive that survived the discontinuity by carrying state through the v0.1.15→v0.1.16→v0.1.17 upgrade live, not by re-syncing from genesis.

Affected height window: **[94370, 102141]** — the v0.1.15/v0.1.16 binaries' lifetime. Only one confirmed bad block (96610) but any tx hitting the buggy code path in that range carries the same risk.

## Decision

PocketScribe assumes that **poktroll Shannon mainnet history is not bit-reproducible from genesis**. Concretely:

1. **No deployment path supports `ps fileplugin` against a poktroll node that started from genesis.** Such a node will halt at h=96610 and never reach tip.
2. **The only supported live-ingestion bootstrap is a snapshot-seeded archive node.** Operators bootstrap a poktroll node from a PNF-published snapshot (post-discontinuity, height ≥ 102142), then point `ps fileplugin` at that node's FilePlugin output directory.
3. **For historical state at heights inside or before the discontinuity window**, the indexer queries Sauron RPC/LCD/gRPC — not a self-hosted node. Sauron is canonical for any historical-block lookup because it returns the chain's committed results regardless of replay determinism.
4. **`synthetic_snapshot` becomes the default genesis_kind for mainnet deployments**, per ADR-019/ADR-020. The `genesis_json` path is supported for completeness (and for testnets like `pocket-lego-testnet` where the chain genesis is recent and the discontinuity does not apply), but is no longer the recommended mainnet default.
5. **The reconciler is unaffected.** ADR-006 forbids the indexer from computing derived state (invariant #3); the reconciler reads chain results via bulk gRPC against Sauron, which returns canonical state at any height. Non-deterministic *replay* does not impair *queries* of recorded state.
6. **`configs/networks/mainnet.yaml` documents a `recommended_start_height` of 102142** (the v0.1.17 applied height — first deterministic binary). New deployments without strong reason to backfill earlier history should set `start_height ≥ 102142`.

## Consequences

### Positive

- Operational reality matches documented design. Operators are not silently led into a 24h+ failing sync.
- Snapshot-bootstrap is faster than full sync anyway (~minutes vs hours+).
- Aligns with PNF's actual operating model — Sauron uses snapshots, every validator uses snapshots, we use snapshots.
- The reconciler's "chain is truth" guarantee remains intact because Sauron exposes the canonical recorded state.

### Negative

- PocketScribe operators MUST obtain a trusted snapshot. We don't host one; PNF distribution is the current path.
- Pre-snapshot historical state (heights < snapshot.height) cannot be reconstructed by re-executing the chain. The only way to obtain entity state for those heights is `ps bootstrap-state --at-height=X` against Sauron's bulk gRPC list endpoints (ADR-019 partial-history mechanism). Pre-snapshot per-block *event* history (e.g., individual `EventSupplierStaked` rows from h=50000) is unrecoverable from a Sauron-only path unless Sauron's archive exposes them — at which point we can backfill from RPC `/block_results` rather than a node's FilePlugin.
- The "genesis-to-tip backfill" path in `docs/architecture/09-backfill.md` is now a documentation artefact, not a supported procedure. The doc needs an amendment pointing here.
- Reproducibility tests (golden fixtures from arbitrary historical heights) must source captures from Sauron's RPC, not from a locally-replayed node.
- Future deterministic replay (if the shim ever ships) would relax this ADR. We track upstream issue #1481 and revisit.

### Neutral

- Testnet `pocket-lego-testnet` (Sauron beta) has only **one** applied upgrade (v0.1.33 @ h=153479) — it appears to have been provisioned at a v0.1.32-era binary and is below the historical discontinuity. Beta deployments may use `genesis_json` bootstrap if desired.
- Snapshot distribution doesn't yet have an official PNF URL we can pin in configs. When it does, we add it to `configs/networks/mainnet.yaml`.

## Alternatives considered

### Option A: Pretend genesis sync works and hope it does

- Pro: minimal docs change.
- Con: every fresh operator will hit the h=96610 halt and lose a day.
- **Rejected**: empirically proven to fail.

### Option B: Implement the "blockchain shim" ourselves inside PocketScribe

- Pro: would unlock genesis-to-tip ingestion.
- Con: violates ADR-006 invariant #3 — the indexer would be computing/recording state the chain didn't write deterministically. We'd be re-emitting events with manufactured outcomes.
- Con: an enormous scope creep for a downstream indexer to shim around an upstream protocol bug.
- **Rejected**: not PocketScribe's job. Belongs in poktroll, where the issue is open.

### Option C: Refuse to support mainnet deployments without a known-good snapshot URL

- Pro: forces operators onto the safe path.
- Con: PNF hasn't published a stable snapshot URL we can pin. Hard requirement creates a chicken-and-egg.
- **Deferred**: revisit when PNF publishes. For now, document the requirement in operations runbook and let operators source snapshots themselves.

## Implementation notes

### Config changes

`configs/networks/mainnet.yaml` is updated to:
- Drop all `shannon-grove-*.poktroll.com` endpoints (kept in a comment block as "legacy / not PNF").
- Use only Sauron endpoints.
- Document `recommended_start_height: 102142` as a commented field with rationale.

### Network config schema (future)

When operators routinely use snapshots, extend `network` config:

```yaml
network:
  id: pocket-mainnet
  # ...
  history:
    discontinuity:
      affected_range_low: 94370
      affected_range_high: 102141
      reason: "v0.1.15/v0.1.16 non-deterministic GetEstimatedUnbondingEndHeight; see ADR-021 / poktroll#1481"
    recommended_start_height: 102142
    canonical_archive_rpc: https://sauron-rpc.infra.pocket.network
    snapshot_source: TBD-pnf-snapshot-url
```

(Not implemented yet — schema only added when the bootstrap-state subcommand needs it.)

### Operations runbook

Add a section to `docs/operations/deployment.md` (or a new `sync-from-genesis.md`) documenting:

1. Bootstrap a poktroll archive node from a PNF snapshot (procedure depends on snapshot format).
2. Verify the snapshot is post-discontinuity (`pocketd query block 102142` returns a block).
3. Enable FilePlugin in the node's `app.toml` (`plugin = "file"`).
4. Start `ps fileplugin` against the FilePlugin output directory.
5. For historical state before the snapshot height, use `ps bootstrap-state --at-height=<snapshot.height>` to seed entity tables via Sauron bulk gRPC.

### Test coverage

- Golden fixtures captured from heights inside the discontinuity window (e.g., 96610) come from Sauron `/block` + `/block_results`, NOT from a locally replayed node.
- Decoder version tests for v0.1.15/v0.1.16 explicitly note this and source from Sauron.

## References

- [poktroll#1481 — Non-deterministic historical block (mainnet)](https://github.com/pokt-network/poktroll/issues/1481)
- [poktroll#1436 — Fix: use BlockTime() for unbonding end height](https://github.com/pokt-network/poktroll/pull/1436)
- ADR-006: chain as source of truth (invariant #3 — indexer never computes derived state).
- ADR-009: bucket sealing (unaffected — sealing is at the indexer layer, not chain replay).
- ADR-019: optional partial-history indexing (`start_height`) — provides the mechanism this ADR makes the default mainnet bootstrap.
- ADR-020: deployment metadata (`genesis_kind` is now `synthetic_snapshot` by default for mainnet).
- `docs/research/poktroll-sync-from-genesis.md` — empirical evidence, RPC probes, upgrade height table.
- `experiments/sync-from-genesis/` — the failed sync attempt scaffolding (kept for the snapshot byproducts).
