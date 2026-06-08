# Can poktroll Shannon mainnet sync from genesis?

> **Short answer**: No. The canonical chain history is non-deterministic in the
> v0.1.15 / v0.1.16 binary lifetime — at least one block (96610) cannot be
> re-derived from genesis with any released binary. PNF mitigation is **snapshot
> distribution**. The canonical chain history *is* fully queryable via Sauron
> RPC/LCD; it just cannot be re-executed locally.
>
> Empirical investigation date: **2026-05-22**.
> Outcome: hypothesis **H3** confirmed (works with manual intervention).
> Resulting decision: [ADR-021](../decisions/ADR-021-shannon-history-discontinuity.md).

---

## Why this matters for PocketScribe

Several pieces of PocketScribe's design assumed genesis-to-tip sync was feasible:

- `docs/architecture/09-backfill.md` — assumed fresh archive node with FilePlugin syncs 1 → head.
- ADR-019 (partial history) — `genesis_kind: 'genesis_json'` assumed reachable height 1.
- Reconciler — assumed any historical height is queryable (this part survives).

The investigation was triggered by Otto (PNF protocol lead) flagging the discontinuity. ADR-021 captures the design consequence.

---

## Track A — Upstream issue investigation

### Source: [pokt-network/poktroll#1481](https://github.com/pokt-network/poktroll/issues/1481)

- **Title**: `[MainNet] Non-deterministic historical block`
- **State**: open (as of 2026-05-22)
- **Author / co-owner**: @Olshansk, @okdas
- **Created**: 2025-06-12 / **Last updated**: 2025-06-30
- **Milestone**: "RelayMiner Stability"; **Type**: Bug; **Labels**: `protocol`, `upgrade`

### Problem summary

- Mainnet (`chain_id = pocket`) cannot be fully synced from genesis because at least one historical block is **non-deterministic**: re-executing it produces a different `LastResultsHash` than the canonical chain.
- Exact replay error captured by @Olshansk after `pocketd rollback` to height 96609 and re-applying 96610:

  ```
  Error in validation err="wrong Block.Header.LastResultsHash.
   Expected 411B9F82AC965528EE3D9A9D4CF1B28D8038A5B8C90648EBF631E69D91053344,
   got      C8D12F54E800AB34DAAEE36A6AFD84A7D308023AA667C6EF3659A179AB65A6DB"
  ```

- **Root cause**: between v0.1.15 (applied @ 94370) and v0.1.16 (applied @ 99293), the function `MorseClaimableAccount.GetEstimatedUnbondingEndHeight` used `time.Until(m.UnstakingTime)` — wall-clock time on the validator, not `sdkCtx.BlockTime()`. That path was hit by `MsgClaimMorseSupplier` in tx `07F9F26146FDCAA701BAF7743854D89540A6187363C8E218E541D7F789114FFD` (height 96610), emitting `EventSupplierUnbondingEnd` with results that depend on the validator's clock at original execution.
- **Fix**: PR [#1436](https://github.com/pokt-network/poktroll/pull/1436) replaces `time.Until` with `sdkCtx.BlockTime()`. Landed in **v0.1.17** (applied @ 102142). Historical bad blocks remain on chain and cannot be re-derived.
- **Affected window**: technically `[94370, 102141]` (lifetime of the buggy binaries). Only h=96610 is confirmed bad, but any other tx in that range hitting the same path is at risk.

### Mitigation discussions (from issue thread)

| Proposal | Author | Status |
|---|---|---|
| Recompile v0.1.15/v0.1.16 with a hardcoded `LegacyShimFixHeight` lookup table of recorded historical outcomes ("blockchain shim") | @Olshansk | designed, not implemented |
| Full-node `--halt-height=94369`, `cosmovisor add-upgrade`, manual step-through; test node at `137.220.40.229` halted at 94369 | @okdas (2025-06-13) | paused |
| **Distribute state snapshots past the bad window** | @Olshansk (2025-06-16) | **official mitigation** — no further work on the issue since |

### Related PRs / issues

| Ref | Title | State |
|---|---|---|
| `#1436` | Deterministic `GetEstimatedUnbondingEndHeight` (uses `BlockTime`) | merged → v0.1.17 |
| `#889` | TLM isolation, earlier non-determinism hardening | merged 2024-11-19 |
| Release `v0.1.17` | Contains the fix | published |

Broader searches for "sync", "genesis", "halt", "AppHash mismatch" surfaced no additional open or closed issues describing a separate discontinuity beyond #1481.

---

## Track A — Empirical probes against Sauron

All probes performed 2026-05-22.

### Sauron mainnet (`https://sauron-rpc.infra.pocket.network`)

`/status`:

| Field | Value |
|---|---|
| `node_info.network` (chain_id) | `pocket` |
| `node_info.id` | `0ef6de745dec386259a1684b3fb766cdf9fc2e1c` (`pnf-seed-one`) — RPC peer |
| `node_info.id` (P2P seed) | `e3f1a09e045433199c94172ef0d6fc9ab7212ad7` (`pnf-seed-two`) — see Track B |
| `tendermint_version` | `0.38.19` |
| `earliest_block_height` | **1** |
| `earliest_block_time` | `2025-03-28T17:35:52.277346Z` |
| `latest_block_height` | **764939** |
| `latest_block_time` | `2026-05-22T18:33:35.772Z` |
| `catching_up` | false |

`/block?height=H` — all returned 200 OK with valid block data:

| Height | Notable |
|---|---|
| 1 | genesis +1 |
| 100, 1000, 10000 | OK |
| 94369 | last block under v0.1.14 |
| 94370 | v0.1.15 applied |
| 96608, 96609, **96610**, 96611 | **including the non-deterministic block** — Sauron has it |
| 99293 | v0.1.16 applied |
| 100000 | OK |
| 102142 | v0.1.17 applied (first deterministic binary) |
| 500000, 700000, 764939 | tip and recent |

LCD `/cosmos/base/tendermint/v1beta1/blocks/1` returns h=1 successfully.

**No `height is not available` errors anywhere across the probed range.** Sauron's archive carries the full canonical history.

### Sauron beta — `pocket-lego-testnet` (`https://sauron-rpc.beta.infra.pocket.network`)

| Field | Value |
|---|---|
| `node_info.network` (chain_id) | `pocket-lego-testnet` (NOT "beta" or "pocket-beta") |
| `node_info.id` | `0d4ddde51763ec62803c3318c90f9ab0854f90ec` (`pnf-lego-validator-one`) |
| `earliest_block_height` | **1** |
| `earliest_block_time` | `2025-04-02T18:47:17Z` |
| `latest_block_height` | 321577 |

Heights 1, 100, 1000, 10000, 100000, 321577 — all 200 OK. Full archive available; no known discontinuity on this testnet (only one applied upgrade, v0.1.33 @ 153479; chain was provisioned at a v0.1.32-era binary).

---

## Track B — Chain-authoritative upgrade history & bootstrap data

### Mainnet upgrade heights (via Sauron LCD `applied_plan/{name}`)

| Version | Height | Notes |
|---:|---|---|
| v0.1.0 | (genesis binary) | covers 1 → 78620 |
| v0.1.1 | none | skipped |
| v0.1.2 | 78621 | first applied upgrade |
| v0.1.3 | 78632 | |
| v0.1.4 | 78641 | |
| v0.1.5 | 78654 | |
| v0.1.6 | 78659 | |
| v0.1.7 | 78665 | |
| v0.1.8 | 78671 | |
| v0.1.9 | 78678 | |
| v0.1.10 | 78683 | |
| v0.1.11 | 78689 | |
| v0.1.12 | 78697 | |
| v0.1.13 | 80510 | |
| v0.1.14 | 93825 | |
| **v0.1.15** | **94370** | **start of non-deterministic window** |
| **v0.1.16** | **99293** | still non-deterministic |
| **v0.1.17** | **102142** | **fix lands (PR #1436); end of non-deterministic window** |
| v0.1.18 | 116100 | |
| v0.1.19 | 117454 | |
| v0.1.20 | 135297 | |
| v0.1.21 | 138931 | |
| v0.1.22 | 155173 | |
| v0.1.23 | 161109 | |
| v0.1.24 | 161169 | |
| v0.1.25 | 190974 | |
| v0.1.26 | 190979 | |
| v0.1.27 | 247893 | (repo's proposed height was 247939 — 46 blocks off) |
| v0.1.28 | 287932 | |
| v0.1.29 | 382250 | |
| v0.1.30 | 484473 | |
| v0.1.31 | 635506 | |
| v0.1.32 | (skipped on mainnet) | no applied_plan |
| **v0.1.33** | **703870** | latest |

### Beta (`pocket-lego-testnet`) upgrade heights

Only **v0.1.33 → 153479** is on chain. All other v0.1.x return no plan — testnet was provisioned at the v0.1.32-era binary.

### Sauron P2P endpoints (for the experiment, if it proceeds)

Both Sauron seeds advertise non-default P2P ports:

| Network | node_id | listen_addr | persistent_peer string |
|---|---|---|---|
| mainnet | `e3f1a09e045433199c94172ef0d6fc9ab7212ad7` | `seed-two.p2p.infra.pocket.network:26663` | `e3f1a09e045433199c94172ef0d6fc9ab7212ad7@seed-two.p2p.infra.pocket.network:26663` |
| beta | `b92242df21c9e5b5140b01e511d0733146dee29c` | `validator-five-lego.p2p.beta.infra.pocket.network:26672` | `b92242df21c9e5b5140b01e511d0733146dee29c@validator-five-lego.p2p.beta.infra.pocket.network:26672` |

### Genesis files

Repo: [`pokt-network/pocket-network-genesis`](https://github.com/pokt-network/pocket-network-genesis) (NOT `pocket-network-resources`; the latter only has README/health-checks at root — common confusion).

| Network | Path | Size | sha256 |
|---|---|---|---|
| mainnet | `shannon/mainnet/genesis.json` | 37139 B | `b4adc3614def79b63e777f90cf0a62cf43ea6a17715bdcd16c982ae8e44abee2` |
| beta | `shannon/testnet-beta/genesis.json` | 26493 B | `06e91f0dfdb3a05ed948efc81891e364c8ae54d1c28e256ed2b3ee5da3fe8183` |

Sauron `/genesis_chunked?chunk=0` works for mainnet — returns `{chunk:"0", total:"1", data:<33716 B base64>}`. Single chunk; can cross-check against the file in the genesis repo.

Verification recipe:

```bash
curl -sf https://raw.githubusercontent.com/pokt-network/pocket-network-genesis/master/shannon/mainnet/genesis.json -o genesis.json
echo "b4adc3614def79b63e777f90cf0a62cf43ea6a17715bdcd16c982ae8e44abee2  genesis.json" | sha256sum -c
# Cross-check against live chain:
curl -sf https://sauron-rpc.infra.pocket.network/genesis_chunked?chunk=0 | jq -r '.result.data' | base64 -d > genesis-from-chain.json
diff <(jq -S . genesis.json) <(jq -S . genesis-from-chain.json)
```

The `seeds` file under `pocket-network-genesis/shannon/testnet-beta/` still points to the old `shannon-grove-seed1.beta.poktroll.com` infra — **do not use**.

---

## Verdict

**H3 — works with manual intervention.** A fresh poktroll node started from `genesis.json` will halt at h=96610 (or any earlier tx hitting the same code path) with a `LastResultsHash` mismatch. The only operational path to a live tip is **bootstrap from a PNF-distributed snapshot past the discontinuity window** (recommended ≥ 102142 = v0.1.17 applied height).

Sauron RPC/LCD/gRPC remain canonical for **all** historical queries — the discontinuity is a replay constraint, not a data-loss event.

---

## Implications for PocketScribe (summary; full treatment in ADR-021)

| Area | Impact |
|---|---|
| ADR-006 — chain as source of truth | **OK in steady state.** Sauron returns canonical chain results at any height. Invariant #3 (no indexer-side derived state) protects us. |
| `docs/architecture/09-backfill.md` — backfill genesis-to-tip | **Broken assumption.** `ps fileplugin` cannot read from a self-syncing-from-genesis node. Live-ingestion bootstrap requires a snapshot-seeded archive node. |
| ADR-019 — partial history | **`synthetic_snapshot` becomes the recommended default** for mainnet; `genesis_json` is acceptable for testnets without discontinuity. `recommended_start_height: 102142` documented in `configs/networks/mainnet.yaml`. |
| Reconciler — historical drift checks | **Unaffected.** Bulk gRPC against Sauron returns canonical state at any height. |
| Golden fixtures | **Source from Sauron RPC**, not from locally-replayed nodes. Fixtures inside `[94370, 102141]` are still valid because we capture the chain's recorded results, not our re-execution of them. |

---

## Endpoint policy (consequence)

PNF-maintained Sauron is the **only** canonical endpoint set. Previous-team `*.poktroll.com` / `shannon-grove-*` endpoints are not maintained and are not used in configs.

| Network | RPC | LCD |
|---|---|---|
| mainnet | `https://sauron-rpc.infra.pocket.network` | `https://sauron-api.infra.pocket.network` |
| beta | `https://sauron-rpc.beta.infra.pocket.network` | `https://sauron-api.beta.infra.pocket.network` |

This is reflected in `configs/networks/mainnet.yaml` and `configs/networks/beta.yaml`.

---

## Related work — what to do next

- **Track upstream**: subscribe to poktroll#1481 for status of the "blockchain shim". If it ever ships, this ADR can be relaxed.
- **Snapshot library** (subproduct of `experiments/sync-from-genesis/`): even though full genesis sync doesn't work, capturing per-version datadir snapshots from the snapshot-bootstrapped node is valuable. Outputs:
  - Per-binary-version datadir tarballs → fast bootstrap for new archive nodes.
  - Per-binary-version FilePlugin output tarballs → golden fixture material for decoder version tests.
- **Operations runbook** (`docs/operations/sync-from-genesis.md` — TODO): document the snapshot bootstrap procedure once PNF publishes a stable snapshot URL.
- **`ps doctor`**: extend with a check that the configured poktroll node's earliest block ≥ recommended_start_height (warn otherwise).

---

## References

- [poktroll#1481](https://github.com/pokt-network/poktroll/issues/1481) — Non-deterministic historical block (mainnet)
- [poktroll#1436](https://github.com/pokt-network/poktroll/pull/1436) — Fix: use `BlockTime()` for unbonding end height (in v0.1.17)
- [`pocket-network-genesis`](https://github.com/pokt-network/pocket-network-genesis) — canonical genesis files
- ADR-006, ADR-019, ADR-020, **ADR-021**
- `experiments/sync-from-genesis/` — empirical experiment scaffolding + `results.md`
- `docs/research/poktroll-versions.md` — chain-authoritative version table
- Sauron `/status`, `/block`, `/genesis_chunked`, LCD `/cosmos/upgrade/v1beta1/applied_plan/{name}`
