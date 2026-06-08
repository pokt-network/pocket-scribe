# poktroll versions — ground truth for PocketScribe

> **Source of truth: the chain itself.** Heights below are verified against
> the live Pocket Network mainnet via `/cosmos/upgrade/v1beta1/applied_plan/{name}`.
> The repo's `upgrade_tx_*_main.json` files hold *proposed* heights and can
> drift from what was actually applied (we saw a 46-block diff on v0.1.27).
>
> Last verified: 2026-05-22 against `https://sauron-api.infra.pocket.network` (PNF-canonical).
>
> **Endpoint policy**: previous-team `shannon-grove-*.poktroll.com` URLs are
> NOT canonical and are not used by PocketScribe. Sauron is the only source.
> See `docs/research/poktroll-sync-from-genesis.md` for context.

## Chain identity

| Field | Value |
|---|---|
| **chain_id** | `pocket` (verified via `/status .node_info.network`; NOT to be confused with `/abci_info .data` which returns the app name `pocketd`) |
| **initial_height** | `1` |
| **genesis_time** | `2025-03-28T17:35:52.277346Z` |
| **app_name** | `pocketd` |
| **canonical genesis** | https://raw.githubusercontent.com/pokt-network/pocket-network-genesis/refs/heads/master/shannon/mainnet/genesis.json |
| **latest height** (as of 2026-05-22) | 764939 |
| **canonical RPC** (PNF) | `https://sauron-rpc.infra.pocket.network` |
| **canonical LCD** (PNF) | `https://sauron-api.infra.pocket.network` |
| **legacy endpoints** (previous team, NOT canonical) | `shannon-grove-rpc.mainnet.poktroll.com`, `shannon-grove-api.mainnet.poktroll.com` |

## Genesis state (chain_id `pocket`, height 1)

| Module | Initial count |
|---|---|
| auth.accounts | 7 (module accounts only) |
| application.applicationList | 0 |
| supplier.supplierList | 0 |
| gateway.gatewayList | 0 |
| service.serviceList | 0 |
| service.relayMiningDifficultyList | 0 |
| staking.validators | 0 (at genesis_time; bonded shortly after) |

**Key insight**: Shannon mainnet started with **no business entities** — applications, suppliers, gateways, services, sessions all grew from zero post-launch. PocketScribe's genesis parser only needs to handle the 7 module accounts and the initial params.

Modules present at genesis (all already migrated to `pocket/*` proto package):
`application, auth, authz, bank, circuit, consensus, crisis, distribution, evidence, feegrant, gateway, genutil, gov, group, migration, mint, params, proof, runtime, service, session, shared, slashing, staking, supplier, tokenomics, upgrade, vesting`.

## Mainnet upgrade history (chain-authoritative)

Verified directly against `/cosmos/upgrade/v1beta1/applied_plan/{name}`:

| Mainnet height | Version | Notes |
|---:|---|---|
| **1** | **v0.1.0** | Genesis binary (no upgrade tx — first version). Covers heights 1 → 78620. |
| 78621 | v0.1.2 | First mainnet upgrade. (v0.1.1 was skipped.) |
| 78632 | v0.1.3 | |
| 78641 | v0.1.4 | |
| 78654 | v0.1.5 | "Reduce memory footprint when iterating over Suppliers and Applications" — no state-breaking changes. |
| 78659 | v0.1.6 | |
| 78665 | v0.1.7 | |
| 78671 | v0.1.8 | |
| 78678 | v0.1.9 | |
| 78683 | v0.1.10 | |
| 78689 | v0.1.11 | |
| 78697 | v0.1.12 | |
| 80510 | v0.1.13 | |
| 93825 | v0.1.14 | |
| **94370** | **v0.1.15** | ⚠️ start of non-deterministic-replay window (see ADR-021 / poktroll#1481). |
| **99293** | **v0.1.16** | ⚠️ still inside non-deterministic window. |
| **102142** | **v0.1.17** | ✅ PR #1436 fix lands; deterministic from here on. End of bad window. |
| 116100 | v0.1.18 | |
| 117454 | v0.1.19 | |
| 135297 | v0.1.20 | |
| 138931 | v0.1.21 | |
| 155173 | v0.1.22 | |
| 161109 | v0.1.23 | |
| 161169 | v0.1.24 | |
| 190974 | v0.1.25 | |
| 190979 | v0.1.26 | |
| **247893** | v0.1.27 | **Chain-authoritative**. The repo's `upgrade_tx_v0.1.27_main.json` says 247939 (the *proposed* plan height). Real applied height is **46 blocks earlier**. |
| 287932 | v0.1.28 | |
| 382250 | v0.1.29 | |
| 484473 | v0.1.30 | |
| 635506 | v0.1.31 | |
| (skipped) | v0.1.32 | **Never applied to mainnet.** `applied_plan` returns height=0. |
| **703870** | **v0.1.33** | **Latest applied mainnet upgrade.** (Repo's `upgrade_tx_v0.1.33_main.json` had `"UPDATE_ME"` as height — never refreshed after the actual upgrade.) |

**Total mainnet upgrades**: 31. **Total decoder versions PocketScribe needs**: **32** (genesis v0.1.0 + 31 actual upgrades).

### ⚠️ Historical replay discontinuity (heights 94370..102141)

Per [ADR-021](../decisions/ADR-021-shannon-history-discontinuity.md) and upstream issue [poktroll#1481](https://github.com/pokt-network/poktroll/issues/1481), the v0.1.15/v0.1.16 binaries contained a wall-clock-dependent code path (`MorseClaimableAccount.GetEstimatedUnbondingEndHeight` using `time.Until` instead of `sdkCtx.BlockTime()`). It produced a non-deterministic result on at least one mainnet block (h=96610), and the bad result was committed to chain.

Replaying mainnet from genesis with any released binary therefore **halts at h=96610** with a `LastResultsHash` mismatch. The fix in v0.1.17 (PR #1436) prevents recurrence but cannot retroactively make the historical block reproducible.

**Operational consequence for PocketScribe**: bootstrap from a snapshot, not from genesis. Sauron RPC/LCD/gRPC remain canonical for historical *queries* (they return the chain's committed state regardless of replay determinism); only local re-execution is impaired. Decoder version tests for v0.1.15/v0.1.16 must therefore source goldens from Sauron, not from a locally-replayed node.

## Pre-mainnet (testnet alpha/beta) versions — NOT needed for mainnet indexing

The repo has Go upgrade handlers for these, but no mainnet `applied_plan`:

| Tag | Status |
|---|---|
| v0.0.1 through v0.0.14 | Pre-Shannon alpha/beta testnets only. **Not on mainnet.** |
| v0.1.0 | Mainnet genesis binary (no upgrade tx). |
| v0.1.1 | Skipped (no mainnet upgrade tx; likely a small intermediate fix). |
| v0.1.32 | **Skipped on mainnet.** Tagged in repo but never went live. |

**If you index Pocket Shannon mainnet only**: you need v0.1.0 + the 31 upgrades above (32 decoder versions).

**If you index testnets (Alpha, Beta)**: you also need the v0.0.x line and per-testnet upgrade tx files (in `upgrade_tx_*_alpha.json` and `upgrade_tx_*_beta.json`).

## How to refresh this data

Use the canonical script (don't trust the repo):

```bash
# Authoritative re-sync from chain (writes /tmp/*.fragment files to review)
scripts/dev/sync-upgrades-from-chain.sh

# Drift check (CI gate)
scripts/dev/sync-upgrades-from-chain.sh --check
```

Or manually:

```bash
# Query a specific version's applied height
curl -sf https://sauron-api.infra.pocket.network/cosmos/upgrade/v1beta1/applied_plan/v0.1.27 | jq
# → {"height":"247893"}

# Currently pending upgrade (if any)
curl -sf https://sauron-api.infra.pocket.network/cosmos/upgrade/v1beta1/current_plan | jq
```

## Critical proto evolution events

### Package rename: `poktroll/*` → `pocket/*` (between v0.0.14 and v0.1.0)

The proto package path changed from `poktroll/...` (alpha/beta testnets) to `pocket/...` (Shannon mainnet). All `.proto` files were renamed. Generated Go import paths changed accordingly.

**Implication for PocketScribe**: If only indexing mainnet, ignore v0.0.x. Start vendored protos at v0.1.0 with package `pocket/*`.

### Migration module (v0.0.13+)

The `migration` module was added in v0.0.13 — used for Morse-to-Shannon state migration. Present in all v0.1.x versions on mainnet from genesis.

### EventClaimSettled breaking restructure

The `EventClaimSettled` proto in early Shannon had:
- `pocket.proof.Claim claim = 1`
- `uint64 num_relays = 3`
- `uint64 num_claimed_compute_units = 4`
- `uint64 num_estimated_compute_units = 5`
- `cosmos.base.v1beta1.Coin claimed_upokt = 6`
- `ClaimSettlementResult settlement_result = 7`

In v0.1.33, fields 1, 6, 7 are **reserved** (removed). Replaced by:
- `int32 proof_requirement_int = 2` (int instead of enum for storage)
- New fields exploded into per-claim/per-supplier event variants.

PocketScribe canonical type `EventClaimSettled` must be the **union** of all fields ever present, with versioning per row (`proto_version`).

### EventSupplierStaked added `operator_address` (v0.1.33)

v0.1.33 adds `string operator_address = 3` to `EventSupplierStaked`. Aditive change; older decoders leave NULL.

## How PocketScribe uses this data

### Initial population of `upgrades` table (schema migration)

`schema/migrations/0004_populate_upgrades.sql` populates the `upgrades` table with the chain-authoritative heights above.

### Drift verification

The reconciler (`ps reconciler`) periodically queries `/cosmos/upgrade/v1beta1/applied_plan/{name}` for each known upgrade name AND the next-N names (e.g., v0.1.34, v0.1.35) to catch new upgrades automatically. If any drift detected, alerts + auto-syncs.

In CI: `scripts/dev/sync-upgrades-from-chain.sh --check` runs to fail if `internal/router/upgrades.go` has drifted.

### Decoder onboarding (Phase 1 spike)

For the spike: implement decoder for **v0.1.33 only** (current production as of May 2026). The router falls through to v0.1.33 for everything; goldens captured from current mainnet will match.

For Phase 2: implement all 32 versions. Use `/generate-decoder` skill per version.

### Order of priority for decoder implementation

1. **v0.1.33** (current production) — implement first.
2. **v0.1.31** (predecessor) — many active events between 635506 and 703870 still around in queries.
3. **v0.1.0** (genesis) — needed to decode early-history blocks.
4. Fill in the rest as full backfill is planned.

## References

- Repo: https://github.com/pokt-network/poktroll
- Mainnet genesis: https://raw.githubusercontent.com/pokt-network/pocket-network-genesis/refs/heads/master/shannon/mainnet/genesis.json
- Cosmos x/upgrade query proto: `cosmos/upgrade/v1beta1/query.proto`
- Canonical (PNF) RPC: https://sauron-rpc.infra.pocket.network
- Canonical (PNF) LCD: https://sauron-api.infra.pocket.network
- Legacy (previous team — not used): shannon-grove-rpc.mainnet.poktroll.com, shannon-grove-api.mainnet.poktroll.com
- Shannon launch retrospective: https://pocket.network/shannon-launch-retro/
