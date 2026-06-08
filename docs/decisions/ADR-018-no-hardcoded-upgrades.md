# ADR-018: No hardcoded upgrade heights — `upgrades` table is DB-driven from chain

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude
**Supersedes**: Hardcoded data in earlier draft of `internal/router/upgrades.go` and `schema/migrations/0004_populate_upgrades.sql`.

## Context

An initial implementation hardcoded the Shannon mainnet upgrade history in:
- `internal/router/upgrades.go` (`DefaultMainnetUpgrades` slice)
- `schema/migrations/0004_populate_upgrades.sql` (INSERT statements)

The data came from the poktroll repo's `tools/scripts/upgrades/upgrade_tx_*_main.json` files. This had **two fatal flaws**:

### Flaw 1: Repo data is the *proposed* height, not the *applied* height

When governance submits a `MsgSoftwareUpgrade` tx, the JSON file holds the proposed `plan.height`. But the actual applied height can differ:
- The tx may be submitted after the proposed height, and the chain re-aligns.
- Direct chain-state queries show the truly-applied height.

Observed: **v0.1.27** — repo said 247939, chain (via `/cosmos/upgrade/v1beta1/applied_plan/v0.1.27`) said **247893**. 46-block discrepancy. Silent. The repo was never updated.

Observed: **v0.1.33** — repo file had `"UPDATE_ME"` placeholder. Chain reported **703870** as actually applied.

### Flaw 2: Hardcoded mainnet ≠ usable on other networks

Hardcoding mainnet heights means PocketScribe **cannot index any other Pocket network** (beta testnet, alpha testnet, internal devnets, forks) without a code change.

Observed: **Beta testnet** at https://sauron-rpc.beta.infra.pocket.network has a different `chain_id` (`pocket-lego-testnet`, vs mainnet's `pocket`) and a completely different history:

> **Correction**: an earlier draft of this ADR incorrectly claimed mainnet and beta shared the chain_id `"pocket"`. That came from confusing `/abci_info .data` (which returns the app name `pocketd` → `"pocket"`) with `/status .node_info.network` (which returns the real chain_id). The authoritative source for chain_id is `/status .node_info.network`. Verified 2026-05-22:
> - Mainnet: `pocket`
> - Beta: `pocket-lego-testnet`
- Genesis: 2025-04-02 (4 days after mainnet)
- Earliest applied upgrade: v0.1.33 at height **153479** (mainnet had this same upgrade at 703870)
- All earlier mainnet upgrades (v0.1.2 → v0.1.31) **never applied to beta** — beta was reset/re-genesised multiple times.

A hardcoded mainnet table indexing beta would route every block before 153479 to `v0_1_31` (wrong; should be `v0_1_0` genesis binary).

## Decision

1. **No hardcoded upgrade data anywhere in the code or migrations.**
2. **The `upgrades` table in Postgres is the single source of truth**, populated by querying the connected chain.
3. **New CLI subcommand `ps sync-upgrades`** queries `/cosmos/upgrade/v1beta1/applied_plan/{name}` for a configurable list of names and upserts the `upgrades` table.
4. **The reconciler** periodically re-syncs (every N minutes) to catch new upgrades automatically.
5. **`internal/router/router.go`** loads upgrades from the DB on startup and on refresh; never has hardcoded data.
6. **Network configs** at `configs/networks/<name>.yaml` describe how to reach each chain (RPC, LCD, genesis info).

## Consequences

### Positive

- **Single source of truth**: the chain itself. No silent drift.
- **Multi-network support**: one PocketScribe deployment per chain. Just change config.
- **New upgrades surface automatically**: reconciler picks them up; no code change.
- **No "repo is the source" trap**: forced discipline of querying the chain.
- **Bootstrap is explicit**: `ps migrate up && ps sync-upgrades && ps consumer ...`.

### Negative

- **Bootstrap requires the chain to be reachable.** If you can't query the chain at startup, you can't populate the upgrades table. Mitigation: cache the table in DB; only re-sync periodically (chain doesn't need to be up for routing once cached).
- **CI tests need static fixtures**: tests use `NewStaticRouter(slice)` rather than the DB.

### Neutral

- Per-network config (`configs/networks/*.yaml`) is one more thing to maintain — but it's small (RPC endpoints + genesis info) and rarely changes.

## Alternatives considered

### Option A: Keep hardcoded mainnet as default, allow override via config
- Pro: works out-of-the-box for mainnet.
- Con: still requires code changes for new mainnet upgrades.
- Con: invites silent drift between hardcoded and chain.
- **Rejected**: any hardcoded data tends to rot.

### Option B: Hardcode per-network (mainnet, beta, alpha) in code
- Pro: zero startup query.
- Con: every operator's custom network requires a code fork.
- Con: still subject to drift; we observed it on mainnet itself.
- **Rejected**: hardcoded data tends to rot, regardless of network count.

### Option C: Read from repo's `upgrade_tx_*_main.json` at runtime
- Pro: structured upstream data.
- Con: the JSONs lag the chain by days/weeks.
- Con: testnet upgrades use `_beta.json`/`_alpha.json` files with similar issues.
- **Rejected**: chain > repo, always.

## Implementation notes

### Files removed / replaced

- `internal/router/upgrades.go` is now empty (just a doc comment + pointer to `router.go`).
- `schema/migrations/0004_populate_upgrades.sql` is a no-op (kept for migration-number stability); the original is preserved as `.OLD` for reference.

### New artifacts

- `internal/router/router.go` — DB-backed Router with `Refresh()`, `DecoderVersionForHeight()`, `UpgradeForHeight()`, `Snapshot()`.
- `internal/router/router_test.go` — table-driven tests using `NewStaticRouter([]Upgrade{...})` snapshots.
- `configs/networks/mainnet.yaml`, `beta.yaml`, `alpha.yaml` — endpoint + genesis info per network.
- `ps sync-upgrades` subcommand (TODO Phase 1) — populates `upgrades` from chain.
- `scripts/dev/sync-upgrades-from-chain.sh` — bash equivalent for ad-hoc verification.

### Reconciler responsibilities (extended)

In addition to entity drift detection, the reconciler:
- Every N minutes, queries `/cosmos/upgrade/v1beta1/applied_plan/{name}` for all known names AND a "next-N speculative names" list (e.g., v0.1.34, v0.1.35, v0.1.36) to catch upgrades that haven't been added to config yet.
- Alerts if a new upgrade is detected without a corresponding decoder.
- Alerts if hardcoded fallback differs from chain (drift guard).

### Bootstrap sequence

```bash
# 1. Run migrations (creates the table, empty)
ps migrate up

# 2. Populate upgrades from the connected chain
ps sync-upgrades --config configs/networks/mainnet.yaml

# 3. Verify
ps inspect upgrades

# 4. Start consumers
ps consumer supplier
```

### When the chain is unreachable at bootstrap

The router refuses to start with empty upgrades. To unblock dev / cold-start scenarios:
- Snapshot the `upgrades` table from another PocketScribe instance (`pg_dump -t upgrades`).
- Or manually `INSERT` into `upgrades` with known values (last-resort).

The reconciler will re-verify against the chain as soon as it's reachable.

## References

- User catching the hardcoded-data smell: "mmmm me parece que hay algo raro aqui, no se puede saber eso en el network con el rpc los upgrade?"
- Follow-up: "te veo hardcodeando esos datos, que pasa si te digo que indexamos mainnet y testnet?"
- ADR-006 (chain as source of truth) — generalizes; this ADR applies it to the upgrade table specifically.
- ADR-008 (versioned decoders) — decoders are per-version; this ADR says heights are not.
- `docs/research/poktroll-versions.md` — current chain-authoritative snapshot.
- `configs/networks/*.yaml` — per-network endpoints.
