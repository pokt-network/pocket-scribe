# ADR-020: Deployment metadata + indexer state tables

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

PocketScribe needs to expose, in one place, the answers to operational questions like:
- Which network is this Postgres indexing? (mainnet / beta / localnet / custom)
- How was it bootstrapped — natural genesis from `genesis.json`, or synthetic snapshot at some height N?
- How far behind the chain are we right now?
- When did the reconciler last run? Did it detect drift?
- Where are the gaps in our `processed_heights`?

These facts live scattered across many tables (`consumer_consolidation`, `bucket_seal`, `cagg_dirty_buckets`, `processed_heights`, the configured network YAML). Stitching them at query time for `ps doctor`, Grafana dashboards, or Hasura clients is slow and error-prone.

Plus: there's no on-chain analog of "this PocketScribe deployment was started by Jorge on 2026-05-22 with `start_height=700000`". That fact lives only in the deployment itself and is needed for sanity checks ("am I pointed at the right Postgres?").

## Decision

Add two tables (both singleton, enforced by PK CHECK):

1. **`deployment`** — immutable per-deployment identity + bootstrap config.
   - `network_id`, `chain_id`, `display_name`
   - `genesis_height`, `genesis_time`, `genesis_kind` (`'genesis_json'` | `'synthetic_snapshot'` | `'archive_replay'`), `genesis_decoder_version`
   - `bootstrapped_at`, `bootstrapped_by_version`, `bootstrapped_by_command`

2. **`indexer_state`** — mutable live metrics (one row, updated periodically).
   - `latest_chain_height`, `latest_chain_time`, `latest_chain_queried_at`
   - `indexed_head_height`, `safe_height`, `indexed_head_updated_at`
   - `lag_blocks`, `lag_seconds`
   - `last_seal_at`, `pending_dirty_buckets`
   - `last_reconciler_run_at`, `last_reconciler_drift_count`
   - `updated_at`, `updated_by`

And two derived views:

- **`gaps`** — per-consumer contiguous-range view of `processed_heights` for "why isn't consolidation advancing?" diagnosis.
- **`deployment_summary`** — denormalized one-row "deployment at a glance" joining `deployment` + `indexer_state`.

## Consequences

### Positive

- **One-query answer to "what is this deployment doing?"** — `SELECT * FROM deployment_summary`.
- **Wrong-cluster protection**: `ps consumer X` startup reads `deployment` and compares `chain_id + genesis_time` against the configured `network.yaml`. Mismatch → refuse to start.
- **Bootstrap is auditable**: `bootstrapped_at`, `bootstrapped_by_version`, `bootstrapped_by_command` provide forensic trail.
- **Genesis kind is explicit**: `genesis_json` vs `synthetic_snapshot` vs `archive_replay` removes ambiguity for queries like "is this a partial-history deployment?"
- **Gap diagnostics** are a one-query view, not a script.
- **Hasura/PostgREST get a clean home view** — `deployment_summary` is what dashboards consume.
- **Prometheus scraping** can join `indexer_state` columns directly into metrics.

### Negative

- **`indexer_state` updates are UPDATEs.** This is documented as the exception to the append-only invariant (alongside `consumer_consolidation.consolidated_up_to`, `bucket_seal.sealed_at`, `aggregate_registry.status`). It's metadata, not chain data — the exception is justified by ergonomics.
- **Singleton tables enforced by PK CHECK** are a slight Postgres quirk. Trivial to live with.

### Neutral

- The state updater is a small loop (~50 LoC) that runs every minute. Can live inside `ps reconciler` or as a dedicated `ps state-updater`. (Phase 1 decision.)

## Alternatives considered

### Option A: No metadata tables; compute everything at query time
- Pro: simpler schema.
- Con: every `ps doctor` invocation joins 6+ tables. Slow.
- Con: no place to store the immutable bootstrap audit (network_id, genesis_kind, etc.).
- **Rejected**: the metadata IS the source of identity; not derivable.

### Option B: Single big "indexer_status" table mixing config + state
- Pro: one table.
- Con: mixing immutable identity with mutable metrics invites bugs (e.g., accidentally UPDATEing `genesis_height`).
- Con: harder to audit "did anyone change the network config?"
- **Rejected**: separation of concerns matters.

### Option C: Store this in a config file, not the DB
- Pro: file is simpler.
- Con: lost when DB is dumped/restored alone (e.g., DR scenarios where pgBackRest restores Postgres but not the host filesystem).
- Con: not exposable via Hasura/PostgREST.
- **Rejected**: the DB is the durable identity, must self-describe.

## Implementation notes

### Bootstrap sequence (updated)

```bash
# 1. Migrate schema (creates deployment, indexer_state tables empty)
ps migrate up

# 2. Sync upgrades from chain (populates `upgrades` table)
ps sync-upgrades --config configs/networks/mainnet.yaml

# 3. Bootstrap state — populates `deployment` row + entity snapshots at start_height
ps bootstrap-state --config configs/networks/mainnet.yaml
#    OR for synthetic:
ps bootstrap-state --config configs/networks/mainnet.yaml --at-height 700000

# 4. Start the state updater (or reconciler, which can include it)
ps reconciler &

# 5. Start consumers — they verify deployment.chain_id == config.chain_id
ps consumer supplier
```

### `ps bootstrap-state` writes:

```sql
INSERT INTO deployment (
  network_id, chain_id, display_name,
  genesis_height, genesis_time, genesis_kind, genesis_decoder_version,
  bootstrapped_at, bootstrapped_by_version, bootstrapped_by_command
) VALUES (
  'pocket-mainnet', 'pocket', 'Shannon Mainnet',
  700000, '2026-05-15T12:34:56Z', 'synthetic_snapshot', 'v0_1_33',
  now(), 'ps v0.0.1-spike', 'ps bootstrap-state --at-height 700000'
) ON CONFLICT (id) DO NOTHING;
```

(`ON CONFLICT DO NOTHING` because re-running bootstrap on an already-bootstrapped deployment is a no-op — to re-bootstrap, drop the database first.)

### Consumer startup safety check (Go pseudocode)

```go
func validateDeployment(ctx context.Context, db *pgxpool.Pool, cfg *NetworkConfig) error {
    var d Deployment
    err := db.QueryRow(ctx, "SELECT chain_id, genesis_time FROM deployment WHERE id=1").
        Scan(&d.ChainID, &d.GenesisTime)
    if err == pgx.ErrNoRows {
        return errors.New("deployment table empty; run `ps bootstrap-state` first")
    }
    if d.ChainID != cfg.Network.ChainID {
        return fmt.Errorf("WRONG CLUSTER: deployment chain_id=%q, config chain_id=%q",
            d.ChainID, cfg.Network.ChainID)
    }
    if !d.GenesisTime.Equal(cfg.Network.GenesisTime) {
        return fmt.Errorf("WRONG NETWORK: deployment genesis=%s, config genesis=%s",
            d.GenesisTime, cfg.Network.GenesisTime)
    }
    return nil
}
```

Every long-running subcommand (consumer, fileplugin, sealing, reconciler) runs this at startup. Fails fast on mismatch.

### State updater loop

A small background goroutine in `ps reconciler` (or standalone `ps state-updater`):

```go
for range time.Tick(60 * time.Second) {
    chainHeight, chainTime, err := chain.LatestHeight(ctx)
    if err != nil {
        db.Exec(ctx, `UPDATE indexer_state SET latest_chain_query_error=$1, latest_chain_queried_at=now() WHERE id=1`, err.Error())
        continue
    }
    db.Exec(ctx, `
      UPDATE indexer_state SET
        latest_chain_height = $1,
        latest_chain_time = $2,
        latest_chain_queried_at = now(),
        latest_chain_query_error = NULL,
        indexed_head_height = (SELECT MAX(height) FROM processed_heights),
        safe_height = (SELECT MIN(consolidated_up_to) FROM consumer_consolidation),
        lag_blocks = $1 - (SELECT MIN(consolidated_up_to) FROM consumer_consolidation),
        lag_seconds = EXTRACT(EPOCH FROM (now() - $2)),
        last_seal_at = (SELECT MAX(sealed_at) FROM bucket_seal),
        pending_dirty_buckets = (SELECT COUNT(*) FROM cagg_dirty_buckets),
        updated_at = now(),
        updated_by = 'state-updater'
      WHERE id = 1
    `, chainHeight, chainTime)
}
```

### `ps doctor` output now sourced from `deployment_summary`

```
$ ps doctor

DEPLOYMENT
  Network:                     pocket-mainnet (Shannon Mainnet)
  Chain ID:                    pocket
  Genesis:                     height=700000 (synthetic_snapshot)
  Genesis decoder:             v0_1_33
  Bootstrapped:                2026-05-15T12:34:56Z by ps v0.0.1-spike

INDEXING STATE
  Latest chain height:         764350 (queried 23s ago)
  Indexed head height:         764347
  Safe height:                 764340
  Lag:                         10 blocks (~600s)
  Last bucket sealed:          22s ago
  Pending dirty buckets:       0
  Last reconciler run:         3m ago — 0 drift

HEALTH
  ✓ Postgres reachable
  ✓ NATS reachable
  ✓ Chain RPC reachable
  ✓ All systems nominal
```

## References

- User request that prompted this ADR: "ok por lo que veo necesitamos una suerte de 'metadata' table que permita saber, el current network height vs el indexed height, gaps, etc, some kind of metrics about the 'state of the indexing' porque entonces por ejemplo, podriamos poner, genesis_height=1 genesis_kind=archive\json\whatever name you think, vs genesis_height=700k, genesis_kind=synthetic."
- ADR-005 (append-only pure) — documents the metadata exception.
- ADR-018 (no hardcoded upgrades) — the `deployment` table is the per-deployment identity that ADR-018 hints at.
- ADR-019 (partial history from height X) — `genesis_kind = 'synthetic_snapshot'` is the realization.
- `schema/migrations/0005_deployment_state.sql` — the actual schema.
