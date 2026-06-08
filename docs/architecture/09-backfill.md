# 09 — Backfill from Genesis

## The core insight

The same code path handles **live ingestion and historical backfill** — there's no "backfill mode". To backfill from genesis, you run a fresh poktroll node with the FilePlugin enabled and let it sync from genesis. The plugin emits files for every historical block as the node replays them at replay-speed (much faster than blocktime). Sidecar + consumers process them just like live data.

```
Fresh archive node
    │
    │  poktrolld init
    │  cp mainnet-genesis.json $HOME/.poktroll/config/genesis.json
    │  edit app.toml: [streaming.abci] plugin="file" + keys=[...]
    │
    ▼
poktrolld start --p2p.persistent_peers=<mainnet>
    │
    │  Replays every block from 1 to head.
    │  Replay rate: ~100-500 blocks/sec for chains like Shannon
    │  (CPU + IO bound, not consensus-bound).
    │  500k blocks ≈ 2-6 hours wallclock.
    │
    ▼
FilePlugin writes block-{H}-{meta,data} for H = 1, 2, 3, ...
    │
    ▼
ps fileplugin processes them in order, publishes to NATS
    │
    ▼
Consumers process them, write to Postgres
    │
    ▼
Once node catches up to mainnet tip, it's a normal live node.
```

## When (and only when) do you backfill from genesis

- Fresh deployment to a new infrastructure.
- After a catastrophic data loss (Postgres + NATS + archive node all gone).
- When introducing a new entity that we haven't indexed before AND need historical state (handled differently — see below).

Don't backfill from genesis for:
- Adding a new aggregate (use the historical refresh procedure in [04-aggregates.md](./04-aggregates.md)).
- Schema additions to existing tables (additive only; old rows have NULL for new columns).
- Bug fixes affecting a height range (use `ps replay --module=X --from=H1 --to=H2` instead).

## Procedure

### 1. Provision

| Component | Spec |
|---|---|
| Archive node host | 8 CPU, 32 GB RAM, 500 GB NVMe SSD (grows ~50 GB/month) |
| NATS host(s) | 3 × 4 CPU, 8 GB RAM, 500 GB SSD |
| Postgres host | 16 CPU, 64 GB RAM, 2 TB NVMe SSD |

### 2. Initialize the chain node

```bash
poktrolld init backfill-node --chain-id=poktroll
curl -s https://github.com/pokt-network/poktroll-networks/raw/main/mainnet/genesis.json \
    > $HOME/.poktroll/config/genesis.json
```

Configure peers:
```toml
# config.toml
persistent_peers = "<mainnet seed list>"
```

Configure streaming (`app.toml`):
```toml
[streaming]
  [streaming.abci]
    keys = ["supplier","application","gateway","service","session","proof","tokenomics","bank","auth"]
    plugin = "file"
    stop-node-on-err = true

[streamers.file]
write_dir = "/var/lib/poktroll/streaming"
output-metadata = "true"
stop-node-on-error = "true"
fsync = "true"
```

### 3. Prepare PocketScribe

```bash
ps migrate up
```

Genesis state parser (one-time):
```bash
ps backfill --genesis-only \
    --genesis-file=$HOME/.poktroll/config/genesis.json
```

This parses `genesis.json` and populates initial state at `block_height = 0`:
- Accounts, balances
- Initial validators
- Any pre-existing suppliers/apps/gateways
- Module params (`param_history` with `effective_from_height = 0`)

Each row carries `snapshot_method = 'genesis'`.

### 4. Tune for backfill throughput

Sidecar throughput tuning (config):
```yaml
fileplugin:
  batch_size: 500              # bigger publish batches
  parallel_subjects: 8         # concurrent subject publishers
  delete_safety_window: 1000   # keep more files during fast catchup
```

Consumer tuning:
```yaml
consumer:
  batch_inserts: 1000          # COPY-style batch INSERT
  ack_pending_max: 10          # higher parallelism (lose strict ordering; OK for append-only)
```

Sealing loop: **disable during backfill** to avoid wasted work on incomplete history. Re-enable post-tip.

```bash
ps backfill --pause-sealing
```

Reconciler: **disable during backfill**. Live data is what reconciler validates against — historical data is "live" relative to the time it was produced.

```bash
ps backfill --pause-reconciler
```

Postgres tuning:
- Disable compression policy temporarily (compression is expensive during heavy writes).
- Increase `maintenance_work_mem` to 2 GB for faster index builds.
- Set `wal_compression = on` for replication efficiency.
- Increase `max_wal_size` to 8 GB to absorb burst writes.
- After backfill, restore normal config.

### 5. Start the node + observe

```bash
sudo systemctl start poktrolld
ps fileplugin                       # in another window or pod
ps consumer supplier                 # one per module
# ... etc
```

In Grafana, watch:
- `pocketscribe_fileplugin_publish_lag_seconds` — should drop to ~0 as it catches up.
- `pocketscribe_consumer_lag_blocks` — should be steady as fast as the node syncs.
- Disk usage on archive node + Postgres.

### 6. Catchup completion

When `poktrolld status` shows `catching_up: false`, the node is at tip. Then:

```bash
# Re-enable sealing
ps sealing &

# Re-enable reconciler
ps reconciler &

# Optionally enable Timescale compression for chunks > 30 days
psql -c "SELECT add_compression_policy('event_claim_settled', INTERVAL '30 days');"
psql -c "SELECT add_compression_policy('event_proof_updated', INTERVAL '30 days');"
psql -c "SELECT add_compression_policy('mint_burn_op', INTERVAL '30 days');"

# Run initial sealing pass for historical buckets
ps sealing --backfill-buckets --from='2024-01-01' --to=now
```

## Cold archive (long-term replay)

Keep the FilePlugin output files for future reindex without re-syncing the chain.

```bash
# Compress + archive periodically
rsync -av --remove-source-files /var/lib/poktroll/streaming/ \
      /archive/streaming-cold/
gzip -r /archive/streaming-cold/
```

Or pipe to MinIO:
```bash
mc cp -r /var/lib/poktroll/streaming/ pocketscribe-archive/streaming/
```

Storage estimate: ~50 GB compressed per 500k blocks.

When you need to reindex (e.g., new schema):

```bash
# Restore files
mc cp -r pocketscribe-archive/streaming/ /var/lib/poktroll/streaming-replay/

# Point sidecar at restore dir
ps fileplugin --dir=/var/lib/poktroll/streaming-replay/

# Consumers see the historical data, write to fresh DB
```

This avoids the ~2-6 hour chain resync. Replay rate is bounded only by IO + Postgres.

## Partial historical state injection (new entity post-launch)

If you decide to start indexing a module that wasn't previously indexed (say, a new module added in a chain upgrade), you have two options:

### Option A: Re-snapshot from chain at the current height
```bash
ps backfill --module=<new_module> --from-current-state
```

The consumer queries the chain at head, stores snapshots at `block_height = current_head`. No historical state, just current onward.

### Option B: Full replay for that module
```bash
ps backfill --module=<new_module> --from-genesis --keep-others-running
```

The consumer replays NATS (within retention window) or chain history for just this module. Other modules unaffected.

## Estimated wallclock

| Source | Estimate |
|---|---|
| Fresh sync from peers (no plugin) | ~2-4 hours (depends on peer speed) |
| Sync with FilePlugin enabled | ~3-6 hours (adds 20-40% overhead) |
| Sidecar publishing as node syncs | runs concurrently, lags by minutes |
| Consumers processing | depends on parallelism; usually catches up within hours of node tip |
| First sealing pass over historical buckets | 30 min to a few hours depending on how many aggregates |

Plan for ~12 hours wallclock end-to-end for a 500k-block chain.

## Verification post-backfill

1. **Reconciler runs clean**: no drift detected on first cycle after tip.
2. **Spot-check current state**:
   ```bash
   psql -c "SELECT COUNT(*) FROM supplier;"  # should match chain
   poktrolld query supplier list-suppliers --output json | jq '.supplier | length'
   ```
3. **Spot-check historical state**:
   ```bash
   # Pick a known height + entity, compare
   psql -c "SELECT * FROM supplier_history WHERE address='pokt1...' AND block_height <= 100000 ORDER BY block_height DESC LIMIT 1"
   poktrolld query supplier show-supplier pokt1... --height=100000
   ```
4. **Aggregate sanity**: pick a sealed weekly bucket; manually compute expected from raw events; compare with aggregate value.

If any of these diverge: investigate before declaring backfill complete.

## See also

- ADR-006 (chain as source of truth).
- `docs/operations/disaster-recovery.md` (TODO) — when to fully rebuild.
- `docs/research/file-plugin-spec.md` — file format gotchas during heavy backfill.
