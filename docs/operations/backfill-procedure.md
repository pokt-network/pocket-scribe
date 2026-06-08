# Backfill Procedure

> Step-by-step playbook for replaying chain history from genesis. See `docs/architecture/09-backfill.md` for the design rationale.

## When to backfill from genesis

- Fresh deployment on new infrastructure.
- Catastrophic data loss (Postgres + NATS + archive node all lost).

**Do NOT** backfill from genesis for:
- Adding a new aggregate → use historical refresh procedure.
- Schema additions → migrations are additive; old rows have NULL for new columns.
- Bug fixes in a height range → use `ps replay --from=H1 --to=H2`.

## Prerequisites

- A working PocketScribe environment (DB, NATS, sidecar, consumers, observability).
- Sufficient disk on the archive node host (300+ GB for Shannon).
- Sufficient disk on the Postgres host (1+ TB recommended for 1-2 years of growth).
- Mainnet seed peer list.
- Latest `genesis.json` for poktroll mainnet.

## Step-by-step

### 1. Provision a fresh archive node

```bash
poktrolld init backfill-node --chain-id=poktroll
curl -s https://example.com/poktroll-mainnet-genesis.json \
    > $HOME/.poktroll/config/genesis.json
```

Edit `$HOME/.poktroll/config/config.toml`:
```toml
persistent_peers = "<mainnet seed list>"
```

### 2. Enable FilePlugin

Edit `$HOME/.poktroll/config/app.toml`:
```toml
[streaming]
  [streaming.abci]
    keys = ["supplier","application","gateway","service","session","proof","tokenomics","bank","auth"]
    plugin = "file"
    stop-node-on-err = true

[streamers.file]
write_dir = "/var/lib/poktroll/streaming"
prefix = ""
output-metadata = "true"
stop-node-on-error = "true"
fsync = "true"
```

### 3. Prepare PocketScribe

```bash
# Apply migrations
ps migrate up

# Parse genesis state (one-time)
ps backfill --genesis-only \
    --genesis-file=$HOME/.poktroll/config/genesis.json
```

The genesis parser populates `block_height = 0` rows with `snapshot_method = 'genesis'` for all entities present in genesis.json.

### 4. Tune for high-throughput backfill

```yaml
# config: backfill mode
fileplugin:
  batch_size: 500
  parallel_subjects: 8
  delete_safety_window: 1000

consumer:
  batch_inserts: 1000
  ack_pending_max: 10

# Pause non-essential services
sealing:
  enabled: false
reconciler:
  enabled: false
```

Postgres tuning (apply to primary):
```sql
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET max_wal_size = '8GB';
SELECT pg_reload_conf();
```

Disable compression policy on hypertables temporarily (compression during heavy writes is slow):
```sql
-- Remove existing policies (if any)
SELECT remove_compression_policy('event_claim_settled');
SELECT remove_compression_policy('event_proof_updated');
SELECT remove_compression_policy('mint_burn_op');
```

### 5. Start everything

```bash
# Archive node
sudo systemctl start poktrolld

# Sidecar (in a pod or local process)
ps fileplugin

# Consumers (one per module)
ps consumer supplier &
ps consumer application &
ps consumer gateway &
ps consumer service &
ps consumer session &
ps consumer tokenomics &
ps consumer bank &
ps consumer authz &
ps consumer validator &
```

### 6. Monitor progress

In Grafana:
- **Sidecar lag** (`pocketscribe_fileplugin_publish_lag_seconds`) — should converge to ~0 as the node catches up.
- **Consumer lag** (`pocketscribe_consumer_lag_blocks`) — should track sidecar publish rate.
- **Disk usage** on archive node + Postgres.
- **NATS stream message rate** + ack pending counts.

Expected wallclock: 6-12 hours for a 500k-block chain at typical hardware.

### 7. Catchup completion

When `poktrolld status | jq .SyncInfo.catching_up` returns `false`:

```bash
# Re-enable sealing
ps sealing &

# Re-enable reconciler
ps reconciler &

# Re-enable compression
psql -c "SELECT add_compression_policy('event_claim_settled', INTERVAL '30 days');"
psql -c "SELECT add_compression_policy('event_proof_updated', INTERVAL '30 days');"
psql -c "SELECT add_compression_policy('mint_burn_op', INTERVAL '30 days');"

# Restore normal Postgres config
psql -c "ALTER SYSTEM SET maintenance_work_mem = '256MB'; SELECT pg_reload_conf();"

# Run sealing backfill for historical buckets
ps sealing --backfill-buckets --from='2024-01-01' --to=now
```

### 8. Validation

```bash
# Entity counts match chain
psql -c "SELECT COUNT(*) FROM supplier"
poktrolld query supplier list-suppliers --output json | jq '.supplier | length'
# numbers should match

# Spot-check historical state at a known height
psql -c "SELECT * FROM supplier_history WHERE address='pokt1...' AND block_height <= 100000 ORDER BY block_height DESC LIMIT 1"
poktrolld query supplier show-supplier pokt1... --height=100000
# fields should match

# Reconciler runs clean
ps reconcile --dry-run
# expected: 0 drifts across all modules
```

### 9. Optionally archive the FilePlugin output

For future replay without re-syncing:

```bash
# Compress and archive
tar -czf /archive/streaming-$(date +%Y%m%d).tar.gz /var/lib/poktroll/streaming/
mc cp /archive/streaming-*.tar.gz pocketscribe-archive/streaming/

# Or rsync incrementally
rsync -av /var/lib/poktroll/streaming/ /archive/streaming-cold/
```

Storage: ~50 GB compressed per 500k blocks.

## Troubleshooting

### Consumer lag grows during backfill

- Increase consumer replicas: `kubectl scale deployment/ps-consumer-supplier --replicas=5`.
- Verify Postgres isn't saturated (CPU, IOPS).
- Check for slow queries: `SELECT * FROM pg_stat_activity WHERE state = 'active' ORDER BY query_start`.

### Archive node falls behind during backfill

- Confirm it has good peer connectivity: `poktrolld status | jq .NodeInfo`.
- Increase `mempool.max_txs_bytes` and `mempool.cache_size` if needed.
- Restart with `--p2p.max_num_inbound_peers=80` and similar tuning.

### NATS retention window exceeded mid-backfill

- Increase MaxAge on the stream: `nats stream edit POKT_CHAIN --max-age=60d`.

### Postgres write IOPS bound

- Move to faster storage (NVMe).
- Tune `checkpoint_timeout`, `max_wal_size`.
- Reduce consumer batch size temporarily (smaller transactions).

### Disk filling on archive node faster than sidecar can publish

- Verify sidecar is healthy and publishing.
- Reduce `delete_safety_window` temporarily (e.g., from 1000 → 100).
- Add disk to streaming volume.

## Partial backfill: new entity post-launch

```bash
# Option A: snapshot from current chain state only
ps backfill --module=<new_module> --from-current-state

# Option B: full history replay for that module
ps backfill --module=<new_module> --from-genesis --keep-others-running
```

## Re-backfill from cold archive (fast)

```bash
# Restore archive
mc cp -r pocketscribe-archive/streaming/ /var/lib/poktroll/streaming-replay/

# Point sidecar at the restore directory
ps fileplugin --dir=/var/lib/poktroll/streaming-replay/

# Consumers process at IO-bound speed (10-100x faster than chain replay)
```

This avoids the ~6-12 hour chain resync. Useful when schema changes require reindexing the full history without changing chain semantics.

## Post-backfill checklist

- [ ] Reconciler reports 0 drifts on first cycle after tip.
- [ ] Spot-checks pass at multiple historical heights.
- [ ] Aggregates sealing successfully (check `bucket_seal` table).
- [ ] Compression policy enabled.
- [ ] Postgres normal config restored.
- [ ] Monitoring shows steady-state metrics.
- [ ] Cold archive captured (if planning future replays).
- [ ] Downstream APIs (Hasura, PostgREST) reconnect and queries work.
