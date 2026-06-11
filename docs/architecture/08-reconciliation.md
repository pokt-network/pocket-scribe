# 08 — Reconciliation: catching drift the chain knows about

## Why reconciliation matters

Even with [chain as source of truth](../decisions/ADR-006-chain-as-source-of-truth.md) (snapshots from KV writes), drift can occur:
- StreamingService misses a write (SDK bug, plugin bug, sidecar bug).
- A consumer crashes between `Commit` and `Ack` — possible? Not in normal flow (idempotent upsert handles it), but a bug in the upsert could leave bad data.
- Mappings have a latent bug that produces wrong snapshots silently.
- A NATS dedup false positive (extremely rare).

The reconciler's job is to **detect drift early and heal it automatically when possible**.

## Design principles

1. **The chain is the oracle.** Reconciler queries the chain (bulk gRPC) and compares against indexed state.
2. **Read-only by default in detection, write-safe in healing.** Healing means inserting an authoritative snapshot — never modifying existing rows.
3. **Bounded resource usage.** Reconciler runs periodically (every 10 min) with bulk queries, not per-entity per-block.
4. **Pluggable per module.** Each module has its own reconcile checker (e.g., `reconcile_supplier`, `reconcile_application`).

## Algorithm

```
loop every N minutes:
  target_height = chain.LatestHeight() - SAFETY_MARGIN   # e.g., head - 10 blocks
  
  for each module in [supplier, application, gateway, service, ...]:
    # 1. Fetch chain state for the module at target_height
    chain_state = chain.bulk_list(module, height=target_height)
    
    # 2. Fetch indexed state for the same height
    indexed_state = db.bulk_at_height(module, height=target_height)
    
    # 3. Compute diff
    mismatches = diff(chain_state, indexed_state)
    
    # 4. Report metrics
    metric.set("reconciler_drift", len(mismatches), tags={"module": module})
    
    # 5. If auto-heal enabled and mismatches < AUTO_HEAL_THRESHOLD:
    if AUTO_HEAL and len(mismatches) < threshold:
      for m in mismatches:
        db.insert_snapshot(module, m.id, target_height, m.chain_state,
                           snapshot_method='reconciler_correction')
        metric.increment("reconciler_auto_heal", tags={"module": module})
    elif len(mismatches) > 0:
      # Many mismatches → probable systemic bug; alert, don't auto-heal
      log.error("massive drift detected", ...)
      metric.increment("reconciler_drift_alert", tags={"module": module})
```

## Implementation: per-module reconciler

Each module provides a `Reconciler` interface:

```go
package reconciler

type ModuleReconciler interface {
    Module() string
    BulkFetchChain(ctx context.Context, height int64) (map[string]*types.Snapshot, error)
    BulkFetchIndexed(ctx context.Context, height int64) (map[string]*types.Snapshot, error)
    Equal(chain, indexed *types.Snapshot) bool
    InsertCorrection(ctx context.Context, height int64, snap *types.Snapshot) error
}
```

For supplier:

```go
package supplier

func (r *Reconciler) BulkFetchChain(ctx context.Context, h int64) (map[string]*types.SupplierSnapshot, error) {
    res, err := r.chainClient.AllSuppliers(ctx, &supplier.QueryAllSuppliersRequest{}, grpc.Header(&md))
    // ... query at height H via gRPC metadata
    out := make(map[string]*types.SupplierSnapshot, len(res.Supplier))
    for _, s := range res.Supplier {
        out[s.OperatorAddress] = adaptToCanonical(s)
    }
    return out, nil
}

func (r *Reconciler) BulkFetchIndexed(ctx context.Context, h int64) (map[string]*types.SupplierSnapshot, error) {
    rows, err := r.db.Query(ctx, `
        SELECT DISTINCT ON (address) ...
        FROM supplier_history
        WHERE block_height <= $1
        ORDER BY address, block_height DESC
    `, h)
    // ...
}

func (r *Reconciler) Equal(chain, indexed *types.SupplierSnapshot) bool {
    return chain.StakeUpokt.Cmp(indexed.StakeUpokt) == 0 &&
           chain.OwnerAddress == indexed.OwnerAddress &&
           reflect.DeepEqual(chain.Services, indexed.Services)
    // ... full struct compare
}
```

## Cadence and budget

- **Default**: every 10 minutes.
- **Per-module budget**: 1 bulk gRPC call per module per cycle. ~9 calls/cycle for the 9 indexed modules.
- **Chain load**: trivial. Bulk list returns all entities (~6k suppliers, ~few hundred apps, etc.) in a single response.
- **Bandwidth**: a few hundred KB per cycle.

Adjustable via config:
```yaml
reconciler:
  interval: 10m
  safety_margin_blocks: 10
  auto_heal: true
  auto_heal_threshold: 10        # if more mismatches than this, alert only
  modules:
    supplier: { enabled: true }
    application: { enabled: true }
    ...
```

## Healing semantics

When auto-heal inserts a correction:

```sql
INSERT INTO supplier_history (
    address, block_height, block_time, ...,
    snapshot_method, triggered_by_event, proto_version, indexed_at
) VALUES (
    'pokt1abc...', $target_height, (SELECT time FROM block WHERE height=$target_height),
    ...,
    'reconciler_correction',
    NULL,
    'v0_1_5',
    now()
)
ON CONFLICT (address, block_height) DO UPDATE SET
    -- Overwrite the original snapshot (rare: only happens if the original existed and was wrong)
    stake_upokt = EXCLUDED.stake_upokt,
    services = EXCLUDED.services,
    -- ... all fields
    snapshot_method = 'reconciler_correction';
```

The `ON CONFLICT DO UPDATE` covers the case where a buggy snapshot already exists at that height. The corrected row will appear in audit traces with `snapshot_method = 'reconciler_correction'`, so operators can investigate.

**This is one of the few documented exceptions to "no UPDATE on history tables"** — see ADR-005. The exception exists because the reconciler is the chain's voice; if it disagrees with what we have, we trust the chain.

## When NOT to auto-heal

The reconciler **alerts but doesn't heal** when:
- Mismatch count exceeds `auto_heal_threshold` (default 10). This suggests a systemic bug, not isolated drift.
- The module's `auto_heal` config is `false` (operator preference for cautious modules).
- The chain isn't reachable (`BulkFetchChain` fails) — skip this cycle.

When alerts fire:
- On-call investigates: was there an upgrade? A NATS outage? A consumer bug?
- Fix root cause.
- Manually trigger heal: `ps reconcile --module=supplier --at-height=<H> --force-heal`.

## Detection-only mode (for paranoid environments)

Set `auto_heal: false` globally. Reconciler reports drift via metrics + alerts but never writes. Operators handle each detected drift case via the manual `ps reconcile --force-heal` command.

Useful for early phases when you want to **observe** drift before trusting auto-heal.

## Manual reconciliation

```bash
# Check drift for one module at the current tip
ps reconcile --module=supplier

# Check at a specific height
ps reconcile --module=supplier --at-height=487231

# Force heal regardless of threshold
ps reconcile --module=supplier --force-heal --at-height=487231

# Reconcile all modules (dry run)
ps reconcile --dry-run
```

## Metrics

```
pocketscribe_reconciler_runs_total{module}
pocketscribe_reconciler_drift_detected_total{module}
pocketscribe_reconciler_auto_heal_total{module}
pocketscribe_reconciler_alert_total{module}
pocketscribe_reconciler_last_run_timestamp_seconds{module}
pocketscribe_reconciler_duration_seconds{module}
pocketscribe_reconciler_chain_fetch_errors_total{module}
```

## Edge cases

- **Chain returns paginated results**: implement pagination in `BulkFetchChain`. Most modules have <10k entities; one page is usually enough.
- **Snapshot in-flight while reconciler reads**: use safety margin (head - N blocks) so the consumer has time to catch up.
- **Time-skew during chain RPC**: query at a specific height (`Block-Height` gRPC metadata header), not "current". Deterministic.
- **A new entity that exists on-chain but not in indexer**: insert a snapshot with `snapshot_method = 'reconciler_correction'`. Same as healing a stale snapshot.
- **An entity that exists in indexer but not on-chain**: rare. Could be a phantom from a buggy decoder. Alert; manual review.

## See also

- ADR-006 (chain as source of truth) — the foundation.
- ADR-009 (bucket sealing) — sealing uses `consumer_consolidation` which interacts with reconciler corrections.
- `docs/operations/runbook.md` — on-call procedure when reconciler alerts fire.
