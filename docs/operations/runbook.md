# Runbook

> On-call procedures for PocketScribe production incidents. Sorted by alert.

## Alert: `PocketScribeConsumerLagHigh` / `PocketScribeConsumerLagCritical`

**What it means**: a consumer is falling behind the chain head.

**Investigate**:
1. Grafana → Consumer Health dashboard → which consumer + by how much.
2. Check pod logs: `kubectl logs deployment/ps-consumer-<module> --tail=200`.
3. Common causes:
   - Pod OOM → scale memory request.
   - Postgres slow → check primary CPU / IOPS / lock contention.
   - Decoder errors (latest poktroll upgrade missing) → check `pocketscribe_consumer_decoder_errors_total`.
4. NATS check: `nats consumer report POKT_CHAIN | grep <module>` — is `ack pending` high?

**Mitigate**:
- Scale up: `kubectl scale deployment/ps-consumer-<module> --replicas=N+1`.
- If decoder error: onboard the missing poktroll version (`/generate-decoder vX.Y.Z`).
- If Postgres bound: check `pg_stat_activity` for blocking queries; consider VACUUM if bloat.

**Resolve**:
- Confirm `pocketscribe_consumer_lag_blocks{consumer=X}` drops to <10.
- If you scaled up, schedule a scale-down once steady state returns.

---

## Alert: `PocketScribeFilepluginStuck`

**What it means**: the sidecar isn't publishing — `publish_lag_seconds > 120`.

**Investigate**:
1. Pod logs: `kubectl logs daemonset/ps-fileplugin --tail=200`.
2. Disk space on archive node host: `df -h /var/lib/poktroll/streaming`.
3. NATS reachable from sidecar pod? `kubectl exec ... -- nc -vz nats 4222`.
4. Are files accumulating? `ls /var/lib/poktroll/streaming | wc -l`.

**Mitigate**:
- If NATS unreachable: fix NATS (see "NATS down" below).
- If disk full: stop the node (it should halt automatically), free space, restart.
- If sidecar OOM: scale memory request and restart.

**Resolve**:
- `publish_lag_seconds` returns near 0.
- File count drains as sidecar catches up.

---

## Alert: `PocketScribeReconcilerDriftDetected`

**What it means**: indexed state for module X does not match chain at a specific height.

**Investigate**:
1. Grafana → Reconciler dashboard → which module, how many drifts, at what heights.
2. Pull reconciler logs: `kubectl logs deployment/ps-reconciler --since=1h | jq 'select(.msg | contains("drift"))'`.
3. Was there a recent poktroll upgrade?
   - If yes: probable cause is new fields in proto that the existing decoder doesn't populate → onboard new decoder version.
4. Was there a consumer crash recently? Check restart counters and processed_heights gaps.

**Mitigate**:
- If <10 drifts: auto-heal already inserted corrections; verify `pocketscribe_reconciler_auto_heal_total` incremented.
- If >10 drifts: systemic issue. Manually inspect a sample:
  ```bash
  psql -c "SELECT * FROM supplier_history WHERE address='pokt1...' ORDER BY block_height DESC LIMIT 5"
  poktrolld query supplier show-supplier pokt1...
  ```
- If decoder bug: write test reproducing it; fix; deploy; `ps replay --module=X --from=H1 --to=H2` to clean up.

**Resolve**:
- `pocketscribe_reconciler_drift_detected_total` rate returns to 0.
- Reconciler next cycle reports green.

---

## Alert: `PocketScribeDiskSpaceLow` / `PocketScribeDiskSpaceCritical`

**What it means**: disk on archive node streaming volume is filling.

**Investigate**:
1. `df -h /var/lib/poktroll/streaming`.
2. `ls /var/lib/poktroll/streaming | wc -l` — how many files pending sidecar deletion?
3. Sidecar healthy? Check publish_lag.

**Mitigate**:
- If sidecar healthy: it deletes after publish + safety window. Maybe shrink safety window temporarily.
- If sidecar unhealthy: fix sidecar first (above).
- If both: emergency mitigation — `ps fileplugin --delete-up-to=H` (force delete; sidecar must have already published).
- Permanent: add more disk to streaming volume.

**Resolve**:
- Disk usage drops below threshold.

---

## Alert: `PocketScribeReplicationLag`

**What it means**: Postgres replica is lagging > 60s behind primary.

**Investigate**:
1. Replica node CPU/IOPS — saturated?
2. Network between primary and replica.
3. WAL volume on primary — keeping up?
4. Long-running query on replica blocking apply?

**Mitigate**:
- Kill blocking query on replica: `SELECT pg_cancel_backend(pid)`.
- If replica is too slow: increase its IOPS allocation.
- Worst case: rebuild replica from base backup.

**Resolve**:
- Replication lag returns to <5s.

---

## Alert: `PocketScribeSealingStalled`

**What it means**: sealing loop iterations stopped incrementing.

**Investigate**:
1. Pod logs: `kubectl logs deployment/ps-sealing --tail=100`.
2. Postgres connections from sealing pod — alive?
3. Is `consumer_consolidation` advancing? If not, root cause is consumer lag.

**Mitigate**:
- Restart sealing pod: `kubectl rollout restart deployment/ps-sealing`.
- Address consumer lag if that's the root cause.

**Resolve**:
- `pocketscribe_sealing_loop_iterations_total` resumes.
- Latest `bucket_seal.sealed_at` is recent.

---

## Common incidents

### NATS down (full cluster)

Severity: critical.

1. Assess: how many NATS nodes are down? `kubectl get pods -l app=nats`.
2. If majority alive: cluster heals itself (Raft re-elects). Just wait or restart unhealthy pods.
3. If majority dead: PVs intact?
   - Yes: restart pods; cluster recovers state from PVs.
   - No (data loss): rebuild from PVs of surviving nodes, or restore from snapshot. **Possible message loss** within last retention window.
4. Once NATS back: sidecars resume publishing; consumers resume consuming. Dedup eats any retries.

### Postgres primary down

1. Confirm failure (network partition vs hard failure).
2. Promote a replica: `pg_promote()` or via Patroni.
3. Update PocketScribe `DATABASE_URL` to new primary.
4. Restart deployments to pick up new connection string (or use connection pool that reconnects).
5. Re-attach failed primary as a new replica once recovered.

### poktroll mainnet halts (network-wide)

Not a PocketScribe-specific incident, but we react.

1. Reconciler will report `chain_fetch_errors` — that's expected during halt.
2. Sidecar stops publishing (no new files from poktroll).
3. Consumers idle (no new messages).
4. **Do nothing**. Wait for governance / chain restart.
5. When chain resumes, everything resumes automatically.

### Bug in decoder produces wrong snapshots

1. Reconciler will eventually detect (if running) → alerts fire.
2. Write a test reproducing the bug.
3. Fix the decoder.
4. Deploy.
5. `ps replay --module=X --from=H1 --to=H2` for the affected range (find via reconciler alerts).
6. Confirm reconciler next cycle reports green.

### Schema migration needed for live system

1. Test the migration locally (`make db-reset && make db-migrate`).
2. Apply during low-traffic window: `kubectl exec -it deployment/ps-doctor -- ps migrate up`.
3. Monitor consumers — additive migrations don't require consumer restart, but new fields won't populate until consumer code knows about them.
4. Deploy new consumer code that populates the new fields.

### Total infrastructure loss

See `docs/operations/disaster-recovery.md` (TODO).

Brief: restore Postgres from pgBackRest PITR, restart NATS (data loss in retention window), bring back archive nodes (sync from peers), redeploy PocketScribe, run `ps reconcile --all` to validate.

---

## Useful commands

```bash
# Cursor status for all consumers
psql -c "SELECT * FROM consumer_consolidation ORDER BY consumer_name"

# Recent bucket seals
psql -c "SELECT * FROM bucket_seal ORDER BY sealed_at DESC LIMIT 20"

# Pending dirty buckets
psql -c "SELECT * FROM cagg_dirty_buckets ORDER BY dirty_since"

# NATS stream / consumer state
nats stream report POKT_CHAIN
nats consumer report POKT_CHAIN

# Force-trigger reconcile
ps reconcile --module=supplier --force

# Force-trigger sealing pass
ps sealing --once

# Spot-check entity vs chain
psql -c "SELECT * FROM supplier WHERE address='pokt1...'"
poktrolld query supplier show-supplier pokt1...
```

## When you're stuck

1. Check Grafana — usually points at the right component.
2. Check pod logs — most issues self-describe.
3. Compare DB state vs chain state — settles "did the indexer process this?" questions.
4. If still stuck: `ps doctor` for a holistic health report.
5. Escalate to whoever maintains the chain (poktroll team) if root cause appears chain-side.
