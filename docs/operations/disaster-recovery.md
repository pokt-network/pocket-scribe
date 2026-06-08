# Disaster Recovery

> Procedures for recovering from major data loss or cluster-wide failure.

## Risk classification

| Scenario | RPO target | RTO target | Procedure |
|---|---|---|---|
| Pod crash | 0 | <1 min | K8s restart |
| Single Postgres replica loss | 0 | <30 min | Rebuild from primary |
| Postgres primary loss | <1 min | <15 min | Failover to replica |
| Postgres full data corruption | <1 hr | <2 hr | pgBackRest PITR |
| NATS cluster loss (data PVs intact) | 0 | <30 min | Restart pods |
| NATS cluster total loss | up to 30d retention window | <4 hr | Rebuild + replay from archive |
| Archive node loss | 0 (HA pair) | <8 hr (single) | Spin replacement, sync |
| All archive nodes lost | 0 | <12 hr | Spin new, sync from peers |
| Full infrastructure loss | as above | <24 hr | Full rebuild from git + backups |
| poktroll mainnet halt | n/a | wait for governance | Do nothing |

## Procedure: Postgres primary failover

**Trigger**: primary down, alerts firing.

1. Confirm primary is truly down (`pg_isready -h primary`).
2. Identify which replica has the freshest state: `SELECT pg_last_wal_receive_lsn() FROM pg_stat_wal_receiver;` on each.
3. Promote the freshest replica:
   ```sql
   SELECT pg_promote();
   ```
4. Update DNS / connection string in PocketScribe config (or in K8s `Service`).
5. Restart PocketScribe consumers to pick up the new primary (`kubectl rollout restart deployment/ps-consumer-*`).
6. Re-attach the failed primary as a new replica once recovered.

## Procedure: Postgres PITR (point-in-time recovery)

**Trigger**: data corruption discovered.

Prerequisite: pgBackRest configured with full + incremental + WAL archival.

1. Decide the target recovery time / LSN (when did corruption start?).
2. Provision a new Postgres instance.
3. Restore latest base backup: `pgbackrest --stanza=pocketscribe restore --target-time="2026-05-22 14:00:00"`.
4. Apply WAL until target.
5. Verify integrity (run sampling queries).
6. Cut over: stop old primary; promote restored to new primary.
7. Re-snapshot any reconciler corrections lost between corruption point and restore time:
   ```bash
   ps reconcile --all --force-heal
   ```

## Procedure: NATS cluster loss

### Scenario A: PVs intact, pods lost

1. Restart pods: `kubectl rollout restart statefulset/nats`.
2. JetStream recovers from PVs. Cluster forms within minutes.
3. Sidecars resume publishing; consumers resume consuming.

### Scenario B: PVs lost, total data loss

1. Provision new NATS cluster.
2. Apply stream config: `make nats-streams`.
3. Sidecars start publishing fresh from where they left off (cursor on local disk).
4. **Data loss window**: any messages published but not consumed AND not yet on disk in archive node FilePlugin (rare; FilePlugin is the durable source).
5. Mitigation: if archive node still has FilePlugin output, sidecar can replay older files (within local retention).
6. Worst case: trigger backfill from archive (cold storage), or trigger `ps reconcile --all --force-heal` to catch state-level drift.

## Procedure: Archive node loss (single node, HA pair)

1. Confirm the remaining archive is healthy and producing.
2. Provision the new node.
3. Initialize, sync from peers (or from peer snapshot for faster bootstrap).
4. Configure FilePlugin (`app.toml`) identically to the surviving node.
5. Deploy `ps fileplugin` sidecar pointed at the new node.
6. Both nodes publish; NATS dedup eats duplicates.

## Procedure: Both archive nodes lost

1. Provision new archive nodes (2x for HA).
2. Sync from mainnet peers (NOT from each other initially).
3. Configure FilePlugin.
4. While syncing, PocketScribe consumers are idle (no new messages).
5. **Sidecars produce all historical blocks** as the nodes replay (per `09-backfill.md`).
6. Consumers process them, dedup against existing rows (idempotent upserts make this safe).
7. Once nodes catch up, normal live ingestion resumes.

Expected wallclock: ~6-12 hours for a 500k-block chain.

## Procedure: PocketScribe deploy lost

The easiest recovery — PocketScribe is **stateless**:

1. Redeploy from container image.
2. Consumers resume from `processed_heights` (in Postgres).
3. Sidecar resumes from cursor (on archive node disk).
4. Sealing + reconciler restart from their respective state in Postgres.
5. **Zero data loss.**

## Procedure: Full infrastructure loss

The catastrophic scenario. Sequence:

1. **Provision new K8s cluster.**
2. **Restore Postgres from pgBackRest PITR** (use latest viable target time).
3. **Bring up NATS** (no data; clean state).
4. **Spin new archive nodes**, sync from mainnet peers (slow).
5. **Apply PocketScribe migrations** (already applied; just ensure schema matches).
6. **Deploy `ps fileplugin`, `ps consumer-*`, `ps sealing`, `ps reconciler`**.
7. **Consumers catch up** to the chain head (using restored Postgres state + new sidecar firehose).
8. **Reconciler runs** — validates indexed state vs chain. Auto-heal corrections.
9. **Deploy downstream APIs** (Hasura, PostgREST, WS bridge).
10. **Validate**: spot-check entity counts and recent aggregate values.

Wallclock: ~24 hours for a fully automated playbook. Add human time for diagnosis.

## Cold archive strategy (for fast recovery)

Per `09-backfill.md`, keep FilePlugin output files in cold storage (MinIO/S3, compressed). For total data loss scenarios:

1. Don't re-sync from peers (slow). Restore the cold archive.
2. Point sidecar at the restored directory.
3. Consumers process them at IO-bound speed (~10-100x faster than chain replay).

Cost of cold archive: ~50 GB compressed per 500k blocks. Worth it.

## Backup verification (quarterly)

- Restore pgBackRest to a scratch instance; query for known historical data; compare.
- Restore NATS PV snapshot to scratch; verify stream config.
- Restore cold archive sample; replay a small range with PocketScribe pointing at a scratch DB.

**Untested backups are not backups.**

## Documentation drift

After every DR event, **update this doc with what you learned**. Examples:
- "Realized step 3 of NATS recovery needs `--reset-cluster` flag."
- "Reconciler bulk-list times out for large modules; increased gRPC timeout."

Living document.
