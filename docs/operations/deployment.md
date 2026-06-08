# Production Deployment

> Initial sketch. Full Helm chart + manifests TODO during Phase 3.

## Topology

See [`docs/architecture/07-ha-scaling.md`](../architecture/07-ha-scaling.md) for the full HA production topology. This doc focuses on the **how** of deployment.

## Target environment

- **Kubernetes 1.30+** (managed or self-hosted; we use bare-metal K8s in prod for cost).
- **Storage class** with NVMe-backed PVs for Postgres + NATS streaming volumes.
- **Network**: pods can reach poktroll archive nodes (peer with mainnet).
- **DNS**: internal service discovery via K8s; external DNS for downstream API endpoints.

## Stack components

| Component | Helm chart / source |
|---|---|
| PostgreSQL + TimescaleDB | `bitnami/postgresql` with Timescale image override |
| NATS JetStream | `nats/nats` (official chart) |
| Prometheus | `prometheus-community/kube-prometheus-stack` |
| Grafana | `grafana/grafana` |
| Loki + Promtail | `grafana/loki-stack` |
| PocketScribe (`ps`) | our Helm chart (TODO `deploy/helm/pocketscribe/`) |
| poktroll archive nodes | community-maintained or our own chart (TODO) |
| Hasura | `hasura/graphql-engine` (downstream) |
| PostgREST | custom Deployment (downstream) |
| NATS WS bridge | custom Deployment (downstream) |
| pgBackRest | standalone or sidecar to Postgres |

## Deploy order

1. Provision the Kubernetes cluster.
2. Install observability stack (Prometheus, Grafana, Loki).
3. Provision Postgres primary + replicas.
4. Provision NATS JetStream cluster.
5. Apply PocketScribe migrations (`ps migrate up`).
6. Deploy poktroll archive nodes with FilePlugin enabled.
7. Deploy `ps fileplugin` (sidecar) per archive node.
8. Deploy `ps consumer` per module (replica counts per ADR-007).
9. Deploy `ps sealing`, `ps reconciler` (singleton each).
10. Deploy downstream APIs (Hasura, PostgREST, WS bridge).
11. Configure ingress / TLS / auth at the downstream layer.

## Container image

Production image (TODO `deploy/docker/Dockerfile`):
```
# Multi-stage:
# Stage 1: golang:1.24 — build the binary with -ldflags
# Stage 2: gcr.io/distroless/static-debian12 — runtime
COPY --from=build /app/bin/ps /app/ps
ENTRYPOINT ["/app/ps"]
```

Built and pushed by goreleaser on tag push.

Alternative: **ko** for pure-Go image builds without a Dockerfile (simpler).

## ConfigMaps + Secrets

- `pocketscribe-config` ConfigMap: log level, metrics port, reconciler interval, sealing interval.
- `pocketscribe-db` Secret: Postgres connection string.
- `pocketscribe-nats` Secret (if NATS uses auth): NATS credentials.
- `pocketscribe-poktroll` ConfigMap: gRPC endpoints for archive nodes (used by reconciler).

## Health checks

Every `ps` subcommand exposes:
- `/healthz` — process is alive.
- `/readyz` — process is ready (DB connected, NATS connected, can do work).
- `/metrics` — Prometheus metrics.

K8s liveness/readiness probes configured to hit these.

## Logging

- All logs JSON via `log/slog`.
- Promtail DaemonSet ships to Loki.
- Structured fields enforced (see `docs/operations/observability.md`).

## Backups

- **Postgres**: pgBackRest with PITR. Full backup nightly; incremental every hour; WAL archival continuous.
- **NATS**: file storage on PV; PV snapshot weekly. JetStream stream config in git.
- **FilePlugin output (archive node)**: optional cold archive to MinIO / S3 (see `docs/architecture/09-backfill.md`).
- **PocketScribe migrations + code**: git. (No state in PocketScribe binary itself.)

## Scaling levers

| Pressure | Lever |
|---|---|
| Consumer lag high | Scale up consumer replicas (`kubectl scale ...`) |
| Sealing loop slow | Reduce `consumers_needed` per aggregate (rethink which consumers must be consolidated) |
| Postgres write IOPS bottleneck | Vertical scale primary; consider Citus sharding for billions of rows |
| Postgres read query slow | Add read replica; route Hasura/PostgREST queries to replica |
| NATS disk pressure | Reduce retention window; or scale up NATS node storage |
| Archive node disk pressure | Tune FilePlugin retention; add cold archive |

## Upgrades

### PocketScribe upgrades

1. Release a new image tag.
2. Apply new migrations: `kubectl exec ... -- ps migrate up`.
3. Rolling deploy: K8s `Deployment` strategy `RollingUpdate` with `maxUnavailable: 1`.
4. Monitor lag/error metrics; rollback if needed.

### poktroll upgrades

When poktroll mainnet upgrades to a new version:
1. Wait for `applied_at_height` to be confirmed on-chain.
2. Onboard the new decoder version: `/generate-decoder vX.Y.Z` (see ADR-008).
3. Update `internal/router/upgrades.go` and apply migration recording the upgrade.
4. Deploy new PocketScribe image with the new decoder.
5. Archive node operators upgrade their poktroll binary independently (orchestrated separately).

### Postgres upgrades

Major version upgrades (e.g., 17 → 18):
- Provision new Postgres cluster on the new version.
- Replicate via logical replication or pg_dump/pg_restore.
- Cut over (during a maintenance window).
- Apply Timescale extension upgrade.

## Disaster recovery

See `docs/operations/disaster-recovery.md` (TODO). Brief:

- Lost Postgres primary: failover to replica; promote; re-attach the failed primary as a new replica.
- Lost NATS cluster: restore from disk PV; replay from archive node files if retention window not enough.
- Lost archive node: spin a new one; sync from peers; FilePlugin starts emitting.
- Lost PocketScribe deploy: redeploy from image; consumers resume from `processed_heights`; no data loss.

## Cost (recap from ADR-007 architecture doc)

Self-hosted on commodity infra: ~$1,100/month total. Cloud-managed equivalent: $5-15k+/month.

## TODO

- Full Helm chart at `deploy/helm/pocketscribe/`.
- Production manifest examples at `deploy/k8s/prod/`.
- Detailed runbook at `docs/operations/runbook.md`.
- Detailed disaster recovery at `docs/operations/disaster-recovery.md`.
- Backfill operational guide at `docs/operations/backfill-procedure.md`.
