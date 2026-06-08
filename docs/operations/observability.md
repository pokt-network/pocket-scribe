# Observability

> With N pods, frequent hot-reloads, NATS lag, sealing loops, and reconciler corrections happening in parallel, you cannot debug without aggregated logs and metrics. Observability is **not optional**.

## Stack

```
Logs:    All pods → Promtail → Loki → Grafana (Loki datasource)
Metrics: All pods → Prometheus (scrape) → Grafana (Prometheus datasource)
Traces:  All pods → OTel collector → Tempo (future; not in MVP)
Alerts:  Alertmanager (Prometheus) → Slack/PagerDuty (production)
```

All four components run in the local dev cluster via Tilt. Same architecture in production (different scale).

## Components

| Component | Role | Image | Port |
|---|---|---|---|
| **Prometheus** | Metrics scraping + alerting | `prom/prometheus` | 9090 |
| **Loki** | Log aggregation | `grafana/loki` | 3100 |
| **Promtail** | Log shipper (one per node) | `grafana/promtail` | DaemonSet |
| **Grafana** | Unified dashboard + queries | `grafana/grafana` | 3001 (dev) |
| **Alertmanager** | Alert routing | `prom/alertmanager` | 9093 |

## Local dev access

```bash
# After `tilt up`:
# Grafana:        http://localhost:3001       (admin / admin)
# Prometheus:     http://localhost:9090
# Loki query API: http://localhost:3100/loki/api/v1/query_range
```

## Metric conventions

All PocketScribe metrics are prefixed `pocketscribe_`:

```
pocketscribe_consumer_processed_blocks_total{consumer="supplier"}
pocketscribe_consumer_lag_blocks{consumer="supplier"}         # head - consolidated_up_to
pocketscribe_consumer_processing_duration_seconds{consumer="supplier"}
pocketscribe_consumer_db_commit_duration_seconds{consumer="supplier"}
pocketscribe_consumer_decoder_errors_total{consumer,version}

pocketscribe_fileplugin_files_processed_total
pocketscribe_fileplugin_files_pending
pocketscribe_fileplugin_publish_lag_seconds                   # head - last_published_height
pocketscribe_fileplugin_publish_errors_total
pocketscribe_fileplugin_files_deleted_total

pocketscribe_nats_publish_duration_seconds
pocketscribe_nats_dedup_hits_total
pocketscribe_nats_consumer_ack_pending{consumer}

pocketscribe_sealing_buckets_sealed_total{aggregate}
pocketscribe_sealing_loop_duration_seconds
pocketscribe_sealing_loop_iterations_total
pocketscribe_cagg_dirty_buckets_pending{aggregate}

pocketscribe_reconciler_runs_total
pocketscribe_reconciler_drift_detected_total{module}
pocketscribe_reconciler_auto_heal_total{module}
pocketscribe_reconciler_last_run_timestamp_seconds

pocketscribe_db_pool_connections_open
pocketscribe_db_pool_connections_idle
pocketscribe_db_query_duration_seconds{operation}
```

Per the DRY invariant, all metric definitions live in **one place**: `internal/metrics/metrics.go`.

## Log conventions

All logs are JSON (via `log/slog`) with these structured fields:

```json
{
  "time": "2026-05-22T14:00:00Z",
  "level": "info",
  "msg": "block processed",
  "service": "ps-consumer-supplier",
  "module": "supplier",
  "block_height": 487231,
  "block_time": "2026-05-22T13:59:55Z",
  "trace_id": "abc123...",        // when present
  "duration_ms": 47
}
```

**Required fields**:
- `service` (set via env var `LOG_SERVICE`; usually `ps-<subcommand>-<module>`)
- `block_height` when applicable (lets Loki link to Prometheus)

**Forbidden**:
- Plain text logs in production.
- Logging entire entity snapshots (privacy + size); log address + height.
- Logging at `info` for things that happen every block (spam). Use `debug` and bump in dev.

## Loki queries (useful in Grafana Explore)

```logql
# All errors across all PocketScribe services in the last hour
{namespace="pocketscribe-dev", service=~"ps-.*"} | json | level="error"

# Decoder errors for a specific version
{service=~"ps-consumer-.*"} | json | msg="decoder error" | version="v0_1_5"

# All activity for a specific block height
{namespace="pocketscribe-dev"} | json | block_height="487231"

# Sidecar publish failures
{service="ps-fileplugin"} | json | level="error" | msg=~".*publish.*"
```

## Prometheus queries

```promql
# Block lag per consumer (warning if > 100, critical if > 1000)
pocketscribe_consumer_lag_blocks

# Reconciler drift detection rate per module
rate(pocketscribe_reconciler_drift_detected_total[1h])

# NATS dedup hit rate (high during HA testing; should be ~0 in normal ops)
rate(pocketscribe_nats_dedup_hits_total[5m])

# 95th percentile consumer processing duration
histogram_quantile(0.95, rate(pocketscribe_consumer_processing_duration_seconds_bucket[5m]))

# Fileplugin lag (head - last_published_height)
pocketscribe_fileplugin_publish_lag_seconds
```

## Dashboards (shipped)

Located in `configs/observability/dashboards/`:

| Dashboard | Purpose |
|---|---|
| `ingestion-overview` | Sidecar lag, NATS publish rate, files pending, errors |
| `consumer-health` | Per-consumer cursor lag, processing duration, decoder errors, restart count |
| `aggregate-sealing` | Buckets sealed (rate), dirty queue depth, refresh duration, per-aggregate status |
| `reconciler` | Drift detected, auto-heal count, run frequency, per-module breakdown |
| `postgres-timescale` | Write throughput, compression ratio, hypertable chunks, slow queries |
| `nats-jetstream` | Stream messages, consumer ack pending, dedup hits, retention usage |
| `logs-firehose` | Loki panel with all ps-* logs, filterable by service/level/block_height |

## Alerting (production)

Alertmanager routes to Slack / PagerDuty based on label matching:

```yaml
# Alert routing (excerpt)
- severity: critical
  receiver: pagerduty-oncall
  matchers:
    - severity = critical
- severity: warning
  receiver: slack-pocketscribe
  matchers:
    - severity = warning
```

Key alerts (defined in `configs/observability/prometheus-rules.yaml`):

| Alert | Severity | Condition |
|---|---|---|
| `PocketScribeConsumerLagHigh` | warning | `consumer_lag_blocks > 100` for 5min |
| `PocketScribeConsumerLagCritical` | critical | `consumer_lag_blocks > 1000` for 5min |
| `PocketScribeFilepluginStuck` | critical | `publish_lag_seconds > 120` for 2min |
| `PocketScribeReconcilerDriftDetected` | warning | `rate(reconciler_drift_detected_total[1h]) > 0` |
| `PocketScribeDiskSpaceLow` | warning | `node_filesystem_avail_bytes < 20%` on streaming volume |
| `PocketScribeDiskSpaceCritical` | critical | `< 5%` (node halts via stop-node-on-err) |
| `PocketScribeReplicationLag` | warning | `pg_replication_lag_seconds > 60` |
| `PocketScribeSealingStalled` | warning | `sealing_loop_iterations_total` not increasing for 10min |

## Debug workflow

When something feels wrong:

1. **Grafana → Logs firehose dashboard**. Filter by service / level / block_height.
2. **If a specific block is suspect**: filter Loki by `block_height="N"` to see every service's log entry for that block.
3. **If a consumer is slow**: Grafana → Consumer health → drill into the specific consumer. Check `processing_duration` p95, `db_commit_duration` p95.
4. **If reconciler alerts drift**: query Postgres directly to see the mismatch (`SELECT FROM <module>_history WHERE block_height = N`); compare with `poktrolld query <module> ...`.
5. **If NATS lag**: `nats consumer report POKT_CHAIN` from a pod or local nats CLI.

## Production sizing (rough)

| Component | Replicas | CPU | Memory | Storage |
|---|---|---|---|---|
| Prometheus | 2 (HA pair) | 2 | 8Gi | 500Gi (30d retention) |
| Loki | 3 | 1 | 4Gi | 500Gi (30d retention) |
| Promtail | DaemonSet | 0.1 | 256Mi | — |
| Grafana | 2 | 0.5 | 1Gi | — |
| Alertmanager | 3 (gossip cluster) | 0.1 | 256Mi | 10Gi |

Bigger production = pre-aggregated dashboards via Recording Rules; cold storage to S3-compatible (MinIO).

## Future additions

- **Tempo** for distributed tracing (NATS publish → consumer process → DB write spans).
- **Pyroscope** for continuous profiling (CPU/memory hotspots in consumers).
- **Mimir** for long-term metrics retention (>30d).

These are NOT in MVP. Add when the existing stack hits its limits.
