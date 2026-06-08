# ADR-002: PostgreSQL + TimescaleDB OSS over ClickHouse

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

The indexer needs:
- Strong **relational queries** for entity history (suppliers, applications, sessions, etc.).
- Strong **time-series performance** for events (claims, proofs, mint/burn ops).
- **Self-hosted** with healthy OSS replication (no cloud lock-in).
- **Operational simplicity** (limit cognitive load on a small team).

Past experience by the team:
- **ClickHouse** — OSS replication is operationally heavy; the best replication features are cloud-only (ClickHouse Cloud).
- **Postgres** — proven streaming replication, well-understood.

## Decision

Use **PostgreSQL latest** (17 at project start) with the **TimescaleDB OSS extension** (2.18+). Single DB cluster. Hypertables for high-volume time-series tables; regular tables for relational entities.

## Consequences

### Positive

- **One DB, one ops story.** Same Postgres, same backups (pgBackRest), same monitoring (pg_exporter), same connection pool (pgxpool), same access control.
- **Battle-tested replication.** Postgres streaming replication is one of the most-deployed replication strategies in the world.
- **TimescaleDB OSS** includes hypertables, columnar compression (10-15x typical), continuous aggregates, retention policies. The cloud-only features (multi-node) we don't need.
- **No new query language.** SQL throughout. Hasura + PostgREST integrate natively.
- **Joins work.** A `JOIN` between `supplier_history` (regular table) and `event_claim_settled` (hypertable) is trivial. In ClickHouse, this hurts.
- **Compression close to ClickHouse** for time-series workloads. Benchmarks showed 10-15x for typical chain data (timestamps, repeated addresses, numeric fields).

### Negative

- **Slower aggregation than ClickHouse** on billions of rows. For Pocket's volume (~5M events/year initially), the gap is negligible. If we hit billions, we revisit.
- **Postgres write throughput** has a ceiling per primary (~50k writes/sec realistically). Mitigated by per-module consumers writing in parallel + batch inserts.
- **TimescaleDB OSS doesn't have multi-node** (distributed hypertables) — that's cloud-only. Mitigated by vertical scaling + read replicas. If we exceed single-node, we look at Citus (also OSS).

### Neutral

- TimescaleDB is a Postgres extension, not a fork — upgrades follow Postgres upgrade cycles.

## Alternatives considered

### Option A: ClickHouse (rejected explicitly)
- Pro: Best-in-class analytics; columnar; fast aggregations.
- Con: OSS replication is operationally painful (team's direct experience).
- Con: Best version (ClickHouse Cloud) is proprietary and cloud-only.
- Con: Joins with non-columnar tables are awkward.
- **Rejected because**: violates "self-hosted, no cloud lock-in" hard constraint.

### Option B: StarRocks / Apache Doris
- Pro: MPP columnar, OSS replication, similar perf to ClickHouse.
- Con: Yet another DB to operate, no native Hasura support, MySQL protocol (not Postgres).
- **Rejected because**: operational overhead not justified at our scale; if we ever exceed Postgres+Timescale, revisit.

### Option C: Pure Postgres (no Timescale)
- Pro: Even simpler operationally.
- Con: No hypertable performance for time-series; no native columnar compression; manual partitioning for hot tables.
- **Rejected because**: events table will hit billions of rows; native partitioning + compression is worth the extension.

### Option D: Two databases (Postgres + analytical store)
- Pro: Right tool for each job.
- Con: Two replication stories, two backup strategies, data sync layer, harder joins.
- **Rejected because**: complexity outweighs benefit at our scale.

## Implementation notes

- `CREATE EXTENSION IF NOT EXISTS timescaledb;` in migration 0001.
- Hypertables: `event_claim_settled`, `event_proof_updated`, `mint_burn_op`, etc.
- Compression policies enabled AFTER initial backfill stabilizes (not in initial migration).
- Streaming replication for read replicas; Hasura's `read_replicas` config to route queries.
- pgBackRest for backup + PITR.

## References

- ADR-006 (chain as source of truth) builds on this storage choice.
- ADR-009 (bucket sealing) uses TimescaleDB continuous aggregates.
