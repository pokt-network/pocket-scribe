# Architecture Decision Records (ADRs)

This directory holds the project's decision log. One ADR per significant decision. Use the [template](./000-adr-template.md) when adding new ones.

## Why ADRs

A decision that isn't written down is a decision that will be re-debated. ADRs preserve the **why** behind today's architecture so future contributors (and Claude) don't relitigate settled questions.

When in doubt: write an ADR. Cheap insurance against repeated discussions.

## Numbering

Sequential, zero-padded 3-digit: `ADR-001`, `ADR-042`. Don't reuse numbers even if an ADR is superseded.

## Status lifecycle

- **Proposed** — written but not yet approved.
- **Accepted** — approved and in effect.
- **Superseded by ADR-NNN** — replaced by a newer decision; kept for history.
- **Deprecated** — no longer applies; not superseded by another ADR.

## Active ADRs (committed)

### Foundational architecture

- [ADR-001: Go over Rust](./ADR-001-go-over-rust.md)
- [ADR-002: PostgreSQL + TimescaleDB OSS over ClickHouse](./ADR-002-postgres-timescale-over-clickhouse.md)
- [ADR-003: Official FilePlugin + Go sidecar over custom in-process plugin](./ADR-003-fileplugin-and-sidecar.md)
- [ADR-004: NATS JetStream over Kafka](./ADR-004-nats-jetstream-over-kafka.md)
- [ADR-005: Append-only pure (no valid_to_height)](./ADR-005-append-only-pure.md)
- [ADR-006: Chain as source of truth (snapshots, not derived state)](./ADR-006-chain-as-source-of-truth.md)
- [ADR-007: Per-module consumers with queue groups](./ADR-007-per-module-consumers.md)
- [ADR-008: Versioned proto decoders with height-based router](./ADR-008-versioned-decoders.md)
- [ADR-009: Bucket sealing with consumer_consolidation](./ADR-009-bucket-sealing.md)
- [ADR-010: (block_height, block_time) invariant](./ADR-010-height-and-time-invariant.md)
- [ADR-011: Downstream APIs (Hasura, PostgREST, NATS WS) not in scope](./ADR-011-downstream-apis-out-of-scope.md)
- [ADR-012: 5-layer testing strategy](./ADR-012-testing-strategy.md)

### Process & tooling

- [ADR-013: Single `ps` CLI binary with cobra subcommands](./ADR-013-single-binary-cli.md)
- [ADR-014: Tilt + kind/k3d for local dev (prod parity)](./ADR-014-tilt-kind-for-dev.md)
- [ADR-015: TDD by default, DRY across the codebase](./ADR-015-tdd-and-dry.md)
- [ADR-016: pgx v5 + sqlc + goose over ORMs](./ADR-016-pgx-sqlc-goose.md)

### Operational scope & data lifecycle

- [ADR-017: Hasura + PostgREST + NATS WS bridge included in the spike](./ADR-017-hasura-postgrest-ws-bridge-in-spike.md)
- [ADR-018: No hardcoded upgrades — `upgrades` table is chain-driven](./ADR-018-no-hardcoded-upgrades.md)
- [ADR-019: Optional partial-history indexing (start_height)](./ADR-019-partial-history-from-height-x.md)
- [ADR-020: Deployment metadata + indexer state](./ADR-020-deployment-metadata-and-indexer-state.md)
- [ADR-021: Shannon mainnet history discontinuity — snapshot-bootstrap is mandatory](./ADR-021-shannon-history-discontinuity.md)

## Personal / draft ADRs

If you're drafting an ADR and don't want it in the public history yet, put it in `docs/decisions/personal/` (gitignored). When ready, move to this directory and renumber.

## Template

Copy `000-adr-template.md` and rename to `ADR-NNN-<kebab-case-title>.md`.
