# 06 — Downstream APIs (out of PocketScribe scope, supported by schema)

> PocketScribe is a **producer** of indexed data. APIs that expose this data to humans and applications are **separate components** — they consume PocketScribe's Postgres + NATS firehose. This doc describes what we support architecturally, even though we don't ship those components.

## The architectural commitment

PocketScribe guarantees that its Postgres schema and NATS subjects are stable, well-documented, and queryable by **any tool that speaks SQL or NATS**. We do not:
- Build a custom REST/GraphQL server.
- Bundle Hasura/PostgREST in our deployment.
- Provide client SDKs (other than the eventual `pkg/scribeclient` Go SDK).

This separation lets the API layer evolve independently — different teams, different rates, different stacks.

## The three downstream layers

### 1. GraphQL via Hasura

[Hasura](https://hasura.io/) is a Haskell-based GraphQL engine that auto-generates a complete GraphQL API from a Postgres schema. PocketScribe-friendly because:

- Reads `COMMENT ON TABLE` / `COMMENT ON COLUMN` and surfaces them as GraphQL descriptions.
- Supports **read replicas** (queries to replicas, mutations + subscriptions to primary).
- **Streaming subscriptions** (cursor-based polling, works against replicas) — usable for ~1-2s real-time without LISTEN/NOTIFY.
- Auto-permissions per role (RLS-aware).

What we ship to make Hasura easy:
- Stable `*_history` tables and `*` views.
- `COMMENT ON` everywhere (auto-docs).
- A canonical metadata snapshot at `configs/downstream/hasura-metadata.json` (TODO).

What we do **not** rely on:
- Hasura LISTEN/NOTIFY subscriptions (tied to primary; defeats replica scaling).
- Hasura Actions (custom resolvers; pushes logic into Hasura, fragile).

### 2. REST + OpenAPI via PostgREST

[PostgREST](https://postgrest.org/) is a Haskell-based REST API auto-generator over Postgres. Complementary to Hasura:

- REST contracts often easier than GraphQL for simple integrations.
- **Auto-generates OpenAPI 3.0** from the schema (`GET /` returns the spec).
- Reads `COMMENT ON` for descriptions in OpenAPI.
- Embeddable relations via query params (`?select=*,supplier(*)`).

We can run **both Hasura and PostgREST against the same database**. No conflict.

### 3. Real-time push via NATS WebSocket bridge

For sub-second push (where Hasura streaming subscriptions' 1-2s polling latency is too slow), expose NATS subjects via a **WebSocket bridge** — a small Go service that:

- Subscribes to filtered NATS subjects.
- Forwards JSON-serialized events to WebSocket clients.
- Handles client subscriptions (per-subject, per-filter).
- Authenticates clients (JWT, API key, depends on deployment).

**Why this over Hasura subscriptions**: latency. NATS publish → bridge → WebSocket round-trip is <100ms. Hasura streaming polls Postgres every N seconds.

**Why this over Postgres LISTEN/NOTIFY**: doesn't scale; ties subscriptions to the write primary.

**Implementation**: a separate small repo (not PocketScribe). The contract is the NATS subject schema, documented below.

## NATS subject contract

PocketScribe publishes to these subjects. Any downstream may subscribe:

```
pokt.block.{height}                    # full block payload (FinalizeBlock req/res + KV changes)
pokt.kv.{store}.{height}               # per-store KV writes
pokt.events.{event_type}.{height}      # per-event-type fan-out
```

**Stability**: subjects are **versioned implicitly** through the payload's `proto_version` field. We don't change subject names; payload schema evolves (always backwards-compatible adds).

**Retention**: 30 days default. Downstream that needs longer must keep its own storage.

**Dedup**: messages carry `Nats-Msg-Id`. Two redundant publishers (HA active-active) get dedup'd within a 24h window.

**JSON envelope** (preferred for WebSocket bridge clients):

```json
{
  "subject": "pokt.events.EventClaimSettled.487231",
  "block_height": 487231,
  "block_time": "2026-05-22T14:00:00Z",
  "proto_version": "v0_1_5",
  "payload": { /* event-specific fields */ }
}
```

## Docs unified

When all three layers run, expose documentation under one site:

```
docs.pocket-indexer.io/
  ├── REST API           → Redoc / Stoplight Elements over PostgREST's OpenAPI
  ├── GraphQL API        → GraphiQL or GraphQL Voyager
  └── Realtime           → Markdown describing NATS subjects + sample WS code
```

All three drive descriptions from `COMMENT ON` in the database — **single source of truth**.

## Why we don't bundle these

- **Speed of iteration** — API layer changes (auth, rate limiting, custom views) shouldn't require PocketScribe releases.
- **Team boundaries** — frontend team may own Hasura config; SRE may own NATS bridge; indexer team owns this repo.
- **Choice for downstream** — some users may prefer to write their own thin SDK over the Postgres views; that's fine.

## Implementation notes for downstream teams

### Hasura metadata strategy

- Track an `aggregate_status` filter so only `status='public'` aggregates are exposed.
- Use `read_replicas` configuration to direct queries to read-only Postgres replicas.
- Avoid Hasura's "Actions" feature — keeps the API layer thin.
- Sync metadata via `hasura metadata apply` in CI.

### PostgREST configuration

- Use `db-anon-role = 'web_anon'` for unauthenticated reads.
- Authenticated roles via JWT (`db-jwt-secret`).
- Limit query depth / row count to prevent DoS (`db-max-rows`).
- Cache responses with `Cache-Control: public, max-age=N` on sealed-bucket aggregates (immutable until reseal).

### NATS WebSocket bridge

- Subscribe to subjects with consumer group per client session.
- Heartbeat every 30s to detect disconnected clients.
- Backpressure: if client can't keep up, drop messages (with lag metric) — don't queue indefinitely.

## What PocketScribe provides for downstream

- `aggregate_registry.status` for filtering exposed aggregates.
- `safe_height` view for cross-entity-consistency queries.
- `bucket_seal` table for "is this aggregate bucket trustworthy?" checks.
- `consumer_consolidation` for per-consumer lag dashboards.
- Stable NATS subjects with documented payload schemas.
- Stable Postgres views (`supplier`, `application`, etc.) that abstract `*_history`.

## What PocketScribe does NOT provide

- HTTP server, GraphQL/REST endpoints.
- Authentication / authorization (lives in downstream).
- Rate limiting (lives at API gateway / Hasura / PostgREST config).
- Cache invalidation (responsibility of downstream cache layer).
- Custom business logic (lives in downstream).

## See also

- ADR-011 (downstream APIs out of scope) — design rationale.
- `docs/operations/development-workflow.md` — how Hasura is wired in Tilt for dev verification.
