# 11 — The "Docs from DB" Pattern (Hasura + PostgREST + WS bridge)

> The central value proposition of PocketScribe's data layer: **add a column to a table, and you automatically get GraphQL + REST + OpenAPI + real-time** with documented schemas. No API code to write.

This isn't an aspiration — it's the engineered pattern. This doc explains how it works and what discipline keeps it working.

## The pattern in 4 layers

```
┌────────────────────────────────────────────────────────────────────┐
│ Layer 1: SCHEMA + COMMENT ON                                       │
│   PostgreSQL is the single source of truth for schema + docs.      │
│                                                                    │
│   CREATE TABLE supplier_history (                                  │
│       address      TEXT NOT NULL,                                  │
│       block_height BIGINT NOT NULL,                                │
│       stake_upokt  NUMERIC NOT NULL,                               │
│       ...                                                          │
│   );                                                               │
│   COMMENT ON TABLE supplier_history IS                             │
│       'On-chain supplier history. Append-only. ...';               │
│   COMMENT ON COLUMN supplier_history.stake_upokt IS                │
│       'Current stake in uPOKT';                                    │
└──────────────────────────────────────┬─────────────────────────────┘
                                       │
        ┌──────────────────────────────┼────────────────────────────┐
        ▼                              ▼                            ▼
┌──────────────────┐         ┌──────────────────┐        ┌──────────────────┐
│ Layer 2a: Hasura │         │ Layer 2b: PostgREST│      │ Layer 2c: NATS   │
│                  │         │                  │        │ WS bridge        │
│ Reads schema     │         │ Reads schema     │        │                  │
│ Reads COMMENT ON │         │ Reads COMMENT ON │        │ Subscribes to    │
│ Generates GraphQL│         │ Generates REST + │        │ pokt.events.*    │
│ + GraphiQL docs  │         │ OpenAPI 3.0      │        │ Forwards to WS   │
│ + Streaming subs │         │ + Swagger UI     │        │ clients          │
└──────────────────┘         └──────────────────┘        └──────────────────┘
        │                              │                            │
        │                              │                            │
        ▼                              ▼                            ▼
   :8080/v1/graphql            :3000/                       ws://...:9090/stream
   :8080/console               :3000/(openapi.json)         Real-time events
   GraphQL + docs              REST + OpenAPI + Swagger     Sub-second push
```

## Layer 1: The schema discipline

Every table and every non-obvious column **must** have `COMMENT ON`. The reviewer agent flags missing comments. Without comments, the auto-generated docs are useless ("supplier_history.x" — what's x?).

Pattern:

```sql
CREATE TABLE supplier_history (
    address       TEXT NOT NULL,
    block_height  BIGINT NOT NULL,
    block_time    TIMESTAMPTZ NOT NULL,
    stake_upokt   NUMERIC(78, 0) NOT NULL,
    services      JSONB NOT NULL,
    ...
    PRIMARY KEY (address, block_height)
);

-- Always: table comment explaining purpose and invariants.
COMMENT ON TABLE supplier_history IS
$$On-chain supplier history. Each row is the full snapshot of a supplier at the
given block_height. Append-only — never UPDATE. Use the `supplier` view for
"current state" queries (which uses DISTINCT ON over this table).

Snapshot source is the ABCI StreamingService KV write for the supplier store;
the indexer never computes derived state.$$;

-- Always: column comments for everything non-obvious.
COMMENT ON COLUMN supplier_history.address IS
    'Bech32 supplier address (pokt1... prefix). Same as operator_address in most cases.';
COMMENT ON COLUMN supplier_history.block_height IS
    'Chain block height at which this snapshot was emitted. PK component.';
COMMENT ON COLUMN supplier_history.block_time IS
    'Chain consensus time (Tendermint header time). Used for time_bucket() and time-range queries.';
COMMENT ON COLUMN supplier_history.stake_upokt IS
    'Current stake in uPOKT (1 POKT = 1,000,000 uPOKT). NUMERIC(78,0) for full uint256 precision.';
COMMENT ON COLUMN supplier_history.services IS
    'Array of SupplierServiceConfig as JSONB. Each entry: { service_id, rev_share, endpoint, ... }.';
-- ... continue for every column
```

**Reviewer agent rule** (`.claude/agents/pocketscribe-reviewer.md` should enforce):
- Every `CREATE TABLE` in a new migration has a `COMMENT ON TABLE`.
- Every column not named `id`, `created_at`, `updated_at`, `indexed_at` has a `COMMENT ON COLUMN`.
- Comments explain **purpose and invariants**, not just retell the column name.

## Layer 2a: Hasura

Hasura reads the Postgres schema + comments and exposes:

- **GraphQL queries** at `:8080/v1/graphql`.
- **GraphiQL** at `:8080/console` (interactive query playground).
- **Streaming subscriptions** (cursor-based, polls every N seconds; works on read replicas).
- **Auto-generated documentation** in GraphiQL using `COMMENT ON` text.

Configuration is **declarative** in `configs/downstream/hasura-metadata.yaml`. Adding a new table to the API:

1. Schema migration adds the table + `COMMENT ON`.
2. Add an entry to `hasura-metadata.yaml` `tables` array with select permissions for `anon` role.
3. `hasura metadata apply` (or restart Hasura with the metadata mounted).

For spike: anon role with read-everything permissions. For production: role per consumer with row-level filters (Hasura supports this declaratively).

**Limitation we accept**: Hasura streaming subscriptions are ~1-2s latency (cursor-based polling). For sub-second real-time, use the NATS WS bridge (Layer 2c).

## Layer 2b: PostgREST

PostgREST reads the same Postgres schema + comments and exposes:

- **REST endpoints** at `:3000/<table>` and `:3000/<view>`.
- **Auto-generated OpenAPI 3.0 spec** at `:3000/` (JSON).
- **Swagger UI / Redoc** can render the spec for a docs site.
- **Query operators** in URLs: `?stake_upokt=gt.1000000&order=block_height.desc&limit=10`.
- **Embedded resources** (foreign keys): `?select=*,application(*)`.

Configuration in `configs/downstream/postgrest.conf`:

```
db-uri = "postgres://..."
db-schemas = "public"
db-anon-role = "pocketscribe_anon"
db-max-rows = 1000
openapi-mode = "follow-privileges"
```

Anon role setup (one-time in DB):
```sql
CREATE ROLE pocketscribe_anon NOLOGIN;
GRANT USAGE ON SCHEMA public TO pocketscribe_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pocketscribe_anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pocketscribe_anon;
```

PostgREST automatically grants `select` on tables based on the role. No metadata file needed for basic exposure.

## Layer 2c: NATS WebSocket bridge

For sub-second real-time push (where Hasura's polling is too slow):

```
NATS subjects ──► ws-bridge process ──► WebSocket clients
                  (small Go service,
                   ~150 LoC)
```

Client connects, sends a subscription:

```
ws://localhost:9090/stream
> {"action":"subscribe","subject":"pokt.events.EventClaimSettled.>"}
< {"subject":"pokt.events.EventClaimSettled.487231","block_height":487231,...}
< {"subject":"pokt.events.EventClaimSettled.487232","block_height":487232,...}
```

Configuration in `configs/downstream/ws-bridge.yaml`:
- Exposed subject patterns.
- Connection limits.
- Auth mode (spike: none; production: JWT).
- Envelope format.

In Phase 1: spike implementation as `ps ws-bridge` subcommand. In Phase 2+: may move to a separate small repo (per ADR-011 separation).

## The promise: schema change → API auto-updates

Workflow when adding a new column to `supplier_history`:

```sql
-- 1. Migration
ALTER TABLE supplier_history ADD COLUMN new_field NUMERIC NULL;
COMMENT ON COLUMN supplier_history.new_field IS
    'New field added in chain upgrade v0.1.6. Means X. NULL for heights < <H>.';

-- 2. Apply
ps migrate up
```

What happens automatically:

| Layer | Without writing code |
|---|---|
| Hasura | `hasura metadata reload` → GraphQL schema gains `new_field`. GraphiQL shows it with the COMMENT as description. |
| PostgREST | Next request to `/` reflects the new column in OpenAPI. Swagger UI shows it. Queries like `?new_field=gt.100` work immediately. |
| WS bridge | If the field affects an event subject, no change needed (events carry full payload). |

**Zero API code written. Documentation updates automatically from the schema.**

This is the value proposition that justifies the whole architecture.

## Discipline that keeps it working

1. **Every column gets `COMMENT ON`.** No exceptions for "obvious" fields — they're not obvious to API consumers.
2. **Views encapsulate canonical access.** `supplier` (not `supplier_history`) is what API consumers expect; the history table is for analytics.
3. **Foreign keys when they exist.** Hasura uses them to auto-derive relationships. If `event_claim_settled.supplier_address → supplier.address`, add an FK (or manual relationship in Hasura metadata).
4. **Status filtering for aggregates.** `aggregate_registry` rows have `status` (`shadow`/`public`/`deprecated`); a view filters to `public` only for downstream APIs.
5. **No ad-hoc SQL in clients.** All client access through Hasura/PostgREST/WS. Schema can evolve safely.

## Anti-patterns

- ❌ Building a custom Go HTTP server. We chose Hasura/PostgREST for this exact reason.
- ❌ Exposing `*_history` tables directly without explaining their semantics. Use views + COMMENT ON to guide consumers.
- ❌ Skipping `COMMENT ON` "because the column name is self-explanatory." Hasura/PostgREST auto-docs will look amateurish.
- ❌ Putting business logic in Hasura Actions / Remote Schemas. Keep API layer thin; logic lives in downstream apps.
- ❌ LISTEN/NOTIFY for real-time. Locks subscriptions to primary; defeats replicas. Use NATS WS bridge.

## In dev (Tilt)

`tilt up` brings Hasura + PostgREST + ws-bridge online alongside PocketScribe. URLs:

- GraphiQL (Hasura): http://localhost:8080
- Swagger UI (PostgREST OpenAPI): http://localhost:3000/ (raw); add Swagger UI from a CDN or `nginx` config for nice rendering
- WS bridge: `ws://localhost:9090/stream`
- Grafana (observability): http://localhost:3001

To verify the "schema → API" loop:
```bash
# Apply a migration that adds a column
psql -c "ALTER TABLE supplier_history ADD COLUMN test_col TEXT NULL; COMMENT ON COLUMN supplier_history.test_col IS 'Test column';"

# Reload Hasura metadata
curl -X POST http://localhost:8080/v1/metadata -H 'X-Hasura-Role: admin' \
     -d '{"type":"reload_metadata","args":{}}'

# Query GraphiQL → notice test_col is there with the COMMENT as description
# Query http://localhost:3000/ → OpenAPI spec now includes test_col
```

## See also

- ADR-017 (Hasura + PostgREST + WS bridge in Phase 1).
- ADR-011 (downstream APIs separation — still applies in production).
- `docs/architecture/06-apis.md` — high-level API layer description.
- `configs/downstream/hasura-metadata.yaml`, `postgrest.conf`, `ws-bridge.yaml` — actual configs.
