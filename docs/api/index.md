# PocketScribe Data APIs — Dev Landing

> Once `tilt up` is running, this page describes the 3 data access methods exposed locally. In production, the same patterns apply (different URLs / auth).

---

## GraphQL (Hasura)

**URL**: http://localhost:8080/v1/graphql

**Console / GraphiQL**: http://localhost:8080/console (admin secret: `hasura-dev-secret`)

Auto-generated GraphQL schema from PostgreSQL + `COMMENT ON` descriptions.

### Sample queries

Current state of all suppliers:
```graphql
query Suppliers {
  supplier(limit: 10, order_by: { stake_upokt: desc }) {
    address
    stake_upokt
    block_height
    block_time
    services
  }
}
```

Supplier at a specific height:
```graphql
query SupplierAtHeight($address: String!, $height: bigint!) {
  supplier_history(
    where: {
      address: { _eq: $address }
      block_height: { _lte: $height }
    }
    order_by: { block_height: desc }
    limit: 1
  ) {
    address
    stake_upokt
    block_height
    block_time
  }
}
```

Rewards in the last 24h per supplier:
```graphql
query Rewards24h {
  rewards_hourly(
    where: { bucket_start: { _gt: "now() - interval '24 hours'" } }
    order_by: { total_settled: desc }
  ) {
    supplier_address
    bucket_start
    total_settled
  }
}
```

Streaming subscription (cursor-based, ~1-2s latency):
```graphql
subscription StreamClaims {
  event_claim_settled_stream(
    batch_size: 100
    cursor: { initial_value: { block_height: 0 }, ordering: ASC }
  ) {
    block_height
    block_time
    supplier_address
    settled_upokt
  }
}
```

---

## REST (PostgREST)

**URL**: http://localhost:3000/

**OpenAPI spec**: http://localhost:3000/ (JSON; render with Swagger UI / Redoc)

Auto-generated REST endpoints from PostgreSQL schema. Every table = endpoint at `/<table_name>`.

### Sample requests

Current state of all suppliers:
```bash
curl 'http://localhost:3000/supplier?limit=10&order=stake_upokt.desc'
```

Filter by stake:
```bash
curl 'http://localhost:3000/supplier?stake_upokt=gt.1000000000&order=block_height.desc'
```

Supplier at specific height:
```bash
curl 'http://localhost:3000/supplier_history?address=eq.pokt1abc...&block_height=lte.487231&order=block_height.desc&limit=1'
```

With embedded relations (if FK defined):
```bash
curl 'http://localhost:3000/event_claim_settled?select=*,supplier(address,stake_upokt)&limit=5'
```

Rewards in the last 24h (using PostgREST function call, optional):
```bash
curl 'http://localhost:3000/rewards_hourly?bucket_start=gt.now()-interval-24-hours&order=total_settled.desc'
```

---

## Real-time (NATS WebSocket bridge)

**URL**: `ws://localhost:9090/stream`

Subscribe to NATS subjects, get live JSON events.

### Sample client (JavaScript)

```javascript
const ws = new WebSocket('ws://localhost:9090/stream');

ws.onopen = () => {
  ws.send(JSON.stringify({
    action: 'subscribe',
    subject: 'pokt.events.EventClaimSettled.>',
  }));
};

ws.onmessage = (msg) => {
  const event = JSON.parse(msg.data);
  console.log(`Claim settled at height ${event.block_height}:`, event.payload);
};
```

### Sample client (curl + websocat)

```bash
echo '{"action":"subscribe","subject":"pokt.events.EventClaimSettled.>"}' \
  | websocat ws://localhost:9090/stream
```

### Subjects you can subscribe to

| Subject pattern | What you get |
|---|---|
| `pokt.block.>` | Full block payloads |
| `pokt.events.<EventType>.>` | Specific event type (e.g., `EventClaimSettled`) |
| `pokt.kv.<store>.>` | Per-store KV writes (high frequency; off-by-default) |

---

## Auto-generated docs

Both Hasura and PostgREST read `COMMENT ON` from PostgreSQL and surface them as field/parameter descriptions:

- **Hasura**: open GraphiQL → hover over any field → see the `COMMENT ON COLUMN` text.
- **PostgREST**: `GET /` returns OpenAPI 3.0; the `description` of each property is the column comment.

**To verify the schema→docs loop**:

```bash
# 1. Add a comment to a column
psql -c "COMMENT ON COLUMN supplier_history.address IS 'Bech32 supplier operator address (pokt1...)';"

# 2. Reload Hasura metadata
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: hasura-dev-secret" \
  -d '{"type":"reload_metadata","args":{}}'

# 3. Open GraphiQL → hover on `address` → see the comment as description.
# 4. Fetch the OpenAPI spec → search for "address" → see the comment in description field.
```

This is the central "docs from DB" pattern PocketScribe is built around. See [`docs/architecture/11-docs-from-db.md`](../architecture/11-docs-from-db.md) for details.

---

## Going to production

The same 3 access methods work in production with:
- Hasura with role-based permissions (admin / app / anon) + JWT auth.
- PostgREST with JWT-authenticated roles + row-level security in Postgres.
- WS bridge with JWT or API key auth + rate limiting.

See `docs/operations/deployment.md` for the production topology.
