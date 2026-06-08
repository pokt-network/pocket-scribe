# PocketScribe Roadmap

Phased plan from zero to production. Each phase has an exit criterion and an explicit decision gate.

## Phase 0 — Design corpus (DONE)

Output: this repository, fully documented design + skeleton.

Exit: human reviews and approves direction.

## Phase 1 — Spike (target: 2-3 weeks)

**Goal**: prove the architecture end-to-end with one consumer, one aggregate, and a **complete docs+API demo** (Hasura + PostgREST + realtime).

This is bigger than a typical spike because we want to **demonstrate the docs-from-DB pattern** from day one — proving that adding a new entity means **automatically getting GraphQL + REST + OpenAPI + real-time** without writing API code. That value proposition is what justifies the whole rewrite.

### Scope

**Ingestion**:
- [ ] poktroll archive node locally (testnet or mainnet) with FilePlugin enabled
- [ ] `ps fileplugin` (sidecar subcommand): 300-LoC publisher that tails FilePlugin output dir, publishes to NATS with `Nats-Msg-Id`
- [ ] NATS JetStream 1-node (local dev) with the POKT_CHAIN stream

**Storage**:
- [ ] PostgreSQL 18 + TimescaleDB extension locally (latest stable)
- [ ] First migration with `block`, `consumer_consolidation`, `processed_heights`, `supplier_history`, `event_claim_settled` (hypertable), `aggregate_registry`, `bucket_seal`
- [ ] **Comprehensive `COMMENT ON`** for every table + column (this feeds Hasura + PostgREST auto-docs)

**Processing**:
- [ ] `ps consumer supplier`: one consumer module (full happy path)
- [ ] One aggregate: `rewards_hourly`, with sealing loop
- [ ] `ps reconciler`: basic drift detection vs chain (even if minimal)

**Downstream APIs** (NEW: part of Phase 1):
- [ ] **Hasura** deployed via Tilt, auto-pointed at our Postgres, exposing `supplier`/`supplier_history`/`rewards_hourly` GraphQL with auto-generated docs from `COMMENT ON`
- [ ] **PostgREST** deployed via Tilt, exposing the same data as REST + auto-generated OpenAPI
- [ ] **NATS WebSocket bridge** (small Go service in `cmd/ps` → `ps ws-bridge` or separate small repo): minimal implementation streaming `pokt.events.>` to WebSocket clients
- [ ] **Docs landing page** (static or generated): shows the 3 access methods with sample queries

**Quality**:
- [ ] One E2E test: poktroll → sidecar → NATS → consumer → Postgres → aggregate sealed → GraphQL query returns result
- [ ] Golden test for the supplier decoder
- [ ] Reconciler test (drift inject + auto-heal)

### Exit criteria

- A `MsgStakeSupplier` on the local node results in:
  1. A row in `supplier_history` within seconds.
  2. The same data queryable via Hasura GraphQL.
  3. The same data queryable via PostgREST REST.
  4. A real-time push via the NATS WS bridge to subscribed clients.
- Kill the consumer mid-stream, restart, no data loss, no duplicates.
- `rewards_hourly` bucket seals once consumer is caught up past hour end.
- **Adding a new column** (e.g., to `supplier_history`) appears in Hasura schema + PostgREST OpenAPI **automatically** on next refresh — no API code to write.

### Decision gate

If the full Phase 1 stack (ingest → write → expose) demonstrates the value proposition, proceed to Phase 2 (full module coverage). Otherwise, identify the gap and re-design before scaling scope.

## Phase 2 — MVP (target: 4-6 weeks after spike)

**Goal**: feature parity with Pocketdex for ALL entities and queries.

Scope:
- [ ] All consumer modules: supplier, application, gateway, service, session, validator, bank/balances, authz
- [ ] All event hypertables: claim_settled, proof_updated, claim_expired, mint_burn_op, supplier_slashed, relay_mining_difficulty, application_overserviced
- [ ] Multi-version decoders: at minimum v0.0.10 (alpha), beta, current
- [ ] Router with upgrades table populated from `x/upgrade` queries
- [ ] Param history (SCD2) for all module params
- [ ] Reconciler running periodically against archive node (full feature, not just spike scope)
- [ ] Continuous aggregates: hourly + daily for rewards, relays, claims (+ rollups)
- [ ] Hasura schema configured with permissions per role
- [ ] PostgREST role configuration (anon, authenticated)
- [ ] NATS WebSocket bridge with auth (JWT or API key)
- [ ] Backfill procedure documented and tested from genesis

Exit criteria:
- All Pocketdex GraphQL queries produce equivalent results within tolerance.
- Genesis-to-tip backfill completes in <12 hours on the target hardware.
- Reconciler running for a week without drift alerts.

## Phase 3 — Production (target: 2-3 weeks after MVP)

**Goal**: deploy to production infra, sunset Pocketdex.

Scope:
- [ ] 2+ archive nodes (HA active-active producers)
- [ ] NATS JetStream 3-replica cluster
- [ ] Postgres primary + 1-2 streaming replicas
- [ ] Consumers scaled per module load
- [ ] Monitoring: Prometheus metrics + Grafana dashboards
- [ ] Alerting: drift, lag, disk, replication
- [ ] Runbook for every alert
- [ ] Backup + PITR with pgBackRest
- [ ] Public docs hosted (Redoc for OpenAPI, GraphiQL for GraphQL)
- [ ] Sunset plan for Pocketdex with parallel run period

Exit criteria:
- One week of parallel-run with Pocketdex showing zero divergence.
- All clients migrated to PocketScribe endpoints.
- Pocketdex decommissioned.

## Phase 4 — Beyond

Possible next steps:

- Decentralized data layer: serve PocketScribe data as a Pocket Network service.
- Cold archive: parquet snapshots of all FilePlugin outputs for cheap reindex.
- ML feeds: realtime feature store derived from PocketScribe streams.
- Other Cosmos chains: re-use the architecture for non-poktroll Cosmos SDK indexing.

## Out of scope (deliberately)

- Frontend / explorer UI (separate project, consumes our APIs).
- Wallet / signing / mempool tracking (out of scope; this is post-finalization).
- Cross-chain (IBC) indexing as a first-class feature (later, if needed).
- Real-time analytics with sub-second latency (we target seconds, not millis).
