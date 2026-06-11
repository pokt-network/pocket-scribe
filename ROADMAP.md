# PocketScribe Roadmap

Phased plan from zero to production. Each phase has an exit criterion and an explicit decision gate.

## Phase 0 — Design corpus (DONE)

Output: this repository, fully documented design + skeleton.

Exit: human reviews and approves direction.

## Phase 1 — Spike (in progress — Slice 1 done 2026-06-11)

**Goal**: prove the architecture end-to-end with one consumer, one aggregate, and a **complete docs+API demo** (Hasura + PostgREST + realtime).

Phase 1 is executed as four vertical slices, each independently demoable. See the [Slice 1 spec](./docs/superpowers/specs/2026-06-08-slice-1-design.md) §2 for the full decomposition. **Slice 1 shipped 2026-06-11.** Slices 2–4 are pending.

### Scope

**Ingestion**:
- [x] poktroll archive node locally (testnet or mainnet) with FilePlugin enabled
- [x] `ps fileplugin` (sidecar subcommand): publisher that tails FilePlugin output dir, fans out per-tx/event/KV to NATS with `Nats-Msg-Id` and `Pocket-Block-Time` header (ADR-022)
- [x] NATS JetStream 1-node (local dev) with the POKT_CHAIN stream

**Storage**:
- [x] PostgreSQL 18 + TimescaleDB extension locally (latest stable)
- [x] Migrations with `block`, `consumer_consolidation`, `consumer_registry`, `processed_heights`, `supplier_history`, `supplier_service_config_update_history`, `aggregate_registry`, `bucket_seal`, `upgrades`
- [ ] **Comprehensive `COMMENT ON`** for every table + column (Slice 3)

**Processing**:
- [x] `ps consumer block` and `ps consumer supplier`: consumers across 31 protocol versions with multi-version decoder library
- [x] `ps reconciler`: upgrades-table refresh (minimal — entity drift detection is Slice 4)
- [x] `ps sync-upgrades`: populates `upgrades` table from mainnet LCD
- [ ] One aggregate: `rewards_hourly`, with sealing loop (Slice 2)

**Downstream APIs** (Slices 3–4):
- [ ] **Hasura** deployed via Tilt — Slice 3
- [ ] **PostgREST** deployed via Tilt — Slice 3
- [ ] **NATS WebSocket bridge** (`ps ws-bridge`) — Slice 4
- [ ] **Docs landing page** — Slice 3

**Quality**:
- [x] 27 spec test scenarios (§11.1) green; golden tests for block + supplier decoder across real fixture heights
- [x] `make ci-full` clean: 100% decoders / ≥90% internal/ coverage gate; `.github/workflows/ci.yml`
- [ ] Full E2E test with live node + aggregate seal + GraphQL query (Slice 4)
- [ ] Reconciler entity drift inject + auto-heal test (Slice 4)

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
