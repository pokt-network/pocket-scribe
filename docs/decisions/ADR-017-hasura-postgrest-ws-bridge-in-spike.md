# ADR-017: Hasura, PostgREST, and NATS WS bridge are part of the Phase 1 spike

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude
**Supersedes**: partially supersedes the "downstream APIs out of scope" framing in ADR-011 (still true that they're SEPARATE components — but they ARE in the initial demo).

## Context

ADR-011 establishes that Hasura, PostgREST, and the NATS WebSocket bridge are **architecturally out of PocketScribe's scope** — they're separate components that consume our Postgres + NATS firehose, owned by downstream teams.

But the user identified that **deferring these components to later phases hides the project's central value proposition**: "podemos documentar y desplegar acceso a la db rapido y facil, con real-time tambien, pero ademas nosotros podemos establecer la estructura correcta para codear la relacion modelos/tablas -> query/docs."

The value of PocketScribe isn't just "we ingest the chain into Postgres faster than Pocketdex." It's "we **structure the data** so that GraphQL + REST + OpenAPI + real-time all come for **free** from the schema." Without demonstrating this end-to-end early, we're missing the most important proof point.

## Decision

The Phase 1 spike includes:
1. **Hasura GraphQL** deployed via Tilt, auto-pointed at our Postgres.
2. **PostgREST + OpenAPI** deployed via Tilt, same DB.
3. **NATS WebSocket bridge** (minimal Go service) streaming `pokt.events.>` to clients.
4. **Comprehensive `COMMENT ON`** on every table + column in `schema/migrations/0001_init.sql`.

Spike exit criterion adds: "Adding a new column appears in Hasura schema + PostgREST OpenAPI automatically — no API code written."

**ADR-011 still applies for production**: in production, these components are deployed/owned/configured by downstream teams. The Phase 1 spike just demonstrates the pattern.

## Consequences

### Positive

- **Value prop visible from week 2.** Demo audience (team, stakeholders) sees: schema change → GraphQL/REST/WS updated automatically.
- **The "right structure for table → query/docs" gets baked in early.** Naming, COMMENT ON discipline, permissions all established before Phase 2 scales.
- **Catches integration issues early.** Hasura quirks (read replicas, subscription modes), PostgREST role setup, WS bridge backpressure — all addressed before Phase 2.
- **Forces schema documentation.** Without comprehensive `COMMENT ON`, Hasura auto-docs are useless. This pressure makes the schema better.
- **Real-time pattern proven.** NATS WS bridge is a small component but the architecture (NATS as event bus, not LISTEN/NOTIFY) is validated.

### Negative

- **Phase 1 is bigger** (~3 weeks instead of 2). Acceptable tradeoff.
- **More moving parts to monitor in dev.** Tilt brings Hasura + PostgREST + WS bridge online. Mitigated by clear labels in Tilt UI.
- **Risk of "spike scope creep"** — could try to over-build Hasura permissions, PostgREST role hierarchy, WS auth. **Mitigation: spike scope explicitly minimal**: no auth, default roles, only `supplier` exposed initially.

### Neutral

- ADR-011's separation of concerns (PocketScribe doesn't OWN these components) remains valid for production. The spike includes them for **demonstration**.

## Alternatives considered

### Option A: Defer Hasura/PostgREST/WS to Phase 2 (original plan)
- Pro: smaller Phase 1.
- **Con (fatal)**: misses demonstrating the central value proposition. Team / stakeholders see "yet another indexer that writes to Postgres" — uninspiring.
- **Rejected** by this ADR.

### Option B: Build a custom Go HTTP server bundled with PocketScribe
- Pro: full control.
- Con: violates ADR-011 separation; doubles the codebase.
- **Rejected**: Hasura + PostgREST already solve this; reinventing isn't justified.

### Option C: Defer just WS bridge (Hasura streaming subs alone)
- Pro: even smaller scope.
- Con: Hasura streaming subs poll Postgres every 1-2s. Doesn't demonstrate sub-second real-time, which is part of the value prop.
- **Rejected**: WS bridge is small enough (~150 LoC) to include.

## Implementation notes

### Tiltfile

Uncomment Hasura + PostgREST helm resources. Add a tiny `ws-bridge` resource. All under the `downstream` label so dev can `tilt up --resources=-downstream` if they want to skip.

### Configs

- `configs/downstream/hasura-metadata.json` — declarative metadata: tracked tables, relationships, permissions (`anon` role: read-only). Imported on Hasura startup.
- `configs/downstream/postgrest.conf` — `db-anon-role`, `db-schemas`, `db-max-rows`, OpenAPI generation enabled.
- `configs/downstream/ws-bridge.yaml` — subjects to expose, auth: none (spike), backpressure config.

### `COMMENT ON` discipline

Migration `0001_init.sql` updated to comment **every table and every non-obvious column**. Future migrations also include `COMMENT ON` for additions. The reviewer agent should flag missing comments.

### CLI

Add `ps ws-bridge` subcommand (Phase 1) or a separate small binary `cmd/ws-bridge/main.go`. Decision: keep as subcommand for the spike (single binary still wins).

### Docs landing page

A simple static HTML / markdown landing at `docs/api/index.md` (or a tiny generator script) showing:
- "GraphQL: http://localhost:8080" → link to GraphiQL.
- "REST: http://localhost:3000" → link to Swagger UI over OpenAPI.
- "Real-time: ws://localhost:9090/stream" → with curl example.
- Sample queries for `supplier`, `rewards_hourly`.

## References

- User request that prompted this ADR: "hasura y postgres deberian ser parte inicial... podemos documentar y desplegar acceso a la db rapido y facil, con real-time tambien, pero ademas nosotros podemos establecer la estructura correcta para codear la relacion modelos/tablas -> query/docs."
- ADR-011 — original separation of concerns (still valid for production).
- ADR-006 — chain-as-source-of-truth (the data we expose).
- `docs/architecture/06-apis.md` — full description of the API layer.
- ROADMAP.md Phase 1 — updated scope.
