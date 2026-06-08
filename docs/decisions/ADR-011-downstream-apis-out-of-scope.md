# ADR-011: Downstream APIs (Hasura, PostgREST, NATS WS bridge) are NOT in PocketScribe scope

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

After the user clarified that "el indexer per se" is the producer of indexed data, and that "los procesos de exponer la data son otros, hasura, postgres etc." — the boundary was made explicit: PocketScribe writes data; other components expose it.

The question is whether we should bundle / ship those exposers (Hasura, PostgREST, NATS WS bridge) with PocketScribe or keep them out of scope.

## Decision

**Downstream APIs are explicitly OUT OF SCOPE for PocketScribe.** The repo:
- Documents the Postgres schema (with `COMMENT ON` for auto-docs).
- Documents NATS subject contracts.
- Provides examples / dev configs (for verification only, see `Tiltfile`).
- **Does not ship** Hasura metadata as a release artifact, does not own PostgREST config, does not own the NATS WS bridge implementation.

## Consequences

### Positive

- **Tight scope.** PocketScribe team owns one job (indexing) extremely well.
- **Independent iteration.** Hasura schema changes don't require PocketScribe releases.
- **Team boundaries respected.** Frontend team owns Hasura; SRE may own NATS bridge; PocketScribe team owns the data.
- **Downstream choice.** Some consumers may prefer to skip Hasura/PostgREST and query Postgres views directly with a thin Go SDK (`pkg/scribeclient`).
- **No HTTP server in PocketScribe.** No CORS, no rate limiting, no auth concerns to address in this codebase.

### Negative

- **More components to deploy** at the system level (PocketScribe + Hasura + PostgREST + WS bridge).
- **Documentation burden** — must document the contracts (schema, subjects) clearly enough that downstream teams can self-serve.
- **Coordination overhead** — schema changes in PocketScribe may require Hasura metadata updates (notifying downstream).

### Neutral

- Tilt dev stack includes Hasura + PostgREST for verification — but they're explicitly tagged `downstream` and disable-able (`tilt up --resources=-hasura,-postgrest`).

## Alternatives considered

### Option A: Bundle a complete API server (custom Go HTTP/GraphQL)
- Pro: one binary; full control over auth, rate limiting, custom views.
- Con: doubles the codebase; couples API iteration to indexer release cycle.
- Con: yet another GraphQL/REST server in the world.
- **Rejected because**: violates focused-scope principle; the OSS ecosystem already has Hasura/PostgREST.

### Option B: Bundle Hasura config as a release artifact
- Pro: easier onboarding (download metadata + connect).
- Con: Hasura config can drift (custom permissions per deployment).
- Con: implies PocketScribe owns Hasura version compatibility.
- **Partial adoption**: dev `Tiltfile` includes Hasura for verification; production config is downstream's responsibility.

### Option C: Bundle PostgREST config only (REST, no GraphQL)
- Pro: simpler than Hasura.
- Con: still couples API iteration; some consumers want GraphQL.
- **Rejected for same reason as B**.

## Implementation notes

### What PocketScribe DOES provide

- Stable Postgres schema with `COMMENT ON` everywhere (auto-docs via Hasura/PostgREST).
- `aggregate_registry.status = 'public'` filter for which aggregates to expose.
- `safe_height` view for cross-entity-consistency queries.
- `bucket_seal` for "is this bucket trustworthy?" checks.
- Stable NATS subject contracts (`pokt.block.{height}`, `pokt.kv.{store}.{height}`, etc.).
- A reference Go SDK (`pkg/scribeclient`) for programmatic access — minimalist, may not exist in MVP.

### What downstream teams MUST own

- Hasura metadata / role permissions / JWT secret.
- PostgREST config (anonymous role, JWT, max rows).
- NATS WS bridge implementation (subject filters, auth, backpressure).
- API gateway / rate limiting / caching.
- CORS, TLS, HTTPS termination.
- API versioning strategy.
- API documentation hosting (Redoc, GraphiQL, etc.).

### Verification scope in PocketScribe

- The Tilt dev stack starts Hasura + PostgREST against the same Postgres for end-to-end testing.
- E2E tests may include "submit a GraphQL query, assert expected response" but only for smoke-testing schema/COMMENTs.
- Beyond smoke-testing, downstream owns the testing.

## References

- Full session transcript: Topic 16 (project scope clarification during naming).
- `docs/architecture/06-apis.md` — what we support architecturally.
- CLAUDE.md mission statement ("acts as a producer of indexed data").
