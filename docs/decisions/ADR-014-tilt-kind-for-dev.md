# ADR-014: Tilt + kind/k3d for local dev (over docker-compose)

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

Local dev for a multi-service Go project needs:
- **Hot reload** on file save (fast feedback loop).
- **Prod parity** (same orchestration as production = K8s).
- **Aggregated logs** + metrics for debugging when N pods restart per minute.
- **Simple bring-up** (one command).

Two options:
- **docker-compose**: simple, fast bring-up, but diverges from prod K8s.
- **Tilt + kind/k3d**: matches prod K8s, hot-reload via `live_update`, requires K8s knowledge.

## Decision

Use **Tilt orchestrating a kind (or k3d) cluster** for all local development. `docker-compose` is reserved for one-off needs only (e.g., quick smoke test).

## Consequences

### Positive

- **Prod parity.** What runs in dev kind matches what runs in prod K8s. Manifests/Helm charts are shared.
- **Hot reload.** Tilt's `live_update` syncs the freshly-built binary into the running pod (no image rebuild) — <2s per save.
- **Aggregated logs in Tilt UI.** Color-coded per service, filterable, live tail.
- **Observability stack runs in dev.** Prometheus + Grafana + Loki run alongside services — same dashboards work locally and in prod.
- **Easy resource toggling.** `tilt up --resources=consumer-supplier,nats,postgres` brings up just what you need.
- **Manifests are version controlled.** `deploy/k8s/dev/` mirrors `deploy/k8s/prod/`.

### Negative

- **Steeper onboarding** for devs unfamiliar with K8s. Mitigated by `make install-deps` automating the prerequisites.
- **Heavier resource usage** than docker-compose — kind clusters consume more CPU/RAM than raw docker containers.
- **Tilt-specific knowledge** required for Tiltfile customization.

### Neutral

- Both options need Docker.

## Alternatives considered

### Option A: docker-compose only
- Pro: simpler, faster bring-up.
- Pro: no K8s knowledge needed.
- **Con (fatal)**: dev environment diverges from prod. Subtle bugs (networking, secrets, service discovery) only surface in prod.
- **Rejected**: bites teams sooner or later.

### Option B: Skaffold + kind
- Pro: similar value prop to Tilt.
- Con: less mature UI, fewer extensions, smaller community.
- **Rejected** in favor of Tilt's better dev UX.

### Option C: Run everything natively on the host (no containers)
- Pro: fastest possible iteration.
- Con: every dev has different Postgres version, different NATS version, different OS quirks.
- Con: doesn't scale to multi-service orchestration.
- **Rejected**: containers are non-negotiable for reproducibility.

## Implementation notes

### `Tiltfile` structure
- Build `bin/ps` on the host (cached Go toolchain) → `live_update` syncs into a thin distroless image.
- Helm charts for stateful services (Postgres, NATS, Grafana).
- Raw K8s manifests for PocketScribe services + poktroll devnet.
- Labels for grouping (`db`, `bus`, `chain`, `consumers`, `observability`, `downstream`, `ops`).

### Cluster choice
- **kind** (Kubernetes IN Docker) for most devs — wide compatibility, well-documented.
- **k3d** as alternative (lighter; K3s is a leaner K8s distribution). Either works.
- Cluster name standardized: `pocketscribe-dev`.

### Observability
- Prometheus + Grafana + Loki + Promtail installed by Tilt.
- Local URLs: Grafana http://localhost:3001, Prometheus http://localhost:9090.
- Same dashboards/queries used in prod.

### Resources
- kind cluster: 4 CPU / 8 GB RAM minimum.
- Recommended dev workstation: 16+ GB RAM, 8+ CPU.

### Exception: when docker-compose is OK
- Quick demos / one-offs that don't represent ongoing dev work.
- Lightweight tests that only need one service (e.g., a standalone Postgres for a unit test).
- Not in `make dev`. Always parallel to Tilt, never replacing.

## References

- Full session transcript: Topic "Tilt + kind/k3d for local dev (prod parity)".
- `Tiltfile` — actual config.
- `docs/operations/development-workflow.md` — workflow patterns.
- `configs/dev/kind-cluster.yaml` — kind cluster definition.
