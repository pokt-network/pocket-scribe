# -*- mode: Python -*-
# PocketScribe — Tilt dev stack
#
# Brings up: kind cluster (assumed already running via `make cluster-up`),
# Postgres + TimescaleDB, NATS JetStream, applies migrations.
#
# Slice 1 Phase A scope. Subsequent slices add: sidecar, consumers, reconciler,
# Hasura, PostgREST, WS bridge.

# Refuse to run against any context except our local kind cluster.
# Prevents accidental deploys to a remote cluster.
allow_k8s_contexts('kind-pocketscribe-dev')

# ─── Data plane: Postgres + TimescaleDB ───────────────────────────────────
k8s_yaml('deploy/dev/postgres.yaml')
k8s_resource(
    'postgres',
    port_forwards=['5432:5432'],
    labels=['data'],
)

# ─── Message bus: NATS JetStream ──────────────────────────────────────────
k8s_yaml('deploy/dev/nats.yaml')
k8s_resource(
    'nats',
    port_forwards=['4222:4222', '8222:8222'],
    labels=['data'],
)

# ─── Schema migrations ────────────────────────────────────────────────────
# Runs goose against the port-forwarded Postgres. Re-runs whenever a migration
# file changes. Depends on postgres being ready.
local_resource(
    'migrations',
    cmd='make migrate-dev',
    deps=['schema/migrations'],
    resource_deps=['postgres'],
    labels=['data'],
    allow_parallel=False,
)
