# -*- mode: Python -*-
# PocketScribe — Tilt dev stack
#
# Brings up: kind cluster (assumed already running via `make cluster-up`),
# Postgres + TimescaleDB, NATS JetStream, applies migrations, runs consumers
# and the sidecar replay as host processes (local_resource) so that the sidecar
# can read /tmp tarballs directly without requiring an extraMount on kind.
#
# Architecture decision (sidecar option b):
#   The kind cluster at pocketscribe-dev has no /tmp mount (only /lib/modules).
#   Re-creating the cluster with extraMounts works but breaks the "already running"
#   constraint. Running `ps fileplugin` and the consumers as local_resource gives
#   Tilt-managed log aggregation, dependency ordering, and file-watch restarts with
#   zero image builds and no cluster changes.

# Refuse to run against any context except our local kind cluster.
allow_k8s_contexts('kind-pocketscribe-dev')

# ─── Shared env / DSN ─────────────────────────────────────────────────────────
DEV_DSN = 'host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable'
DEV_NATS = 'nats://localhost:4222'
MAINNET_CONFIG = 'configs/networks/mainnet.yaml'

# ─── Data plane: Postgres + TimescaleDB ───────────────────────────────────────
k8s_yaml('deploy/dev/postgres.yaml')
k8s_resource(
    'postgres',
    port_forwards=['5432:5432'],
    labels=['data'],
)

# ─── Message bus: NATS JetStream ──────────────────────────────────────────────
k8s_yaml('deploy/dev/nats.yaml')
k8s_resource(
    'nats',
    port_forwards=['4222:4222', '8222:8222'],
    labels=['data'],
)

# ─── Schema migrations ────────────────────────────────────────────────────────
# Runs goose against the port-forwarded Postgres.  Re-runs whenever a migration
# file changes.  Depends on postgres being ready.
local_resource(
    'migrations',
    cmd = 'make migrate-dev',
    deps = ['schema/migrations'],
    resource_deps = ['postgres'],
    labels = ['data'],
    allow_parallel = False,
)

# ─── Upgrade registry ─────────────────────────────────────────────────────────
# One-shot: populates the `upgrades` table from Sauron (mainnet).
# Trigger manually after the first migration or when upgrade list changes.
# Marked trigger_mode=TRIGGER_MODE_MANUAL so it does not auto-run on every
# tilt up (it queries a remote endpoint and is idempotent but slow).
local_resource(
    'sync-upgrades',
    cmd = 'go run ./cmd/ps sync-upgrades --config ' + MAINNET_CONFIG + ' --dsn "' + DEV_DSN + '"',
    deps = [],
    resource_deps = ['migrations'],
    labels = ['pipeline'],
    trigger_mode = TRIGGER_MODE_MANUAL,
    allow_parallel = False,
)

# ─── Consumers (long-running, restart on source change) ───────────────────────
# Both connect to port-forwarded Postgres and NATS.

local_resource(
    'consumer-block',
    serve_cmd = 'go run ./cmd/ps consumer block --dsn "' + DEV_DSN + '" --nats-url ' + DEV_NATS,
    deps = [
        'internal/consumer/block',
        'internal/app/consumer/block.go',
        'internal/store',
        'internal/nats',
        'internal/router',
        'internal/decoders',
    ],
    resource_deps = ['migrations', 'nats'],
    labels = ['pipeline'],
)

local_resource(
    'consumer-supplier',
    serve_cmd = 'go run ./cmd/ps consumer supplier --config ' + MAINNET_CONFIG + ' --dsn "' + DEV_DSN + '" --nats-url ' + DEV_NATS,
    deps = [
        'internal/consumer/supplier',
        'internal/app/consumer/supplier.go',
        'internal/store',
        'internal/nats',
        'internal/router',
        'internal/decoders',
    ],
    resource_deps = ['migrations', 'sync-upgrades', 'nats'],
    labels = ['pipeline'],
)

# ─── Sidecar replay (manual trigger, era by era) ──────────────────────────────
# Run `scripts/replay_era.sh v0.1.X` from a terminal to extract a tarball and
# feed it to the sidecar.  This Tilt resource wraps a single manual invocation
# so the run appears in the Tilt UI and logs are aggregated.
#
# Usage from Tilt UI: set ERA_VERSION in the cmd below to the desired version
# (e.g. v0.1.0) and click the refresh button, or call replay_era.sh directly
# from a terminal (preferred for sequential era-by-era replay).
#
# ERA_VERSION is controlled by replay_era.sh; this resource is a convenience
# handle — it will exit 0 immediately if called without a real ERA_VERSION.
local_resource(
    'sidecar-replay',
    cmd = 'echo "Trigger replay via: ./scripts/replay_era.sh <version> — e.g. ./scripts/replay_era.sh v0.1.0"',
    deps = ['scripts/replay_era.sh'],
    resource_deps = ['nats'],
    labels = ['pipeline'],
    trigger_mode = TRIGGER_MODE_MANUAL,
)
