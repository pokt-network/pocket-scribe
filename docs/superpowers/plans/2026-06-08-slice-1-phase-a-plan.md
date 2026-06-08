# Slice 1 Phase A — Prerequisites Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Tilt + kind-based local dev stack (Postgres+TimescaleDB + NATS JetStream + migrations) and a green `make ci` baseline on a fresh Go tree, so subsequent phases (B through G) have a working ground floor.

**Architecture:** A `kind` single-node cluster (config already exists at `configs/dev/kind-cluster.yaml`) hosts K8s deployments for Postgres (timescale/timescaledb image) and NATS JetStream. The Tiltfile (currently a failing stub) is replaced with a real config that applies manifests, sets up port-forwards, and runs migrations as a local_resource depending on Postgres readiness. The Go tree gets `cmd/ps/main.go` and `internal/*` package skeletons (`doc.go` files) so `make ci` (vet + fmt-check + lint + test) exercises a non-empty toolchain.

**Tech Stack:** kind, Tilt, Kubernetes, `timescale/timescaledb:latest-pg18`, NATS 2.10+ (`nats:2.10-alpine`), Go 1.26, golangci-lint, goose, GNU make.

**Spec reference:** `docs/superpowers/specs/2026-06-08-slice-1-design.md` Section 9 Phase A.

**Pre-existing artifacts the plan builds on:**
- `configs/dev/kind-cluster.yaml` — kind cluster definition with NodePort mappings.
- `configs/networks/{mainnet,beta,localnet}.yaml` — per-network configs (NOTE: actual field is `network.genesis_decoder_version`, not `genesis_version` as the spec writes — a follow-up task here aligns the spec to reality).
- `.golangci.yml` — lint config (Go 1.26, default standard set + extras).
- `go.mod` — Go 1.26, deps for cobra/viper/pgx/goose/nats.
- `Makefile` — has `verify-migrations`, `regenerate-snapshots`, `regenerate-migrations`, `clean`.
- `schema/migrations/` — 38 migrations, 244 tables, validated.

---

### Task 1: Postgres + TimescaleDB K8s manifest

**Files:**
- Create: `deploy/dev/postgres.yaml`

The kind cluster maps containerPort 30000 to host port 5432. The Postgres Service must be type NodePort with nodePort 30000 to be reachable on `localhost:5432`.

- [ ] **Step 1: Create the manifest**

Write `deploy/dev/postgres.yaml`:

```yaml
# Postgres + TimescaleDB for local dev.
# Reachable on localhost:5432 via kind NodePort mapping (configs/dev/kind-cluster.yaml).
# Ephemeral: data is lost on pod restart. Migrations re-apply via Tilt local_resource.
---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-secret
  labels:
    app: postgres
type: Opaque
stringData:
  POSTGRES_USER: pocketscribe
  POSTGRES_PASSWORD: dev_only_password
  POSTGRES_DB: pocketscribe
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init
  labels:
    app: postgres
data:
  001-timescaledb.sql: |
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: timescale/timescaledb:latest-pg18
          ports:
            - containerPort: 5432
              name: postgres
          envFrom:
            - secretRef:
                name: postgres-secret
          volumeMounts:
            - name: init
              mountPath: /docker-entrypoint-initdb.d
            - name: data
              mountPath: /var/lib/postgresql/data
          readinessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - pocketscribe
                - -d
                - pocketscribe
            initialDelaySeconds: 5
            periodSeconds: 3
            timeoutSeconds: 3
          livenessProbe:
            exec:
              command:
                - pg_isready
                - -U
                - pocketscribe
                - -d
                - pocketscribe
            initialDelaySeconds: 30
            periodSeconds: 10
      volumes:
        - name: init
          configMap:
            name: postgres-init
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  type: NodePort
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
      nodePort: 30000
      name: postgres
```

- [ ] **Step 2: Verify YAML is parseable**

Run:

```bash
kubectl apply --dry-run=client -f deploy/dev/postgres.yaml
```

Expected: each resource validates without error (output lists each resource with `(dry run)`).

- [ ] **Step 3: Commit**

```bash
git add deploy/dev/postgres.yaml
git commit -m "feat(dev): postgres + timescaledb k8s manifest for tilt stack"
```

---

### Task 2: NATS JetStream K8s manifest

**Files:**
- Create: `deploy/dev/nats.yaml`

NodePort 30001 → host 4222 (client), 30002 → host 8222 (monitor) per kind config.

- [ ] **Step 1: Create the manifest**

Write `deploy/dev/nats.yaml`:

```yaml
# NATS JetStream — single-node for local dev.
# Reachable on localhost:4222 (client) and localhost:8222 (monitor).
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: nats-config
  labels:
    app: nats
data:
  nats.conf: |
    port: 4222
    http_port: 8222
    server_name: pocketscribe-dev
    jetstream {
      store_dir: /data
      max_memory_store: 256MB
      max_file_store: 2GB
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nats
  labels:
    app: nats
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nats
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: nats
    spec:
      containers:
        - name: nats
          image: nats:2.10-alpine
          args:
            - -c
            - /etc/nats/nats.conf
          ports:
            - containerPort: 4222
              name: client
            - containerPort: 8222
              name: monitor
          volumeMounts:
            - name: config
              mountPath: /etc/nats
            - name: data
              mountPath: /data
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8222
            initialDelaySeconds: 2
            periodSeconds: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8222
            initialDelaySeconds: 15
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: nats-config
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: nats
  labels:
    app: nats
spec:
  type: NodePort
  selector:
    app: nats
  ports:
    - port: 4222
      targetPort: 4222
      nodePort: 30001
      name: client
    - port: 8222
      targetPort: 8222
      nodePort: 30002
      name: monitor
```

- [ ] **Step 2: Verify YAML is parseable**

Run:

```bash
kubectl apply --dry-run=client -f deploy/dev/nats.yaml
```

Expected: each resource validates.

- [ ] **Step 3: Commit**

```bash
git add deploy/dev/nats.yaml
git commit -m "feat(dev): nats jetstream k8s manifest for tilt stack"
```

---

### Task 3: Cluster bringup/teardown Makefile targets

**Files:**
- Modify: `Makefile`

The existing comment in `configs/dev/kind-cluster.yaml` says "Used by `make cluster-up`" — match that naming.

- [ ] **Step 1: Add cluster-up and cluster-down targets to Makefile**

Append before `clean:` in `Makefile`:

```make
# ─── Local dev cluster (kind + Tilt) ───────────────────────────────────────

.PHONY: cluster-up cluster-down

cluster-up: ## Create the local kind cluster (idempotent)
	@kind get clusters | grep -qx pocketscribe-dev || \
	  kind create cluster --config configs/dev/kind-cluster.yaml
	@echo "kind cluster pocketscribe-dev is ready. kubectl context: kind-pocketscribe-dev"

cluster-down: ## Delete the local kind cluster
	@kind delete cluster --name pocketscribe-dev
```

Also update the `.PHONY` declaration at the top of the file to include the new targets. Find:

```make
.PHONY: help \
        verify-migrations regenerate-migrations regenerate-snapshots \
        clean
```

Replace with:

```make
.PHONY: help \
        verify-migrations regenerate-migrations regenerate-snapshots \
        cluster-up cluster-down \
        clean
```

- [ ] **Step 2: Verify make help shows the new targets**

Run:

```bash
make help
```

Expected: output includes `cluster-up` and `cluster-down` lines.

- [ ] **Step 3: Verify cluster-up works**

Run:

```bash
make cluster-up
```

Expected: either creates the cluster (first run) or no-op (subsequent runs). Final line: `kind cluster pocketscribe-dev is ready. kubectl context: kind-pocketscribe-dev`.

- [ ] **Step 4: Verify kubectl context is set**

Run:

```bash
kubectl config current-context
```

Expected: `kind-pocketscribe-dev`.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "feat(dev): make cluster-up / cluster-down targets for kind cluster"
```

---

### Task 4: Migrations runner make target

**Files:**
- Modify: `Makefile`

Wired so Tilt can call `make migrate-dev` once Postgres is ready. Uses goose against the port-forwarded Postgres (which kind exposes at localhost:5432 via NodePort 30000).

- [ ] **Step 1: Add migrate-dev target to Makefile**

Append after the new `cluster-down` target:

```make
# ─── Migrations against local dev stack ────────────────────────────────────

.PHONY: migrate-dev migrate-dev-status migrate-dev-down

DEV_PG_DSN := host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable

migrate-dev: ## Apply all goose migrations against the Tilt-managed Postgres
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" up

migrate-dev-status: ## Show goose migration status against dev Postgres
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" status

migrate-dev-down: ## Roll back one migration on dev Postgres (use with care)
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" down
```

Update `.PHONY` to include these three new targets.

- [ ] **Step 2: Verify goose is installed and migrate-dev-status works syntactically**

Run (note: actual application happens after Tilt brings Postgres up; here we just verify the command parses):

```bash
which goose && goose --version
```

Expected: a path and a version string. If not installed:

```bash
go install github.com/pressly/goose/v3/cmd/goose@latest
```

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(dev): make migrate-dev target wires goose to tilt postgres"
```

---

### Task 5: Replace Tiltfile stub with real config

**Files:**
- Modify: `Tiltfile`

Replace the failing stub with a Tiltfile that deploys Postgres + NATS + runs migrations.

- [ ] **Step 1: Write the new Tiltfile**

Replace the entire content of `Tiltfile`:

```python
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
    readiness_timeout='90s',
)

# ─── Message bus: NATS JetStream ──────────────────────────────────────────
k8s_yaml('deploy/dev/nats.yaml')
k8s_resource(
    'nats',
    port_forwards=['4222:4222', '8222:8222'],
    labels=['data'],
    readiness_timeout='60s',
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
```

- [ ] **Step 2: Verify Tilt parses the file (without deploying)**

Run:

```bash
tilt alpha tiltfile-result --file Tiltfile 2>&1 | head -20
```

Expected: no syntax errors. (If `tilt alpha tiltfile-result` is unavailable, run `tilt up --hud=false --port=10350` in a separate terminal and verify it doesn't fail to parse — see Task 8.)

- [ ] **Step 3: Commit**

```bash
git add Tiltfile
git commit -m "feat(dev): real Tiltfile with postgres + nats + migrations"
```

---

### Task 6: `make ci` target

**Files:**
- Modify: `Makefile`

The CI target chains vet + fmt-check + lint + test. Each piece is its own target so devs can run them in isolation.

- [ ] **Step 1: Add ci-related targets to Makefile**

Append after the migrate targets:

```make
# ─── CI checks (lint, vet, test, fmt) ──────────────────────────────────────

.PHONY: ci vet fmt-check fmt lint test

ci: vet fmt-check lint test ## Run all CI checks

vet: ## go vet on the whole module
	@go vet ./...

fmt-check: ## Verify gofmt is clean
	@unformatted=$$(gofmt -l . | grep -v '^vendor/' | grep -v '^third_party/'); \
	if [ -n "$$unformatted" ]; then \
	  echo "gofmt -w needed for these files:"; \
	  echo "$$unformatted"; \
	  exit 1; \
	fi

fmt: ## Apply gofmt to the whole tree
	@gofmt -w .

lint: ## Run golangci-lint
	@golangci-lint run ./...

test: ## Run go test (no race detector — see ci-race for that)
	@go test ./...

ci-race: ## Run go test with the race detector
	@go test -race ./...
```

Update `.PHONY` at the top of the file to include `ci vet fmt-check fmt lint test ci-race`.

- [ ] **Step 2: Verify golangci-lint is installed**

Run:

```bash
which golangci-lint && golangci-lint --version
```

Expected: a path and a version >= 2.0.0 (matching the `version: "2"` in `.golangci.yml`). If missing:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(make): ci target chains vet + fmt-check + lint + test"
```

---

### Task 7: Go skeleton — `cmd/ps/main.go` + `internal/*` packages with doc.go

**Files:**
- Create: `cmd/ps/main.go`
- Create: `internal/app/doc.go`
- Create: `internal/app/fileplugin/doc.go`
- Create: `internal/app/consumer/doc.go`
- Create: `internal/app/indexer/doc.go`
- Create: `internal/app/reconciler/doc.go`
- Create: `internal/app/migrate/doc.go`
- Create: `internal/app/inspect/doc.go`
- Create: `internal/app/sync/doc.go`
- Create: `internal/config/doc.go`
- Create: `internal/consumer/doc.go`
- Create: `internal/decoders/doc.go`
- Create: `internal/fileplugin/doc.go`
- Create: `internal/log/doc.go`
- Create: `internal/metrics/doc.go`
- Create: `internal/nats/doc.go`
- Create: `internal/router/doc.go`
- Create: `internal/store/doc.go`
- Create: `internal/types/doc.go`
- Create: `internal/version/doc.go`

The skeleton matches the layout in `CLAUDE.md` "Quick reference" plus the spec Section 12 CLI surface. Each `doc.go` is a 3-5 line package comment that says what the package is for and points at the spec section.

- [ ] **Step 1: Create cmd/ps/main.go**

```go
// Package main is the entry point for the ps CLI.
//
// The actual subcommand wiring lives in internal/app/* per CLAUDE.md
// quick-reference layout. This stub is a placeholder until Phase B brings
// cobra-based subcommands online.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "ps: command not yet implemented (Slice 1 Phase A skeleton)")
	os.Exit(1)
}
```

- [ ] **Step 2: Create internal/app/doc.go**

```go
// Package app holds the composition roots for each ps subcommand.
//
// Each subpackage (fileplugin, consumer, indexer, reconciler, migrate,
// inspect, sync) owns its own Cobra command construction, dependency
// wiring, and lifecycle. cmd/ps/main.go is intentionally thin.
package app
```

- [ ] **Step 3: Create internal/app/fileplugin/doc.go**

```go
// Package fileplugin is the composition root for the ps fileplugin
// subcommand (the sidecar). See docs/superpowers/specs/2026-06-08-slice-1-design.md
// Section 4.1 for the sidecar contract; ADR-022 for NATS subject discipline;
// ADR-023 for live vs bootstrap mode.
package fileplugin
```

- [ ] **Step 4: Create internal/app/consumer/doc.go**

```go
// Package consumer is the composition root for the ps consumer <module>
// subcommand. Each concrete consumer (block, supplier, ...) is wired here
// on top of the generic runtime in internal/consumer.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Sections 4.7, 4.8.
package consumer
```

- [ ] **Step 5: Create internal/app/indexer/doc.go**

```go
// Package indexer is the composition root for ps indexer, which runs all
// enabled consumer modules in one process. See CLAUDE.md CLI section.
package indexer
```

- [ ] **Step 6: Create internal/app/reconciler/doc.go**

```go
// Package reconciler is the composition root for ps reconciler.
// In Slice 1 it ships only the upgrades refresh loop (calls sync-upgrades
// periodically). Full drift detection is Slice 4.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Section 4.5.
package reconciler
```

- [ ] **Step 7: Create internal/app/migrate/doc.go**

```go
// Package migrate is the composition root for ps migrate up/down/status.
// Wraps goose against the configured Postgres DSN. See ADR-016.
package migrate
```

- [ ] **Step 8: Create internal/app/inspect/doc.go**

```go
// Package inspect is the composition root for ps inspect <thing>
// (cursors, streams, seals). Read-only observability over indexer state.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Section 12.
package inspect
```

- [ ] **Step 9: Create internal/app/sync/doc.go**

```go
// Package sync is the composition root for ps sync-upgrades. Queries
// the configured chain RPC for applied_plan/{name} and upserts the
// upgrades table. See ADR-018; spec Section 4.4.
package sync
```

- [ ] **Step 10: Create internal/config/doc.go**

```go
// Package config loads PocketScribe configuration (network YAML files
// under configs/networks/, environment overrides). Single source of
// truth for "how to reach this chain" per ADR-018.
package config
```

- [ ] **Step 11: Create internal/consumer/doc.go**

```go
// Package consumer is the generic consumer runtime: cursor tracking via
// consumer_consolidation, processed_heights writes, ack-after-commit
// pattern (invariant #5), passive gap detection, self-registration in
// consumer_registry, restart safety.
// See docs/superpowers/specs/2026-06-08-slice-1-design.md Section 4.6.
package consumer
```

- [ ] **Step 12: Create internal/decoders/doc.go**

```go
// Package decoders contains per-version decoder packages (v0_1_0 through
// v0_1_33). Each version package implements the common Decoder interface
// (block header, tx, state entity, stateless event).
// See ADR-008 (versioned decoders) and spec Section 4.2.
package decoders
```

- [ ] **Step 13: Create internal/fileplugin/doc.go**

```go
// Package fileplugin implements the sidecar that reads per-block
// FilePlugin output (block-{H}-meta + block-{H}-data) and publishes
// per-event NATS messages per ADR-022.
package fileplugin
```

- [ ] **Step 14: Create internal/log/doc.go**

```go
// Package log centralizes slog setup for PocketScribe. Structured logging
// with consistent fields (consumer, height, chain_id).
package log
```

- [ ] **Step 15: Create internal/metrics/doc.go**

```go
// Package metrics is the single source of truth for Prometheus metric
// names per CLAUDE.md DRY invariant. Other packages must register metrics
// only through here.
package metrics
```

- [ ] **Step 16: Create internal/nats/doc.go**

```go
// Package nats centralizes JetStream subject constants and helpers per
// ADR-022. Single source of truth for subject naming (CLAUDE.md DRY).
package nats
```

- [ ] **Step 17: Create internal/router/doc.go**

```go
// Package router resolves block_height -> decoder version via the
// upgrades table (DB-driven per ADR-018). No hardcoded heights.
// Periodic refresh keeps the in-memory map fresh as new upgrades land.
package router
```

- [ ] **Step 18: Create internal/store/doc.go**

```go
// Package store wraps pgx v5 + sqlc-generated queries per ADR-016.
// All Postgres access flows through this package. Migrations live in
// schema/migrations/ and are applied via goose.
package store
```

- [ ] **Step 19: Create internal/types/doc.go**

```go
// Package types defines canonical entity types (BlockHeader, Tx,
// Entity, DecodedEvent) used by decoders and consumers. The single
// shared vocabulary across versioned decoder packages.
package types
```

- [ ] **Step 20: Create internal/version/doc.go**

```go
// Package version exposes build-time version metadata (commit SHA,
// build date, semver). Populated via -ldflags at build time.
package version
```

- [ ] **Step 21: Verify the tree compiles**

Run:

```bash
go build ./...
```

Expected: succeeds silently (the only buildable artifact is `cmd/ps/main.go`; internal/* doc.go files are package declarations only).

- [ ] **Step 22: Verify make ci passes on the skeleton**

Run:

```bash
make ci
```

Expected: each step (vet, fmt-check, lint, test) succeeds. `go test ./...` will print `no test files` for every internal/* package — that is OK.

- [ ] **Step 23: Commit**

```bash
git add cmd/ internal/
git commit -m "feat(skeleton): cmd/ps + internal/* package layout with doc.go"
```

---

### Task 8: README dev workflow section

**Files:**
- Modify: `README.md`

Add a "Local development" section that walks a new contributor through bringing the stack online.

- [ ] **Step 1: Read the current README to find the best insertion point**

Run:

```bash
head -40 README.md
```

Identify where a "Local development" section fits (likely near the top, before deep architecture sections; or as its own H2 after the project intro).

- [ ] **Step 2: Add the Local development section**

Insert the following section near the top of the README (after the intro paragraphs, before any deep-architecture content):

```markdown
## Local development

### Prerequisites

- **Go 1.26+** (`go version`)
- **Docker** (with at least 8 GB RAM allocated)
- **kind** (`kind version`) — installs via `go install sigs.k8s.io/kind@latest`
- **Tilt** (`tilt version`) — see https://docs.tilt.dev/install.html
- **kubectl** (`kubectl version --client`)
- **golangci-lint v2+** (`golangci-lint --version`) — install: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
- **goose** (`goose --version`) — install: `go install github.com/pressly/goose/v3/cmd/goose@latest`

### Bringing up the local stack

```bash
make cluster-up   # one-time per session: create the kind cluster
tilt up           # brings up postgres + nats; applies migrations
                  # press space in the terminal to open the Tilt UI;
                  # ctrl-C stops it (resources keep running)

# When done:
tilt down         # remove the deployed resources (cluster stays)
make cluster-down # delete the kind cluster entirely
```

`make cluster-up` is idempotent — re-running it is a no-op if the cluster already exists.

After `tilt up` shows all resources green:
- Postgres reachable at `localhost:5432` (user `pocketscribe`, password `dev_only_password`, db `pocketscribe`).
- NATS reachable at `localhost:4222` (client) and `localhost:8222` (monitor).
- The `pocketscribe` database has the full 244-table schema applied via goose.

### CI checks locally

```bash
make ci         # vet + fmt-check + lint + test
make ci-race    # same, with the race detector
make fmt        # apply gofmt to the tree
```

### Resetting the dev stack

```bash
tilt down
tilt up    # fresh start; migrations re-run automatically
```

Or for a full reset (incl. the cluster itself):

```bash
make cluster-down
make cluster-up
tilt up
```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): local development workflow with kind + tilt"
```

---

### Task 9: Align spec to actual config field name

**Files:**
- Modify: `docs/superpowers/specs/2026-06-08-slice-1-design.md`

The spec writes `genesis_version: v0.1.0` but actual configs use the nested form `network.genesis_decoder_version: v0_1_0` (underscored). Spec needs to match reality to avoid downstream confusion.

- [ ] **Step 1: Update the spec config YAML examples in Section 4.11**

Find this block in the spec:

```yaml
# configs/networks/mainnet.yaml
chain_id: pocket
rpc: https://sauron-rpc.infra.pocket.network
genesis_version: v0.1.0

# configs/networks/beta.yaml
chain_id: pocket-lego-testnet
rpc: https://sauron-rpc.beta.infra.pocket.network
genesis_version: v0.1.32

# configs/networks/localnet.yaml
chain_id: pocket-localnet
rpc: http://localhost:26657
genesis_version: v0.1.34
```

Replace with (matching the actual structure on disk):

```yaml
# configs/networks/mainnet.yaml (excerpt)
network:
  id: pocket-mainnet
  chain_id: pocket
  genesis_decoder_version: v0_1_0
endpoints:
  rpc: [https://sauron-rpc.infra.pocket.network]

# configs/networks/localnet.yaml (excerpt)
network:
  id: pocket-localnet
  chain_id: poktroll
  genesis_decoder_version: v0_1_33
endpoints:
  rpc: [http://localhost:26657]
```

- [ ] **Step 2: Update references to `network.genesis_version` elsewhere in the spec**

Search and replace, but read each match before editing — the symbol `genesis_version` may appear in pseudocode and prose; `genesis_decoder_version` is the field name in the YAML. Pseudocode like "if V ≤ network.genesis_version" should become "if V ≤ network.genesis_decoder_version".

Run:

```bash
grep -n "genesis_version" docs/superpowers/specs/2026-06-08-slice-1-design.md
```

Edit each hit so the field name matches the actual YAML structure.

- [ ] **Step 3: Verify no stale references remain**

Run:

```bash
grep -c "genesis_version[^_]" docs/superpowers/specs/2026-06-08-slice-1-design.md
```

Expected: 0 (or only matches that are intentionally generic prose).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md
git commit -m "docs(spec): align Slice 1 spec to actual genesis_decoder_version field"
```

---

### Task 10: End-to-end smoke

**Files:** none — this is a verification gate.

- [ ] **Step 1: From a clean slate, bring up the stack**

```bash
make cluster-down 2>/dev/null || true   # ignore if no cluster exists
make cluster-up
tilt up &                                # background so this terminal stays free
TILT_PID=$!
sleep 90                                 # wait for postgres + nats + migrations
```

Expected: no errors. Tilt UI accessible at http://localhost:10350.

- [ ] **Step 2: Verify Postgres is reachable and has the schema**

```bash
PGPASSWORD=dev_only_password psql -h localhost -p 5432 -U pocketscribe -d pocketscribe -c "\dt" | head -20
```

Expected: a long list of tables — `block`, `consumer_consolidation`, `processed_heights`, `aggregate_registry`, `bucket_seal`, `supplier_history`, etc.

- [ ] **Step 3: Verify TimescaleDB extension is present**

```bash
PGPASSWORD=dev_only_password psql -h localhost -p 5432 -U pocketscribe -d pocketscribe -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';"
```

Expected: a row with `timescaledb` and a 2.x version.

- [ ] **Step 4: Verify NATS JetStream is alive**

```bash
curl -s http://localhost:8222/healthz
curl -s http://localhost:8222/jsz | head -20
```

Expected: `{"status":"ok"}` for healthz; JetStream info for jsz.

- [ ] **Step 5: Verify `make ci` is green**

```bash
make ci
```

Expected: each sub-step passes; final exit code 0.

- [ ] **Step 6: Tear down**

```bash
kill $TILT_PID 2>/dev/null || true
tilt down
# leave the cluster up if you plan to continue immediately;
# otherwise: make cluster-down
```

- [ ] **Step 7: Update the spec with Phase A done**

Add a note at the bottom of `docs/superpowers/specs/2026-06-08-slice-1-design.md` (or at the top under Status) confirming Phase A complete:

```markdown
**Phase A complete**: <commit SHA of this smoke commit> — Tilt stack green, make ci green on skeleton, ready for Phase B (Layer 0 orchestration skeleton).
```

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md
git commit -m "docs(spec): mark Slice 1 Phase A complete; ready for Phase B"
```

---

## Phase A exit checklist

When all the above commits are in place:

- [ ] `make cluster-up` brings up the kind cluster (idempotent).
- [ ] `tilt up` deploys postgres + nats + migrations; all three resources green.
- [ ] Postgres reachable at localhost:5432 with the 244-table schema.
- [ ] NATS JetStream reachable at localhost:4222 (client) and localhost:8222 (monitor).
- [ ] `make ci` exits 0 (vet, fmt-check, lint, test all green on the skeleton).
- [ ] Spec aligned to actual config field name (`genesis_decoder_version`).
- [ ] Phase A completion noted in the spec.

When this checklist is green, the foundation is ready for **Phase B — Layer 0 orchestration skeleton (tests 1-13)**, which will be planned next in its own document: `docs/superpowers/plans/<date>-slice-1-phase-b-plan.md`.
