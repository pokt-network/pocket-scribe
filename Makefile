# PocketScribe — Makefile
#
# Targets exposed here are TESTED to work today. As we add real Go code,
# build/test/lint targets will grow. The current repo state is skills +
# schema migrations + archeology artifacts; the targets below reflect that.

.PHONY: help \
        build \
        verify-migrations regenerate-migrations regenerate-snapshots \
        cluster-up cluster-down \
        migrate-dev migrate-dev-status migrate-dev-down \
        ci vet fmt-check fmt lint test test-integration ci-race \
        clean \
        tools-proto gen-proto gen-check

help: ## Print this help
	@awk 'BEGIN {FS = ":.*?## "} \
	     /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─── Schema migrations ─────────────────────────────────────────────────────

verify-migrations: ## Apply schema/migrations/* against a fresh TimescaleDB (docker)
	@bash .claude/skills/verify-migrations/run.sh

regenerate-snapshots: ## Re-extract proto shape snapshots for every vendored version
	@for vdir in third_party/proto/poktroll/*/; do \
	  v=$$(basename $$vdir | tr '_' '.'); \
	  echo "=== $$v ==="; \
	  bash .claude/skills/generate-decoder/run.sh $$v >/dev/null; \
	done
	@echo "Snapshots in docs/research/.shapes/"

regenerate-migrations: ## Re-generate schema/migrations/00NN_decoder_*.sql from snapshots
	@rm -f schema/migrations/00*_decoder_v0_1_*.sql
	@for vdir in third_party/proto/poktroll/*/; do \
	  v=$$(basename $$vdir | tr '_' '.'); \
	  bash .claude/skills/generate-migration-from-diff/run.sh $$v >/dev/null; \
	done
	@ls schema/migrations/ | grep _decoder_ | wc -l | awk '{print "Migrations: "$$1}'

# ─── Proto codegen (buf) ───────────────────────────────────────────────────

BUF_VERSION       := v1.70.0
GOGOPROTO_VERSION := v1.7.0
PROTO_BIN         := $(shell go env GOPATH)/bin

tools-proto: ## Install pinned buf + protoc-gen-gocosmos into GOPATH/bin
	@go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	@go install github.com/cosmos/gogoproto/protoc-gen-gocosmos@$(GOGOPROTO_VERSION)
	@echo "Installed buf $(BUF_VERSION) + protoc-gen-gocosmos $(GOGOPROTO_VERSION) into $(PROTO_BIN)"

# Versions that own a generated tree (shape-range representatives + v0_1_30,
# which Phase C committed; range map: docs/research/supplier-shape-breaks.md).
DECODER_GEN_VERSIONS := v0_1_0 v0_1_8 v0_1_27 v0_1_30

gen-proto: tools-proto ## Generate decoder bindings (offline, ephemeral buf workspaces) + envelope, then strip global registrations
	@for v in $(DECODER_GEN_VERSIONS); do \
	  bash scripts/gen_decoder_protos.sh $$v || exit 1; \
	done
	@PATH="$(PROTO_BIN):$$PATH" buf generate --template buf.gen.envelope.yaml internal/proto
	@go run ./tools/stripregister $(foreach v,$(DECODER_GEN_VERSIONS),internal/decoders/$(v)/gen)
	@echo "Generated + stripped: $(DECODER_GEN_VERSIONS) and internal/proto/gen/"

gen-check: ## Verify committed generated code matches the protos (regenerate + strip + diff)
	@$(MAKE) gen-proto >/dev/null
	@if ! git diff --quiet -- $(foreach v,$(DECODER_GEN_VERSIONS),internal/decoders/$(v)/gen) internal/proto/gen; then \
	  echo "generated code is stale; run 'make gen-proto' and commit the result:"; \
	  git --no-pager diff --stat -- internal/decoders/*/gen internal/proto/gen; \
	  exit 1; \
	fi
	@echo "generated code up to date."

# ─── Local dev cluster (kind + Tilt) ───────────────────────────────────────

cluster-up: ## Create the local kind cluster (idempotent)
	@kind get clusters | grep -qx pocketscribe-dev || \
	  kind create cluster --config configs/dev/kind-cluster.yaml
	@echo "kind cluster pocketscribe-dev is ready. kubectl context: kind-pocketscribe-dev"

cluster-down: ## Delete the local kind cluster
	@kind delete cluster --name pocketscribe-dev

# ─── Migrations against local dev stack ────────────────────────────────────

DEV_PG_DSN := host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable

migrate-dev: ## Apply all goose migrations against the Tilt-managed Postgres
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" up

migrate-dev-status: ## Show goose migration status against dev Postgres
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" status

migrate-dev-down: ## Roll back one migration on dev Postgres (use with care)
	@goose -dir schema/migrations postgres "$(DEV_PG_DSN)" down

# ─── CI checks (lint, vet, test, fmt) ──────────────────────────────────────

ci: vet fmt-check lint test ## Run all CI checks

vet: ## go vet on the whole module
	@go vet ./...

fmt-check: ## Verify gofmt is clean
	@unformatted=$$(gofmt -l . | grep -v '^vendor/' | grep -v '^third_party/' | grep -v '^\.claude/'); \
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

test-integration: ## Run container-backed integration tests (needs Docker)
	@go test -tags=integration -count=1 ./test/...

ci-race: ## Run go test with the race detector
	@go test -race ./...

# ─── Build ─────────────────────────────────────────────────────────────────

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X github.com/pokt-network/pocketscribe/internal/version.Version=$(VERSION) \
           -X github.com/pokt-network/pocketscribe/internal/version.Commit=$(COMMIT) \
           -X github.com/pokt-network/pocketscribe/internal/version.Date=$(DATE)

build: ## Build the ps binary into bin/ps with version metadata
	@go build -ldflags "$(LDFLAGS)" -o bin/ps ./cmd/ps

# ─── Housekeeping ──────────────────────────────────────────────────────────

clean: ## Remove transient outputs (.shapes, generated migrations)
	@rm -rf docs/research/.shapes/
	@rm -f schema/migrations/00*_decoder_v0_1_*.sql
	@echo "Cleaned snapshots + decoder migrations."
