# Slice 1 Phase B — Layer 0 Orchestration Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the version-agnostic consumer orchestration skeleton — a generic consumer runtime (cursor tracking, ack-after-commit, passive gap detection, self-registration) plus per-height sealing logic — driven entirely by synthetic fixtures, with zero chain-decoding code, so that spec Section 11.1 tests 1–13 are green.

**Architecture:** A NATS JetStream stream carries one synthetic message per block height (`pokt.block.<H>`, marker-byte payload, deterministic `Nats-Msg-Id`). A generic `consumer.Runtime` subscribes via a durable pull consumer, and for each message runs an ack-after-commit transaction in the `store` package: `BEGIN → handler.Handle(tx) → INSERT processed_heights → advance consumer_consolidation contiguously → COMMIT → Ack`. Two `NoOpHandler` instances run in parallel to exercise the AND-seal logic. Sealing is a derived query over `consumer_registry` (active set) × `consumer_consolidation` (per-consumer high-water mark) — no materialization. Consumers self-register into the new `consumer_registry` table; `ps deregister-consumer` flips `active=false`. Version-gating of the required set (`FirstValidVersion`) is stored but **not yet enforced** — that is deferred to Phase F per the spec build order.

**Tech Stack:** Go 1.26 · cobra v1.10.1 · viper v1.21.0 · pgx/v5 v5.8.0 (+ pgxpool, stdlib) · goose v3.27.0 (embedded FS) · nats.go v1.46.1 (`jetstream` package) · prometheus/client_golang v1.24.0 · slog (stdlib) · testcontainers-go v0.40.0 (postgres + nats modules) · TimescaleDB (`timescale/timescaledb:latest-pg18`). Tests use the stdlib `testing` package only (no testify).

**Spec reference:** `docs/superpowers/specs/2026-06-08-slice-1-design.md` Section 9 Phase B; exit criterion Section 11.1 tests 1–13; design constraints Sections 4.6, 4.9, 4.10, 6.

**ADR constraints honored:** ADR-005 (append-only, no `valid_to_*`), ADR-007 (per-module consumer + ack-after-commit), ADR-016 (all Postgres access through `store`), ADR-018 (no hardcoded upgrades; config-driven), ADR-022 (NATS subject + `Nats-Msg-Id` discipline).

**Pre-existing artifacts the plan builds on:**
- `schema/migrations/0001_init.sql` — already defines `block`, `processed_heights` (`PK(consumer_name, height)`), `consumer_consolidation` (`PK consumer_name`, `consolidated_up_to BIGINT DEFAULT 0`, `updated_at`), `aggregate_registry`, `bucket_seal`. **Do not modify these.**
- `schema/migrations/0038_decoder_v0_1_33.sql` — current highest migration. Next number is `0039`.
- `configs/networks/{mainnet,beta,localnet}.yaml` — network config files the loader parses. Key path `network.genesis_decoder_version` (underscored, e.g. `v0_1_0`).
- `internal/*/doc.go` — 19 package-doc stubs declaring intent. Implementation files land beside them.
- `cmd/ps/main.go` — stub that exits 1. Task 2 replaces it with cobra wiring.
- `Makefile`, `Tiltfile`, `.golangci.yml` — Phase A dev stack; extended, not rewritten.

---

## Hard rules for the executor (read once, obey throughout)

1. **No `Co-Authored-By` / AI-attribution footer** in any commit message. Project rule.
2. **No `time.Now()` / `clock_timestamp()` as a queryable axis.** `forbidigo` enforces this. SQL `now()` on audit columns (`consumer_consolidation.updated_at`, `consumer_registry.registered_at/deregistered_at`) is allowed — those are metadata, never a `WHERE`/`GROUP BY` axis. Go-side wall-clock reads are not needed anywhere in Phase B; if you reach for one, stop and reconsider.
3. **Append-only.** The only `UPDATE`s allowed are on cursor/registry metadata: `consumer_consolidation.consolidated_up_to`, `consumer_registry.active/deregistered_at`. Never `UPDATE` a data row.
4. **All Postgres access flows through `internal/store`** (ADR-016). Handlers receive a `pgx.Tx` and call store helpers; they never open their own connection.
5. **DRY single sources of truth:** NATS subjects → `internal/nats/subjects.go`; metric names → `internal/metrics/metrics.go`; config loading → `internal/config`. Do not duplicate these strings elsewhere.
6. **`archeology/` is a separate Go module.** Never add deps to the root `go.mod` to satisfy it; never let its code into the root module's build/lint.
7. **TDD:** for every behavioral task, write the failing test first, see it fail for the right reason, implement minimally, see it pass, commit.
8. **Integration tests are build-tagged `//go:build integration`** and run via `make test-integration` (Task 5 adds it). `make test` / `make ci` stay container-free and fast. `make ci` must be green at the end of every task.
9. **Adding a dependency.** Phase B starts from a bare `go.mod` (Task 1). When a task first imports an external module, pin it explicitly: `go get <module>@<version>` **then** `go mod tidy`, before building. This keeps every commit's `go.mod`/`go.sum` diff exact. Pinned versions: cobra `v1.10.1`, viper `v1.21.0`, pgx/v5 `v5.8.0`, goose/v3 `v3.27.0`, nats.go `v1.46.1`, prometheus/client_golang `v1.24.0`, testcontainers-go + `modules/postgres` + `modules/nats` `v0.40.0`. (`github.com/docker/docker` + `github.com/docker/go-connections` are imported directly by the fixed-port test helper but come transitively from testcontainers — `go mod tidy` records them; no explicit `go get` needed.) Phase B uses **no** `testify`, `uuid`, or `goldie`.

---

## File inventory (what this plan creates / modifies)

**Create:**
- `schema/migrations/0039_consumer_registry.sql`
- `schema/embed.go` — `//go:embed migrations/*.sql` → `embed.FS`
- `internal/version/version.go`
- `internal/app/root.go`, `internal/app/version_cmd.go`, `internal/app/migrate/cmd.go`, `internal/app/deregister/cmd.go`
- `internal/config/config.go`, `internal/config/types.go`, `internal/config/config_test.go`
- `internal/store/store.go`, `internal/store/migrate.go`, `internal/store/consumer_registry.go`, `internal/store/processed_heights.go`, `internal/store/consumer_consolidation.go`, `internal/store/process.go`, `internal/store/seal.go`
- `internal/nats/subjects.go`, `internal/nats/subjects_test.go`, `internal/nats/client.go`
- `internal/metrics/metrics.go`, `internal/log/log.go`
- `internal/consumer/types.go`, `internal/consumer/noop.go`, `internal/consumer/runtime.go`
- `test/testcontainers/postgres.go`, `test/testcontainers/nats.go`, `test/testcontainers/main_test.go`
- `test/fixtures/synthetic/synthetic.go`
- `test/integration/migrations_test.go`, `test/integration/registry_test.go`, `test/integration/cursor_test.go`, `test/integration/seal_test.go`, `test/integration/runtime_test.go`, `test/integration/resilience_test.go`, `test/integration/deregister_test.go`

**Modify:**
- `go.mod` (Task 1 trims; Task 16 finalizes via tidy)
- `cmd/ps/main.go` (Task 2)
- `Makefile` (Task 2 `build`+ldflags; Task 5 `test-integration`)
- `docs/superpowers/specs/2026-06-08-slice-1-design.md` (Task 16 marks Phase B complete)

> The doc.go stubs in `internal/version`, `internal/store`, `internal/config`, `internal/nats`, `internal/metrics`, `internal/log`, `internal/consumer`, `internal/app/*` already exist — add `.go` files beside them; leave the doc.go files in place.

---

## Task 1: Trim `go.mod` to a Phase-B-buildable dependency set

**Why first:** Phase A's `go.mod` lists forward-looking decoder/proto/observability deps that Phase B never imports, and at least one pin is broken (`cosmossdk.io/store v1.5.2` → "unknown revision store/v1.5.2"). That broken pin makes `go mod tidy` fail. Nothing in Phase B imports cosmos/cometbft/protobuf/grpc/otel, so we remove them (and the cometbft `replace`) now and re-introduce them in Phase C when the codegen pipeline lands.

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Reduce `go.mod` to a bare module declaration**

Rewrite `go.mod` to exactly this — dropping every `require` and the cometbft `replace`:

```go
module github.com/pokt-network/pocketscribe

go 1.26

// Dependencies are added per-task via `go get <module>@<version>` as imports
// land (Hard rule 9), then finalized with `go mod tidy` in Task 17. The
// decoder/proto/observability deps removed here (cosmos-sdk, cometbft,
// cosmossdk.io/store, protobuf, grpc, otel) and the cometbft `replace` return
// in Phase C with the codegen pipeline.
```

> Phase A's `go.mod` listed forward-looking deps, one with a broken pin (`cosmossdk.io/store v1.5.2` → "unknown revision store/v1.5.2") that makes `go mod tidy` fail. Nothing in Phase B imports them. Starting bare and pinning each dep with `go get @version` at first use keeps every commit's `go.mod`/`go.sum` diff minimal and exact.

- [ ] **Step 2: Tidy and confirm the stub module still builds**

Run:
```bash
go mod tidy
go build ./...
make ci
```
Expected: `go mod tidy` succeeds now that the broken pin is gone, leaving an empty `require` block and a trimmed `go.sum`; `go build ./...` exits 0 (only `doc.go` stubs); `make ci` green.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore(deps): reset go.mod to bare module; defer cosmos/proto to Phase C"
```

---

## Task 2: Version metadata + cobra root + `ps version`

Brings the CLI online (Phase A forward debt). After this task `ps version` prints build metadata injected via `-ldflags`, and `cmd/ps/main.go` is a thin cobra entrypoint instead of an exit-1 stub.

**Files:**
- Create: `internal/version/version.go`, `internal/app/root.go`, `internal/app/version_cmd.go`
- Create test: `internal/version/version_test.go`
- Modify: `cmd/ps/main.go`, `Makefile`

- [ ] **Step 1: Write the failing test for the version package**

`internal/version/version_test.go`:
```go
package version

import (
	"strings"
	"testing"
)

func TestStringContainsAllFields(t *testing.T) {
	Version, Commit, Date = "v1.2.3", "abc1234", "2026-06-08T00:00:00Z"
	got := String()
	for _, want := range []string{"v1.2.3", "abc1234", "2026-06-08T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, missing %q", got, want)
		}
	}
}

func TestDefaults(t *testing.T) {
	if Version == "" || Commit == "" || Date == "" {
		t.Fatal("version vars must have non-empty defaults")
	}
}
```

- [ ] **Step 2: Run it; verify it fails to compile**

Run: `go test ./internal/version/...`
Expected: FAIL — `undefined: Version` / `undefined: String`.

- [ ] **Step 3: Implement `internal/version/version.go`**

```go
package version

import "fmt"

// Build-time metadata. Overridden via -ldflags -X (see Makefile `build`).
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String renders the version line shown by `ps version`.
func String() string {
	return fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, Date)
}
```

- [ ] **Step 4: Run the test; verify it passes**

Run: `go test ./internal/version/...`
Expected: PASS.

- [ ] **Step 5: Implement the cobra root and `ps version` subcommand**

`internal/app/root.go`:
```go
package app

import "github.com/spf13/cobra"

// NewRootCmd builds the `ps` command tree. cmd/ps/main.go executes it.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ps",
		Short:         "PocketScribe — a Go-native indexer for Pocket Network's Shannon protocol",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd())
	return root
}
```

`internal/app/version_cmd.go` (package `app`) — the version subcommand constructor lives directly in `package app` next to the root, keeping the command tree in one place and avoiding any name clash with the `internal/version` library package:

```go
package app

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/version"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), version.String())
			return nil
		},
	}
}
```

> There is no `internal/app/version/` package — Phase A created only `internal/version/`. Do not add an `internal/app/version/` subpackage; the version subcommand is a constructor in `package app`.

`cmd/ps/main.go` (replace the stub entirely):
```go
// Package main is the entry point for the ps CLI. All subcommand wiring lives
// in internal/app; this file stays thin.
package main

import (
	"fmt"
	"os"

	"github.com/pokt-network/pocketscribe/internal/app"
)

func main() {
	if err := app.NewRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ps:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 6: Add a `build` target with `-ldflags` version injection to the Makefile**

Insert after the `# ─── Housekeeping ──` section header (or before it — anywhere in the file). Add `build` to the `.PHONY` list as well.

```makefile
# ─── Build ─────────────────────────────────────────────────────────────────

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X github.com/pokt-network/pocketscribe/internal/version.Version=$(VERSION) \
           -X github.com/pokt-network/pocketscribe/internal/version.Commit=$(COMMIT) \
           -X github.com/pokt-network/pocketscribe/internal/version.Date=$(DATE)

build: ## Build the ps binary into bin/ps with version metadata
	@go build -ldflags "$(LDFLAGS)" -o bin/ps ./cmd/ps
```

> `$(shell date ...)` is a Makefile build-time value, not a Go `time.Now()` read — `forbidigo` does not see it, and `Date` is build metadata (never a query axis). Leave `bin/` untracked (add `bin/` to `.gitignore` if not already present).

- [ ] **Step 7: Verify the binary builds and reports version**

Run:
```bash
echo "/bin/" >> .gitignore   # only if not already ignored
make build
./bin/ps version
./bin/ps --help
```
Expected: `ps version` prints e.g. `v0.x.y-... (commit <sha>, built <ts>)`; `--help` lists `version`. `make ci` still green: `make ci`.

- [ ] **Step 8: Commit**

```bash
git add internal/version cmd/ps/main.go internal/app/root.go internal/app/version_cmd.go Makefile .gitignore
git commit -m "feat(cli): cobra root with ps version and -ldflags build metadata"
```

---

## Task 3: Embedded migrations + store base (pool) + `ps migrate`

Wires goose against the embedded migration set so `ps migrate up/down/status` works from any working directory and from a shipped binary (Phase A forward debt). Establishes the `store.Store` pgx pool wrapper that every later task uses. The authoritative apply-against-a-database test lands in Task 5 (it needs the testcontainers harness); this task verifies compilation and `ps migrate --help`.

**Files:**
- Create: `schema/embed.go`, `internal/store/store.go`, `internal/store/migrate.go`, `internal/app/migrate/cmd.go`
- Modify: `internal/app/root.go` (register the subcommand)

- [ ] **Step 1: Embed the migrations**

`schema/embed.go`:
```go
// Package schema embeds the SQL migration set so goose can apply it from a
// compiled binary, independent of the working directory.
package schema

import "embed"

// Migrations holds every numbered goose migration under schema/migrations/.
//
//go:embed migrations/*.sql
var Migrations embed.FS
```

- [ ] **Step 2: Implement the store pool wrapper**

`internal/store/store.go`:
```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the single entry point for all Postgres access (ADR-016). It owns a
// pgx connection pool; every query in the indexer goes through this package.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a pgx pool against dsn (libpq keyword or URL form) and verifies
// connectivity with a ping.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for advanced callers (tests, bulk copy).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }
```

- [ ] **Step 3: Implement the goose migration runner**

`internal/store/migrate.go`:
```go
package store

import (
	"context"
	"database/sql"
	"fmt"

	// Register the pgx stdlib driver under the name "pgx" for goose, which
	// requires a *sql.DB. This is the migration-tool boundary only — all
	// runtime data access still goes through pgxpool above (ADR-016 honored;
	// database/sql is not used for query work).
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/pokt-network/pocketscribe/schema"
)

// Migrate applies the embedded migration set against dsn. command is one of
// "up", "down", "status". It opens its own short-lived *sql.DB because goose
// operates on database/sql, not pgxpool.
func Migrate(ctx context.Context, dsn, command string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open sql db for goose: %w", err)
	}
	defer db.Close()

	goose.SetBaseFS(schema.Migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}

	const dir = "migrations" // relative to the embed root in schema.Migrations
	switch command {
	case "up":
		return goose.UpContext(ctx, db, dir)
	case "down":
		return goose.DownContext(ctx, db, dir)
	case "status":
		return goose.StatusContext(ctx, db, dir)
	default:
		return fmt.Errorf("unknown migrate command %q (want up|down|status)", command)
	}
}
```

- [ ] **Step 4: Implement the `ps migrate` subcommand**

`internal/app/migrate/cmd.go`:
```go
package migrate

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// defaultDSN matches the Tilt-managed dev Postgres (see Makefile DEV_PG_DSN).
const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds `ps migrate {up,down,status}`.
func NewCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply, roll back, or inspect schema migrations (goose)",
	}
	cmd.PersistentFlags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")

	for _, sub := range []struct{ use, short, cmd string }{
		{"up", "Apply all pending migrations", "up"},
		{"down", "Roll back the most recent migration", "down"},
		{"status", "Show migration status", "status"},
	} {
		cmd.AddCommand(&cobra.Command{
			Use:   sub.use,
			Short: sub.short,
			Args:  cobra.NoArgs,
			RunE: func(c *cobra.Command, _ []string) error {
				return store.Migrate(c.Context(), dsn, sub.cmd)
			},
		})
	}
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

Register it in `internal/app/root.go` — add the import and one line:
```go
import (
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/app/migrate"
)
// ... inside NewRootCmd, after root.AddCommand(newVersionCmd()):
	root.AddCommand(migrate.NewCmd())
```

- [ ] **Step 5: Verify it compiles and the command tree is correct**

Run:
```bash
go build ./...
go run ./cmd/ps migrate --help
go run ./cmd/ps migrate status --help
```
Expected: builds clean; `migrate` shows `up`, `down`, `status`; `status --help` shows the inherited `--dsn` flag. (A live apply is tested in Task 5.) `make ci` green.

- [ ] **Step 6: Commit**

```bash
git add schema/embed.go internal/store/store.go internal/store/migrate.go internal/app/migrate internal/app/root.go
git commit -m "feat(migrate): embedded goose migrations + store pool + ps migrate"
```

---

## Task 4: Migration `0039_consumer_registry.sql`

Adds the only new table in Phase B. It backs self-registration and the `required_set` derivation. DDL is taken verbatim from spec Section 4.9.

**Files:**
- Create: `schema/migrations/0039_consumer_registry.sql`

- [ ] **Step 1: Write the migration**

`schema/migrations/0039_consumer_registry.sql`:
```sql
-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS consumer_registry (
    consumer_name        TEXT PRIMARY KEY,
    first_valid_version  TEXT NOT NULL,        -- semver tag, e.g. "v0.1.0"
    active               BOOLEAN NOT NULL DEFAULT true,
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    deregistered_at      TIMESTAMPTZ
);
-- +goose StatementEnd
-- +goose StatementBegin
COMMENT ON TABLE consumer_registry IS
    'Self-registered consumer instances. Used to derive required_set(H) for per-height sealing. Consumers INSERT on startup (idempotent via ON CONFLICT DO NOTHING). Operators flip active=false via ps deregister-consumer for explicit decommission.';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS consumer_registry;
-- +goose StatementEnd
```

> `now()` here is a column default for audit metadata (`registered_at`), never a query axis — invariant-safe. `first_valid_version` is stored for every consumer but **not enforced** in Phase B sealing; Phase F adds semver-gated `required_set(H)`.

- [ ] **Step 2: Verify the full migration set applies cleanly against a disposable TimescaleDB**

Run: `make verify-migrations`
Expected: the `verify-migrations` skill spins `timescale/timescaledb:latest-pg18`, applies `0001`…`0039` with `goose up`, and reports success (no error, `consumer_registry` among the created tables). If it reports a failure, fix the SQL and re-run.

- [ ] **Step 3: Commit**

```bash
git add schema/migrations/0039_consumer_registry.sql
git commit -m "feat(schema): 0039 consumer_registry table for self-registration"
```

---

## Task 5: testcontainers harness + integration build tag + first migration test

Stands up the shared-container test harness used by every later integration test, and proves the embedded migrations (including `0039`) apply to a real database. Establishes the `//go:build integration` convention and a `make test-integration` target so containers never slow `make test` / `make ci`.

**Files:**
- Create: `test/testcontainers/postgres.go`, `test/testcontainers/nats.go`, `test/testcontainers/main_test.go`
- Create: `test/integration/migrations_test.go`
- Modify: `Makefile`

- [ ] **Step 1: Add the `test-integration` target to the Makefile**

Add `test-integration` to `.PHONY` and this target near `test`:
```makefile
test-integration: ## Run container-backed integration tests (needs Docker)
	@go test -tags=integration -count=1 ./test/...
```

- [ ] **Step 2: Write the Postgres harness**

`test/testcontainers/postgres.go`:
```go
//go:build integration

// Package testcontainers provides TimescaleDB + NATS JetStream container
// constructors for integration tests. Everything here is integration-tagged so
// it never compiles into the fast unit build. It is a pure constructor library;
// the package-shared singletons + TestMain live in test/integration.
package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// pgImage matches the dev stack and the verify-migrations skill.
const pgImage = "timescale/timescaledb:latest-pg18"

// PG bundles a running TimescaleDB container with a connected pool.
type PG struct {
	DSN       string
	Pool      *pgxpool.Pool
	Container *postgres.PostgresContainer
}

// StartPostgres launches a TimescaleDB container, applies every embedded
// migration, and returns a connected pool. extra customizers let the resilience
// test bind a fixed host port.
func StartPostgres(ctx context.Context, extra ...testcontainers.ContainerCustomizer) (*PG, error) {
	opts := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("pocketscribe"),
		postgres.WithUsername("pocketscribe"),
		postgres.WithPassword("dev_only_password"),
		// The v0.40 postgres module applies NO wait strategy by default — without
		// this the container is reported "ready" before Postgres accepts
		// connections and goose fails. BasicWaitStrategies waits for the readiness
		// log (twice, due to the init restart) and the listening port.
		postgres.BasicWaitStrategies(),
	}
	opts = append(opts, extra...)

	c, err := postgres.Run(ctx, pgImage, opts...)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}
	if err := store.Migrate(ctx, dsn, "up"); err != nil {
		return nil, fmt.Errorf("apply migrations: %w", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return &PG{DSN: dsn, Pool: pool, Container: c}, nil
}

// Reset truncates the Phase B coordination tables so a test starts clean
// without re-applying migrations.
func (pg *PG) Reset(t *testing.T) {
	t.Helper()
	_, err := pg.Pool.Exec(context.Background(),
		`TRUNCATE consumer_registry, consumer_consolidation, processed_heights RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("reset coordination tables: %v", err)
	}
}

// PostgresFixedPort starts a dedicated TimescaleDB container bound to hostPort
// so it can be stopped and restarted at a stable address. Used only by the
// Postgres-restart resilience test. Terminated via t.Cleanup.
func PostgresFixedPort(t *testing.T, hostPort string) *PG {
	t.Helper()
	ctx := context.Background()
	pg, err := StartPostgres(ctx, fixedHostPort("5432/tcp", hostPort))
	if err != nil {
		t.Fatalf("start fixed-port postgres: %v", err)
	}
	t.Cleanup(func() {
		pg.Pool.Close()
		_ = pg.Container.Terminate(ctx)
	})
	return pg
}

// fixedHostPort binds containerPort (e.g. "5432/tcp") to a fixed hostPort so a
// container keeps the same address across stop/start.
func fixedHostPort(containerPort, hostPort string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.HostConfigModifier = func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				nat.Port(containerPort): []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: hostPort}},
			}
		}
		return nil
	}
}
```

> `docker/docker` and `docker/go-connections` are transitive deps of testcontainers-go; importing them directly makes `go mod tidy` record them as direct requires (expected). No separate `wait` import is needed — `BasicWaitStrategies()` comes from the postgres module.

- [ ] **Step 3: Write the NATS harness**

`test/testcontainers/nats.go`:
```go
//go:build integration

package testcontainers

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

// natsImage pins a JetStream-capable server. The testcontainers nats module
// starts it with `-js` (JetStream enabled) by default in v0.40.
const natsImage = "nats:2.10-alpine"

// NC bundles a running NATS JetStream container with a connected client.
type NC struct {
	URL       string
	Client    *natsx.Client
	Container *tcnats.NATSContainer
}

// StartNATS launches a NATS JetStream server and returns a connected client.
// extra customizers let the resilience test bind a fixed host port.
func StartNATS(ctx context.Context, extra ...testcontainers.ContainerCustomizer) (*NC, error) {
	c, err := tcnats.Run(ctx, natsImage, extra...)
	if err != nil {
		return nil, fmt.Errorf("start nats: %w", err)
	}
	url, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("nats connection string: %w", err)
	}
	client, err := natsx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	return &NC{URL: url, Client: client, Container: c}, nil
}

// NATSFixedPort starts a dedicated NATS JetStream container bound to hostPort so
// it can be stopped and restarted at a stable address (NATS-reconnect test).
// Terminated via t.Cleanup.
func NATSFixedPort(t *testing.T, hostPort string) *NC {
	t.Helper()
	ctx := context.Background()
	nc, err := StartNATS(ctx, fixedHostPort("4222/tcp", hostPort))
	if err != nil {
		t.Fatalf("start fixed-port nats: %v", err)
	}
	t.Cleanup(func() {
		nc.Client.Close()
		_ = nc.Container.Terminate(ctx)
	})
	return nc
}
```

> The testcontainers nats module (v0.40) starts the server with `-js`, so JetStream is enabled without extra flags. A `Stop`+`Start` bounce preserves the JetStream file storage (and thus the stream + durable + messages), which the NATS-reconnect test relies on.

- [ ] **Step 4: Own the shared containers from the integration package**

The `testcontainers` package above is a pure constructor library (no `_test.go`, no singletons). The package-shared instances and `TestMain` live in `test/integration`, so exactly one TimescaleDB + one NATS boot for the whole suite (the 39-migration apply happens once). Tests reference the package-level `pg` / `nats` vars and isolate via `pg.Reset(t)` + `freshStream(t)`.

`test/integration/main_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	tc "github.com/pokt-network/pocketscribe/test/testcontainers"
)

// Package-shared harness, booted once in TestMain.
var (
	pg   *tc.PG
	nats *tc.NC
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	p, err := tc.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness postgres:", err)
		os.Exit(1)
	}
	n, err := tc.StartNATS(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness nats:", err)
		os.Exit(1)
	}
	pg, nats = p, n
	code := m.Run()
	pg.Pool.Close()
	_ = pg.Container.Terminate(ctx)
	nats.Client.Close()
	_ = nats.Container.Terminate(ctx)
	os.Exit(code)
}
```

- [ ] **Step 5: Write the migrations integration test**

`test/integration/migrations_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"
)

// Test (spec Phase B): existing + new migrations apply cleanly; consumer_registry exists.
func TestMigrationsApplyAndRegistryExists(t *testing.T) {
	ctx := context.Background()

	var regclass *string
	err := pg.Pool.QueryRow(ctx, `SELECT to_regclass('public.consumer_registry')::text`).Scan(&regclass)
	if err != nil {
		t.Fatalf("query regclass: %v", err)
	}
	if regclass == nil || *regclass != "consumer_registry" {
		t.Fatalf("consumer_registry table missing after migrate up (got %v)", regclass)
	}

	// Sanity: the Phase A coordination tables are present too.
	for _, tbl := range []string{"processed_heights", "consumer_consolidation", "block", "bucket_seal"} {
		var n int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, tbl).Scan(&n); err != nil {
			t.Fatalf("check %s: %v", tbl, err)
		}
		if n != 1 {
			t.Fatalf("expected table %s to exist", tbl)
		}
	}
}
```

- [ ] **Step 6: Run the integration suite**

Run: `make test-integration`
Expected: containers start, 39 migrations apply, `TestMigrationsApplyAndRegistryExists` PASS. Also confirm the fast path is unaffected: `make ci` (no containers, green).

- [ ] **Step 7: Commit**

```bash
git add test/testcontainers test/integration/main_test.go test/integration/migrations_test.go Makefile
git commit -m "test(integration): testcontainers harness + migrations-apply test"
```

---

## Task 6: `internal/config` network YAML loader

The loader for `configs/networks/<name>.yaml` (Phase A forward debt). Single source of truth for config (DRY invariant). Phase B does not yet wire it into a running command — it is unit-tested standalone and consumed by later phases (`ps sync-upgrades`, consumers). Structs mirror the **actual** YAML keys (underscored `genesis_decoder_version`), not the spec prose.

**Files:**
- Create: `internal/config/types.go`, `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test against the real config files**

`internal/config/config_test.go`:
```go
package config

import "testing"

func TestLoadMainnet(t *testing.T) {
	cfg, err := Load("../../configs/networks/mainnet.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Network.ID != "pocket-mainnet" {
		t.Errorf("Network.ID = %q, want pocket-mainnet", cfg.Network.ID)
	}
	if cfg.Network.ChainID != "pocket" {
		t.Errorf("Network.ChainID = %q, want pocket", cfg.Network.ChainID)
	}
	if cfg.Network.GenesisDecoderVersion != "v0_1_0" {
		t.Errorf("GenesisDecoderVersion = %q, want v0_1_0", cfg.Network.GenesisDecoderVersion)
	}
	if cfg.Network.GenesisHeight != 1 {
		t.Errorf("GenesisHeight = %d, want 1", cfg.Network.GenesisHeight)
	}
	if len(cfg.Endpoints.RPC) == 0 {
		t.Error("expected at least one RPC endpoint")
	}
}

func TestLoadLocalnetDynamicGenesisTime(t *testing.T) {
	cfg, err := Load("../../configs/networks/localnet.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// genesis_time is the literal string "dynamic" on localnet — must not error.
	if cfg.Network.GenesisTime != "dynamic" {
		t.Errorf("GenesisTime = %q, want dynamic", cfg.Network.GenesisTime)
	}
	if cfg.Network.GenesisDecoderVersion != "v0_1_33" {
		t.Errorf("GenesisDecoderVersion = %q, want v0_1_33", cfg.Network.GenesisDecoderVersion)
	}
}

func TestLoadValidatesRequiredFields(t *testing.T) {
	if _, err := Load("testdata/missing_chain_id.yaml"); err == nil {
		t.Fatal("expected validation error for missing chain_id")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("testdata/does_not_exist.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}
}
```

Also create the invalid fixture `internal/config/testdata/missing_chain_id.yaml`:
```yaml
network:
  id: broken
  display_name: Missing chain_id
  genesis_height: 1
  genesis_decoder_version: v0_1_0
endpoints:
  rpc:
    - http://localhost:26657
```

- [ ] **Step 2: Run it; verify it fails**

Run: `go test ./internal/config/...`
Expected: FAIL — `undefined: Load`.

- [ ] **Step 3: Implement the config types**

`internal/config/types.go`:
```go
package config

// Config is a parsed network config file (configs/networks/<name>.yaml).
type Config struct {
	Network   Network   `mapstructure:"network"`
	Endpoints Endpoints `mapstructure:"endpoints"`
}

// Network describes the chain a PocketScribe deployment indexes. Field names
// mirror the on-disk YAML exactly (ADR-018: config is the source of truth).
type Network struct {
	ID                    string `mapstructure:"id"`
	ChainID               string `mapstructure:"chain_id"`
	DisplayName           string `mapstructure:"display_name"`
	GenesisHeight         int64  `mapstructure:"genesis_height"`
	GenesisTime           string `mapstructure:"genesis_time"` // RFC3339 or the literal "dynamic" (localnet)
	GenesisDecoderVersion string `mapstructure:"genesis_decoder_version"` // underscored, e.g. "v0_1_0"
	StartHeight           *int64 `mapstructure:"start_height"`            // optional partial-history bootstrap (ADR-019)
}

// Endpoints lists the chain access endpoints. Each is a list; any may be empty.
type Endpoints struct {
	RPC  []string `mapstructure:"rpc"`
	LCD  []string `mapstructure:"lcd"`
	GRPC []string `mapstructure:"grpc"`
}
```

- [ ] **Step 4: Implement the loader**

`internal/config/config.go`:
```go
package config // package comment lives in internal/config/doc.go (do not repeat it — revive package-comments)

import (
	"fmt"

	"github.com/spf13/viper"
)

// Load reads and validates a network config file at path.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config %s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	var missing []string
	if c.Network.ID == "" {
		missing = append(missing, "network.id")
	}
	if c.Network.ChainID == "" {
		missing = append(missing, "network.chain_id")
	}
	if c.Network.GenesisDecoderVersion == "" {
		missing = append(missing, "network.genesis_decoder_version")
	}
	if c.Network.GenesisHeight < 1 {
		missing = append(missing, "network.genesis_height (must be >= 1)")
	}
	if len(c.Endpoints.RPC) == 0 {
		missing = append(missing, "endpoints.rpc (need at least one)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid fields: %v", missing)
	}
	return nil
}
```

- [ ] **Step 5: Run the tests; verify they pass**

Run: `go test ./internal/config/...`
Expected: PASS (all four). `make ci` green.

- [ ] **Step 6: Commit**

```bash
git add internal/config
git commit -m "feat(config): network YAML loader with validation"
```

---

## Task 7: NATS subjects (single source of truth) + JetStream client

Defines the subject scheme and deterministic `Nats-Msg-Id` (ADR-022) in **one** place, plus a thin JetStream client wrapper used by the publisher and the runtime. Subject parsing/building is pure and unit-tested; the client wrapper is exercised by the integration tests.

**Files:**
- Create: `internal/nats/subjects.go`, `internal/nats/subjects_test.go`, `internal/nats/client.go`

- [ ] **Step 1: Write the failing test for subjects + MsgID**

`internal/nats/subjects_test.go`:
```go
package nats

import "testing"

func TestBlockSubjectRoundTrip(t *testing.T) {
	subj := BlockSubject(635505)
	if subj != "pokt.block.635505" {
		t.Fatalf("BlockSubject = %q, want pokt.block.635505", subj)
	}
	h, err := HeightFromBlockSubject(subj)
	if err != nil {
		t.Fatalf("HeightFromBlockSubject: %v", err)
	}
	if h != 635505 {
		t.Fatalf("height = %d, want 635505", h)
	}
}

func TestHeightFromBadSubject(t *testing.T) {
	for _, bad := range []string{"pokt.block.", "pokt.block.abc", "pokt.tx.5.0", "garbage"} {
		if _, err := HeightFromBlockSubject(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestMsgIDDeterministic(t *testing.T) {
	a := MsgID(BlockSubject(5), 5, 0)
	b := MsgID(BlockSubject(5), 5, 0)
	if a != b {
		t.Fatalf("MsgID not deterministic: %q != %q", a, b)
	}
	if a == MsgID(BlockSubject(6), 6, 0) {
		t.Fatal("MsgID collided across heights")
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `go test ./internal/nats/...`
Expected: FAIL — undefined identifiers.

- [ ] **Step 3: Implement `subjects.go`**

`internal/nats/subjects.go`:
```go
package nats

import (
	"fmt"
	"strconv"
	"strings"
)

// StreamName is the JetStream stream that carries all PocketScribe chain
// messages. Single source of truth — do not redeclare elsewhere (DRY).
const StreamName = "POKT"

// StreamSubjects are the subject filters bound to StreamName.
var StreamSubjects = []string{"pokt.>"}

// BlockSubjectFilter is the wildcard a block-level consumer subscribes to (one
// message per height). Single source of truth — consumers and tests MUST use
// this constant rather than re-typing the literal (DRY).
const BlockSubjectFilter = "pokt.block.*"

const blockPrefix = "pokt.block."

// BlockSubject returns the subject carrying the block envelope for height h
// (ADR-022: one block-level message per height).
func BlockSubject(h int64) string {
	return blockPrefix + strconv.FormatInt(h, 10)
}

// HeightFromBlockSubject parses the height out of a pokt.block.<H> subject.
func HeightFromBlockSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, blockPrefix) {
		return 0, fmt.Errorf("not a block subject: %q", subject)
	}
	rest := subject[len(blockPrefix):]
	if rest == "" || strings.Contains(rest, ".") {
		return 0, fmt.Errorf("malformed block subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// MsgID returns the deterministic Nats-Msg-Id for a message, derived from
// (subject, height, intra-block index) per ADR-022. Replaying the same logical
// message always yields the same id, enabling JetStream dedup.
func MsgID(subject string, height int64, index int) string {
	return fmt.Sprintf("%s|%d|%d", subject, height, index)
}
```

- [ ] **Step 4: Run the test; verify it passes**

Run: `go test ./internal/nats/...`
Expected: PASS.

- [ ] **Step 5: Implement the JetStream client wrapper**

`internal/nats/client.go`:
```go
package nats

import (
	"context"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client wraps a NATS connection and its JetStream context. It auto-reconnects
// indefinitely so a transient NATS outage does not kill a consumer.
type Client struct {
	nc *natsgo.Conn
	js jetstream.JetStream
}

// Connect dials url with infinite reconnect and returns a JetStream-ready
// client.
func Connect(ctx context.Context, url string) (*Client, error) {
	nc, err := natsgo.Connect(url,
		natsgo.RetryOnFailedConnect(true),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(200*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats %s: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream context: %w", err)
	}
	return &Client{nc: nc, js: js}, nil
}

// JetStream returns the JetStream context.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Conn returns the underlying NATS connection.
func (c *Client) Conn() *natsgo.Conn { return c.nc }

// Close drains and closes the connection.
func (c *Client) Close() { c.nc.Close() }

// EnsureStream creates or updates the POKT stream with a dedup window so that
// duplicate Nats-Msg-Ids within the window are dropped by the server.
func (c *Client) EnsureStream(ctx context.Context, dedupeWindow time.Duration) (jetstream.Stream, error) {
	return c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   StreamSubjects,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: dedupeWindow,
	})
}
```

> `dedupeWindow` is a configured `time.Duration` value, not a wall-clock read — no `forbidigo` concern.

- [ ] **Step 6: Verify build**

Run: `go build ./... && go test ./internal/nats/...`
Expected: builds; subject tests PASS. `make ci` green.

- [ ] **Step 7: Commit**

```bash
git add internal/nats/subjects.go internal/nats/subjects_test.go internal/nats/client.go
git commit -m "feat(nats): subject scheme + deterministic Nats-Msg-Id + JetStream client"
```

---

## Task 8: Minimal metrics + slog setup

Single source of truth for metric names (DRY invariant) and a small structured-logger constructor. Both are tiny but real — the runtime increments the gap counter and writes structured gap logs.

**Files:**
- Create: `internal/metrics/metrics.go`, `internal/log/log.go`
- Create test: `internal/metrics/metrics_test.go`

- [ ] **Step 1: Write the failing metrics test**

`internal/metrics/metrics_test.go`:
```go
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestConsumerMetricsRegisterAndCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConsumer(reg)

	m.GapsTotal.WithLabelValues("noop-a").Inc()
	m.GapsTotal.WithLabelValues("noop-a").Inc()

	if got := testutil.ToFloat64(m.GapsTotal.WithLabelValues("noop-a")); got != 2 {
		t.Fatalf("gaps_total = %v, want 2", got)
	}
	m.Consolidated.WithLabelValues("noop-a").Set(42)
	if got := testutil.ToFloat64(m.Consolidated.WithLabelValues("noop-a")); got != 42 {
		t.Fatalf("consolidated_up_to = %v, want 42", got)
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `go test ./internal/metrics/...`
Expected: FAIL — `undefined: NewConsumer`.

- [ ] **Step 3: Implement metrics**

`internal/metrics/metrics.go`:
```go
package metrics // package comment lives in internal/metrics/doc.go (do not repeat it — revive package-comments)

import "github.com/prometheus/client_golang/prometheus"

const namespace = "pocketscribe"

// Consumer holds the metrics emitted by the generic consumer runtime.
type Consumer struct {
	Processed    *prometheus.CounterVec // messages successfully processed
	GapsTotal    *prometheus.CounterVec // times a gap was observed during consolidation
	Consolidated *prometheus.GaugeVec   // current consolidated_up_to high-water mark
}

// NewConsumer constructs and registers the consumer metric vectors on reg.
func NewConsumer(reg prometheus.Registerer) *Consumer {
	counter := func(name, help string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, []string{"consumer"})
		reg.MustRegister(v)
		return v
	}
	gauge := func(name, help string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, []string{"consumer"})
		reg.MustRegister(v)
		return v
	}
	return &Consumer{
		Processed:    counter("messages_processed_total", "Messages processed (committed) per consumer."),
		GapsTotal:    counter("gaps_total", "Gap observations during contiguous consolidation, per consumer."),
		Consolidated: gauge("consolidated_up_to", "Per-consumer contiguous high-water mark (block height)."),
	}
}
```

- [ ] **Step 4: Implement the logger**

`internal/log/log.go`:
```go
package log // package comment lives in internal/log/doc.go (do not repeat it — revive package-comments)

import (
	"io"
	"log/slog"
)

// New returns a JSON slog.Logger writing to w at the given level.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
```

- [ ] **Step 5: Run the test; verify it passes**

Run: `go test ./internal/metrics/... ./internal/log/...`
Expected: metrics PASS; log builds. `make ci` green.

- [ ] **Step 6: Commit**

```bash
git add internal/metrics internal/log
git commit -m "feat(observability): consumer metric vectors + slog setup"
```

---

## Task 9: Store — `consumer_registry` CRUD (self-registration, decommission, active set)

Implements the registry operations. Tests cover **spec test 9** (self-registration is idempotent) and the registry half of **test 13** (decommission flips `active=false`).

**Files:**
- Create: `internal/store/consumer_registry.go`
- Create test: `test/integration/registry_test.go`

- [ ] **Step 1: Write the failing integration test**

`test/integration/registry_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"
)

func TestSelfRegistrationIdempotent(t *testing.T) { // spec test 9
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for i := 0; i < 3; i++ {
		if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
			t.Fatalf("register #%d: %v", i, err)
		}
	}
	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM consumer_registry WHERE consumer_name = 'noop-a'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 registry row after 3 registrations, got %d", count)
	}
}

func TestDeregisterFlipsActive(t *testing.T) { // registry half of spec test 13
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")
	_ = s.RegisterConsumer(ctx, "noop-b", "v0.1.0")

	changed, err := s.DeregisterConsumer(ctx, "noop-b")
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if !changed {
		t.Fatal("expected deregister to change a row")
	}
	active, err := s.RequiredSet(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0] != "noop-a" {
		t.Fatalf("RequiredSet = %v, want [noop-a]", active)
	}

	// Deregistering again is a no-op.
	changed, _ = s.DeregisterConsumer(ctx, "noop-b")
	if changed {
		t.Fatal("second deregister should not change a row")
	}
}
```

Add a shared helper in `test/integration/helpers_test.go`:
```go
//go:build integration

package integration

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// storeFrom wraps the shared pool in a *store.Store for tests that exercise the
// store API. (Store.New pings; the shared pool is already live, so we build the
// Store directly from the shared DSN.)
func storeFrom(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(t.Context(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}
```

> `t.Context()` is available in Go 1.26's `testing` package and is canceled at test cleanup — use it for store calls.

- [ ] **Step 2: Run it; verify it fails**

Run: `make test-integration`
Expected: FAIL — `undefined: (*store.Store).RegisterConsumer` etc.

- [ ] **Step 3: Implement the registry store methods**

`internal/store/consumer_registry.go`:
```go
package store

import (
	"context"
	"fmt"
)

// RegisterConsumer idempotently records a consumer in consumer_registry. Called
// on consumer startup; re-running never duplicates a row (ON CONFLICT DO NOTHING).
func (s *Store) RegisterConsumer(ctx context.Context, name, firstValidVersion string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name) DO NOTHING`,
		name, firstValidVersion)
	if err != nil {
		return fmt.Errorf("register consumer %q: %w", name, err)
	}
	return nil
}

// DeregisterConsumer flips active=false for an explicit decommission. Returns
// true if a currently-active row was changed. This UPDATE touches registry
// metadata only (allowed exception to append-only).
func (s *Store) DeregisterConsumer(ctx context.Context, name string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE consumer_registry
		 SET active = false, deregistered_at = now()
		 WHERE consumer_name = $1 AND active = true`,
		name)
	if err != nil {
		return false, fmt.Errorf("deregister consumer %q: %w", name, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RequiredSet returns the consumers whose sign-off height H must wait on.
//
// Phase B: required_set == the set of currently-active consumers; the height
// argument is accepted but not yet used. Phase F adds semver-gated membership
// (FirstValidVersion vs network.genesis_decoder_version / upgrades), at which
// point height becomes significant.
func (s *Store) RequiredSet(ctx context.Context, _ int64) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT consumer_name FROM consumer_registry WHERE active = true ORDER BY consumer_name`)
	if err != nil {
		return nil, fmt.Errorf("query required set: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan consumer name: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}
```

- [ ] **Step 4: Run the tests; verify they pass**

Run: `make test-integration`
Expected: `TestSelfRegistrationIdempotent` and `TestDeregisterFlipsActive` PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/consumer_registry.go test/integration/registry_test.go test/integration/helpers_test.go
git commit -m "feat(store): consumer_registry self-registration + decommission + required set"
```

---

## Task 10: Store — processed_heights, contiguous consolidation, and the ack-after-commit transaction

The heart of the runtime's correctness. `ProcessHeight` runs the atomic transaction; `advanceConsolidation` computes the new contiguous high-water mark with a single window-function query. Tests cover **spec test 1** (contiguous advance), **test 10** (out-of-order: cursor frozen behind the gap), the freeze half of **test 2**, and duplicate-height idempotency (the DB half of **test 11**).

**Files:**
- Create: `internal/store/processed_heights.go`, `internal/store/consumer_consolidation.go`, `internal/store/process.go`
- Create test: `test/integration/cursor_test.go`

- [ ] **Step 1: Write the failing integration test**

`test/integration/cursor_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// noWrite is a handler body that writes no data rows (NoOp-equivalent).
func noWrite(_ context.Context, _ pgx.Tx) error { return nil }

func TestCursorAdvancesContiguously(t *testing.T) { // spec test 1
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for h := int64(1); h <= 5; h++ {
		cur, err := s.ProcessHeight(ctx, "noop-a", h, noWrite)
		if err != nil {
			t.Fatalf("ProcessHeight(%d): %v", h, err)
		}
		if cur != h {
			t.Fatalf("after processing %d, cursor = %d, want %d", h, cur, h)
		}
	}
}

func TestOutOfOrderFreezesAtGap(t *testing.T) { // spec test 10 + freeze half of test 2
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// Process 1,2 then jump to 4 (gap at 3). Cursor must stay at 2.
	for _, h := range []int64{1, 2} {
		if _, err := s.ProcessHeight(ctx, "noop-a", h, noWrite); err != nil {
			t.Fatal(err)
		}
	}
	cur, err := s.ProcessHeight(ctx, "noop-a", 4, noWrite)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 2 {
		t.Fatalf("cursor after out-of-order 4 = %d, want 2 (gap at 3)", cur)
	}
	// processed_heights still recorded 4.
	if ok, _ := s.HasProcessed(ctx, "noop-a", 4); !ok {
		t.Fatal("height 4 should be recorded in processed_heights despite the gap")
	}
	// Now 3 arrives — cursor jumps to 4.
	cur, err = s.ProcessHeight(ctx, "noop-a", 3, noWrite)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 4 {
		t.Fatalf("cursor after gap fill = %d, want 4", cur)
	}
}

func TestDuplicateHeightIdempotent(t *testing.T) { // DB half of spec test 11
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for i := 0; i < 2; i++ {
		if _, err := s.ProcessHeight(ctx, "noop-a", 7, noWrite); err != nil {
			t.Fatalf("ProcessHeight dup #%d: %v", i, err)
		}
	}
	var n int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=7`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed_heights rows for (noop-a,7) = %d, want 1", n)
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `make test-integration`
Expected: FAIL — `undefined: (*store.Store).ProcessHeight` / `HasProcessed`.

- [ ] **Step 3: Implement processed_heights helpers**

`internal/store/processed_heights.go`:
```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// insertProcessedHeight records that consumer processed height, idempotently.
// Runs inside the caller's transaction.
func insertProcessedHeight(ctx context.Context, tx pgx.Tx, consumer string, height int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO processed_heights (consumer_name, height)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name, height) DO NOTHING`,
		consumer, height)
	if err != nil {
		return fmt.Errorf("insert processed height (%s,%d): %w", consumer, height, err)
	}
	return nil
}

// HasProcessed reports whether consumer has a processed_heights row for height.
func (s *Store) HasProcessed(ctx context.Context, consumer string, height int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_heights WHERE consumer_name=$1 AND height=$2)`,
		consumer, height).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has processed (%s,%d): %w", consumer, height, err)
	}
	return exists, nil
}
```

- [ ] **Step 4: Implement consolidation read + contiguous advance**

`internal/store/consumer_consolidation.go`:
```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ConsolidatedUpTo returns the consumer's contiguous high-water mark, or 0 if
// it has never consolidated.
func (s *Store) ConsolidatedUpTo(ctx context.Context, consumer string) (int64, error) {
	var cur int64
	err := s.pool.QueryRow(ctx,
		`SELECT consolidated_up_to FROM consumer_consolidation WHERE consumer_name=$1`,
		consumer).Scan(&cur)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read consolidation %q: %w", consumer, err)
	}
	return cur, nil
}

// readConsolidationTx reads the cursor inside an open transaction.
func readConsolidationTx(ctx context.Context, tx pgx.Tx, consumer string) (int64, error) {
	var cur int64
	err := tx.QueryRow(ctx,
		`SELECT consolidated_up_to FROM consumer_consolidation WHERE consumer_name=$1`,
		consumer).Scan(&cur)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read consolidation (tx) %q: %w", consumer, err)
	}
	return cur, nil
}

// advanceConsolidation computes the new contiguous high-water mark for consumer
// starting from current, then upserts consumer_consolidation. The window query
// returns the largest H such that every height in (current, H] is present in
// processed_heights (i.e. the unbroken run starting at current+1). If the very
// next height is missing, it returns current unchanged. Commutative: the result
// depends only on the set of processed heights, not arrival order.
func advanceConsolidation(ctx context.Context, tx pgx.Tx, consumer string, current int64) (int64, error) {
	var next int64
	err := tx.QueryRow(ctx,
		`WITH run AS (
		     SELECT height, ROW_NUMBER() OVER (ORDER BY height) AS rn
		     FROM processed_heights
		     WHERE consumer_name = $1 AND height > $2
		 )
		 SELECT COALESCE(MAX(height), $2) FROM run WHERE height = $2 + rn`,
		consumer, current).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("compute consolidation for %q: %w", consumer, err)
	}

	// GREATEST keeps the cursor monotonic even if two writers ever race (the
	// ADR-007 multi-instance case) — consolidated_up_to can never regress.
	if _, err := tx.Exec(ctx,
		`INSERT INTO consumer_consolidation (consumer_name, consolidated_up_to, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (consumer_name) DO UPDATE
		 SET consolidated_up_to = GREATEST(consumer_consolidation.consolidated_up_to, EXCLUDED.consolidated_up_to),
		     updated_at = now()`,
		consumer, next); err != nil {
		return 0, fmt.Errorf("upsert consolidation for %q: %w", consumer, err)
	}
	return next, nil
}
```

- [ ] **Step 5: Implement the ack-after-commit transaction**

`internal/store/process.go`:
```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ProcessHeight runs the ack-after-commit transaction body for one message
// (invariant #5): BEGIN → write(tx) → INSERT processed_heights → advance
// consolidation → COMMIT. It returns the consumer's new consolidated_up_to.
//
// write performs the handler's data inserts within the same transaction; pass a
// no-op for NoOp consumers. The caller acks the NATS message only after this
// returns nil.
func (s *Store) ProcessHeight(
	ctx context.Context,
	consumer string,
	height int64,
	write func(ctx context.Context, tx pgx.Tx) error,
) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if err := write(ctx, tx); err != nil {
		return 0, fmt.Errorf("handler write at height %d: %w", height, err)
	}
	if err := insertProcessedHeight(ctx, tx, consumer, height); err != nil {
		return 0, err
	}
	current, err := readConsolidationTx(ctx, tx, consumer)
	if err != nil {
		return 0, err
	}
	next, err := advanceConsolidation(ctx, tx, consumer, current)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit at height %d: %w", height, err)
	}
	return next, nil
}
```

- [ ] **Step 6: Run the tests; verify they pass**

Run: `make test-integration`
Expected: `TestCursorAdvancesContiguously`, `TestOutOfOrderFreezesAtGap`, `TestDuplicateHeightIdempotent` PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/store/processed_heights.go internal/store/consumer_consolidation.go internal/store/process.go test/integration/cursor_test.go
git commit -m "feat(store): ack-after-commit ProcessHeight with contiguous consolidation"
```

---

## Task 11: Store — per-height seal (derived `is_sealed(H)`)

The AND-seal predicate over the active set × consolidation cursors. Tests cover **spec test 7** (one consumer) and **test 8** (two consumers, sealed only when both cross H). Driven at the store level by upserting consolidation directly, so the predicate is verified in isolation before the full runtime exists.

**Files:**
- Create: `internal/store/seal.go`
- Create test: `test/integration/seal_test.go`

- [ ] **Step 1: Write the failing integration test**

`test/integration/seal_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"
)

// setConsolidation upserts a consumer's cursor directly (test scaffolding).
func setConsolidation(t *testing.T, name string, upTo int64) {
	t.Helper()
	_, err := pg.Pool.Exec(context.Background(),
		`INSERT INTO consumer_consolidation (consumer_name, consolidated_up_to, updated_at)
		 VALUES ($1,$2, now())
		 ON CONFLICT (consumer_name) DO UPDATE SET consolidated_up_to = EXCLUDED.consolidated_up_to`,
		name, upTo)
	if err != nil {
		t.Fatalf("set consolidation: %v", err)
	}
}

func TestSealOneConsumer(t *testing.T) { // spec test 7
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")

	setConsolidation(t, "noop-a", 4)
	assertSealed(t, s, 4, true)
	assertSealed(t, s, 5, false)

	setConsolidation(t, "noop-a", 5)
	assertSealed(t, s, 5, true)
}

func TestSealTwoConsumersAND(t *testing.T) { // spec test 8
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")
	_ = s.RegisterConsumer(ctx, "noop-b", "v0.1.0")

	setConsolidation(t, "noop-a", 10)
	setConsolidation(t, "noop-b", 7)
	// H=8: a crossed it, b has not → NOT sealed.
	assertSealed(t, s, 8, false)
	// H=7: both crossed → sealed.
	assertSealed(t, s, 7, true)

	// b catches up to 10 → H=8..10 now sealed.
	setConsolidation(t, "noop-b", 10)
	assertSealed(t, s, 10, true)
}

func assertSealed(t *testing.T, s interface {
	IsSealed(context.Context, int64) (bool, error)
}, h int64, want bool) {
	t.Helper()
	got, err := s.IsSealed(context.Background(), h)
	if err != nil {
		t.Fatalf("IsSealed(%d): %v", h, err)
	}
	if got != want {
		t.Fatalf("IsSealed(%d) = %v, want %v", h, got, want)
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `make test-integration`
Expected: FAIL — `undefined: (*store.Store).IsSealed`.

- [ ] **Step 3: Implement the seal predicate**

`internal/store/seal.go`:
```go
package store

import (
	"context"
	"fmt"
)

// IsSealed reports whether height is sealed: every consumer in the required set
// (Phase B: all active consumers) has consolidated_up_to >= height, and the
// required set is non-empty. Derived at query time — no materialized seal row
// in Slice 1 (a materialized block_seal is deferred to Slice 2).
func (s *Store) IsSealed(ctx context.Context, height int64) (bool, error) {
	var sealed bool
	err := s.pool.QueryRow(ctx,
		`SELECT
		     count(*) FILTER (WHERE r.active) > 0
		     AND count(*) FILTER (WHERE r.active AND COALESCE(c.consolidated_up_to, 0) < $1) = 0
		 FROM consumer_registry r
		 LEFT JOIN consumer_consolidation c ON c.consumer_name = r.consumer_name`,
		height).Scan(&sealed)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	return sealed, nil
}
```

- [ ] **Step 4: Run the tests; verify they pass**

Run: `make test-integration`
Expected: `TestSealOneConsumer`, `TestSealTwoConsumersAND` PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/seal.go test/integration/seal_test.go
git commit -m "feat(store): derived per-height is_sealed over active required set"
```

---

## Task 12: Consumer runtime — interface, NoOpHandler, and the processing loop

The generic runtime that ties NATS → `store.ProcessHeight` → ack together, with self-registration, passive gap detection, and Nak-on-error (so a transient Postgres/handler failure redelivers rather than skips). `NoOpHandler` writes no data rows — the runtime's coordination writes are the only DB effect.

> **Deviation from the spec's literal interface (documented):** spec Section 4.6 sketches `Handle(msg *NATSMessage, decoder Decoder) error`. Phase B has no decoder (no chain code), so the Phase B `Handler.Handle(ctx, tx, msg)` omits the `decoder` parameter and adds the transaction handle the handler must write within (ADR-016). Phase D introduces `Decoder` and threads it through. This keeps Phase B free of an unimplemented `Decoder` stub.

**Files:**
- Create: `internal/consumer/types.go`, `internal/consumer/noop.go`, `internal/consumer/runtime.go`

This task has no standalone unit test — the runtime is meaningfully testable only end-to-end through NATS + Postgres, which Tasks 14–15 do. Verification here is compilation. (The TDD red/green for the runtime's behavior lives in Tasks 14–15, written test-first against this code.)

- [ ] **Step 1: Implement the consumer types**

`internal/consumer/types.go`:
```go
package consumer // package comment lives in internal/consumer/doc.go (do not repeat it — revive package-comments)

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Message is the runtime's view of one block-level NATS message.
type Message struct {
	Height  int64
	Subject string
	MsgID   string
	Data    []byte
}

// Handler is the per-module business logic invoked inside the ack-after-commit
// transaction. In Phase B the only implementation is NoOpHandler; real handlers
// (block, supplier, …) arrive in Phase D+.
type Handler interface {
	// ID is the stable consumer name (also the consumer_registry PK and the
	// JetStream durable name).
	ID() string
	// FirstValidVersion is the semver tag at which this consumer becomes
	// applicable (e.g. "v0.1.0"). Stored in consumer_registry; height-gating of
	// the required set is deferred to Phase F.
	FirstValidVersion() string
	// Handle writes this consumer's data rows for msg within tx. It MUST NOT
	// commit or roll back — the runtime owns the transaction.
	Handle(ctx context.Context, tx pgx.Tx, msg Message) error
}
```

- [ ] **Step 2: Implement NoOpHandler**

`internal/consumer/noop.go`:
```go
package consumer

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// NoOpHandler is a consumer that records progress (processed_heights +
// consolidation via the runtime) but writes no data rows. Used to validate the
// orchestration and AND-seal logic without any chain decoding.
type NoOpHandler struct {
	id                string
	firstValidVersion string
}

// NewNoOpHandler builds a NoOpHandler with the given id and first-valid version.
func NewNoOpHandler(id, firstValidVersion string) NoOpHandler {
	return NoOpHandler{id: id, firstValidVersion: firstValidVersion}
}

// ID returns the consumer's stable name.
func (h NoOpHandler) ID() string { return h.id }

// FirstValidVersion returns the semver tag recorded in consumer_registry.
func (h NoOpHandler) FirstValidVersion() string { return h.firstValidVersion }

// Handle writes nothing.
func (h NoOpHandler) Handle(_ context.Context, _ pgx.Tx, _ Message) error { return nil }
```

- [ ] **Step 3: Implement the runtime loop**

`internal/consumer/runtime.go`:
```go
package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// reprocessDelay is how long a Nak'd message waits before redelivery, so a
// transient Postgres outage does not spin the loop.
const reprocessDelay = 500 * time.Millisecond

// Runtime drives one consumer: subscribe → ack-after-commit → repeat.
type Runtime struct {
	handler  Handler
	store    *store.Store
	consumer jetstream.Consumer
	logger   *slog.Logger
	metrics  *metrics.Consumer
}

// Config wires a Runtime's collaborators.
type Config struct {
	Handler  Handler
	Store    *store.Store
	Consumer jetstream.Consumer
	Logger   *slog.Logger
	Metrics  *metrics.Consumer
}

// NewRuntime constructs a Runtime.
func NewRuntime(cfg Config) *Runtime {
	return &Runtime{
		handler:  cfg.Handler,
		store:    cfg.Store,
		consumer: cfg.Consumer,
		logger:   cfg.Logger,
		metrics:  cfg.Metrics,
	}
}

// reconnectDelay is the backoff between attempts to re-establish the message
// iterator after a NATS disconnect.
const reconnectDelay = 500 * time.Millisecond

// Run self-registers the consumer, then processes messages until ctx is
// canceled, transparently re-establishing the subscription across NATS
// disconnects. It returns ctx.Err() on clean shutdown.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.store.RegisterConsumer(ctx, r.handler.ID(), r.handler.FirstValidVersion()); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.consume(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Recoverable (NATS disconnect / iterator closed by the server):
			// back off, then re-establish the subscription.
			r.logger.Warn("consume interrupted; reconnecting", "consumer", r.handler.ID(), "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectDelay):
			}
			continue
		}
		return nil // consume returns nil only on clean ctx cancellation
	}
}

// consume creates a message iterator and processes messages until ctx is
// canceled (returns nil) or the iterator fails (returns the error so Run can
// reconnect). Each message follows the ack-after-commit protocol.
func (r *Runtime) consume(ctx context.Context) error {
	iter, err := r.consumer.Messages()
	if err != nil {
		return err
	}
	defer iter.Stop()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			iter.Stop() // unblocks iter.Next()
		case <-done:
		}
	}()
	defer close(done)

	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return err // disconnect / closed by server → Run reconnects
		}
		if herr := r.handle(ctx, msg); herr != nil {
			r.logger.Error("process failed; will redeliver", "consumer", r.handler.ID(), "err", herr)
			_ = msg.NakWithDelay(reprocessDelay)
			continue
		}
		_ = msg.Ack() // ack only AFTER a successful commit (invariant #5)
	}
}

func (r *Runtime) handle(ctx context.Context, msg jetstream.Msg) error {
	id := r.handler.ID()
	height, err := natsx.HeightFromBlockSubject(msg.Subject())
	if err != nil {
		_ = msg.Term() // unparseable subject: never redeliver
		r.logger.Error("bad subject; terminating", "consumer", id, "subject", msg.Subject())
		return nil
	}
	m := Message{
		Height:  height,
		Subject: msg.Subject(),
		MsgID:   natsx.MsgID(msg.Subject(), height, 0),
		Data:    msg.Data(),
	}

	next, err := r.store.ProcessHeight(ctx, id, height, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.Handle(ctx, tx, m)
	})
	if err != nil {
		return err
	}

	r.metrics.Processed.WithLabelValues(id).Inc()
	r.metrics.Consolidated.WithLabelValues(id).Set(float64(next))
	if next < height { // a gap sits between the contiguous frontier and this height
		r.metrics.GapsTotal.WithLabelValues(id).Inc()
		r.logger.Warn("gap detected", "consumer", id, "from", next+1, "to", height-1, "processed", height)
	}
	return nil
}
```

- [ ] **Step 4: Verify the package compiles**

Run: `go build ./...`
Expected: clean build. `make ci` green (no new unit tests; runtime behavior is covered in Tasks 14–15).

- [ ] **Step 5: Commit**

```bash
git add internal/consumer/types.go internal/consumer/noop.go internal/consumer/runtime.go
git commit -m "feat(consumer): generic runtime, NoOpHandler, ack-after-commit loop"
```

---

## Task 13: Synthetic fixtures

The Go equivalent of the spec's "bash-generated `block-{H}-meta`/`block-{H}-data` with marker bytes" — deterministic marker payloads plus a file generator that mirrors the FilePlugin output layout (so Phase D can reuse the path). Integration publishers use the in-memory `MarkerData` directly.

**Files:**
- Create: `test/fixtures/synthetic/synthetic.go`, `test/fixtures/synthetic/synthetic_test.go`

- [ ] **Step 1: Write the failing test**

`test/fixtures/synthetic/synthetic_test.go`:
```go
package synthetic

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestMarkerDataDeterministicAndHeightTagged(t *testing.T) {
	a := MarkerData(635505)
	b := MarkerData(635505)
	if !bytes.Equal(a, b) {
		t.Fatal("MarkerData not deterministic")
	}
	if bytes.Equal(MarkerData(1), MarkerData(2)) {
		t.Fatal("MarkerData should differ by height")
	}
	if !bytes.HasPrefix(a, []byte("PSCRIBE-DATA")) {
		t.Fatalf("MarkerData missing marker prefix: %q", a)
	}
}

func TestGenerateWritesFiles(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, 1, 3); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	for h := 1; h <= 3; h++ {
		for _, suffix := range []string{"meta", "data"} {
			p := filepath.Join(dir, // block-<h>-<suffix>
				fileName(int64(h), suffix))
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected file %s: %v", p, err)
			}
		}
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `go test ./test/fixtures/synthetic/...`
Expected: FAIL — undefined `MarkerData` / `Generate` / `fileName`.

- [ ] **Step 3: Implement the synthetic package**

`test/fixtures/synthetic/synthetic.go`:
```go
// Package synthetic generates marker-byte block fixtures for Phase B
// orchestration tests — no real proto. The on-disk layout mirrors FilePlugin
// output (block-<H>-meta / block-<H>-data) so Phase D can reuse the path.
package synthetic

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

var (
	dataMarker = []byte("PSCRIBE-DATA\x00")
	metaMarker = []byte("PSCRIBE-META\x00")
)

// MarkerData returns the deterministic data payload for height h.
func MarkerData(h int64) []byte { return marker(dataMarker, h) }

// MarkerMeta returns the deterministic meta payload for height h.
func MarkerMeta(h int64) []byte { return marker(metaMarker, h) }

func marker(prefix []byte, h int64) []byte {
	b := make([]byte, len(prefix)+8)
	copy(b, prefix)
	binary.BigEndian.PutUint64(b[len(prefix):], uint64(h))
	return b
}

func fileName(h int64, suffix string) string {
	return fmt.Sprintf("block-%d-%s", h, suffix)
}

// Generate writes block-<H>-meta and block-<H>-data marker files into dir for
// heights lo..hi inclusive.
func Generate(dir string, lo, hi int64) error {
	for h := lo; h <= hi; h++ {
		if err := os.WriteFile(filepath.Join(dir, fileName(h, "meta")), MarkerMeta(h), 0o644); err != nil {
			return fmt.Errorf("write meta %d: %w", h, err)
		}
		if err := os.WriteFile(filepath.Join(dir, fileName(h, "data")), MarkerData(h), 0o644); err != nil {
			return fmt.Errorf("write data %d: %w", h, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run the test; verify it passes**

Run: `go test ./test/fixtures/synthetic/...`
Expected: PASS. `make ci` green.

- [ ] **Step 5: Commit**

```bash
git add test/fixtures/synthetic
git commit -m "test(fixtures): synthetic marker-byte block fixtures"
```

---

## Task 14: Full-loop integration tests (spec tests 1, 2, 7, 8, 10, 11, 3)

Drives the real runtime through real NATS JetStream + Postgres. This task delivers the **authoritative** end-to-end versions of: test 1 (contiguous), test 2 (forced gap + gap recorded), test 10 (out-of-order delivery), test 11 (duplicate `Nats-Msg-Id` dedup), tests 7/8 (per-height seal, 1 and 2 consumers), and test 3 (kill + restart resumes, no duplicates).

**Files:**
- Create: `test/integration/nats_helpers_test.go`, `test/integration/runtime_test.go`

- [ ] **Step 1: Write the NATS/runtime test helpers**

`test/integration/nats_helpers_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/test/fixtures/synthetic"
)

const dedupeWindow = 2 * time.Minute

// freshStream ensures the POKT stream exists and purges it so each test starts
// from an empty stream (the NATS server is shared across the package).
func freshStream(t *testing.T) jetstream.Stream {
	t.Helper()
	ctx := context.Background()
	stream, err := nats.Client.EnsureStream(ctx, dedupeWindow)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	if err := stream.Purge(ctx); err != nil {
		t.Fatalf("purge stream: %v", err)
	}
	return stream
}

// publishHeightsTo publishes one synthetic block message per height to js, each
// with a deterministic Nats-Msg-Id.
func publishHeightsTo(t *testing.T, js jetstream.JetStream, heights ...int64) {
	t.Helper()
	ctx := context.Background()
	for _, h := range heights {
		subj := natsx.BlockSubject(h)
		if _, err := js.Publish(ctx, subj, synthetic.MarkerData(h),
			jetstream.WithMsgID(natsx.MsgID(subj, h, 0))); err != nil {
			t.Fatalf("publish height %d: %v", h, err)
		}
	}
}

// publishHeights publishes to the shared NATS server.
func publishHeights(t *testing.T, heights ...int64) {
	t.Helper()
	publishHeightsTo(t, nats.Client.JetStream(), heights...)
}

// publishHeightTwice publishes height h twice with the SAME Nats-Msg-Id (shared
// server) to exercise server-side dedup.
func publishHeightTwice(t *testing.T, h int64) {
	t.Helper()
	ctx := context.Background()
	js := nats.Client.JetStream()
	subj := natsx.BlockSubject(h)
	id := natsx.MsgID(subj, h, 0)
	for i := 0; i < 2; i++ {
		if _, err := js.Publish(ctx, subj, synthetic.MarkerData(h), jetstream.WithMsgID(id)); err != nil {
			t.Fatalf("publish dup %d: %v", h, err)
		}
	}
}

// waitConnected blocks until the client's NATS connection is (re-)established or
// the timeout elapses.
func waitConnected(t *testing.T, c *natsx.Client, timeout time.Duration) {
	t.Helper()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		if c.Conn().IsConnected() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("nats client did not reconnect within timeout")
		case <-tick.C:
		}
	}
}

// durableConsumer creates/updates a durable pull consumer named `name` filtered
// to block subjects with the given AckWait. It is deleted on cleanup.
func durableConsumer(t *testing.T, stream jetstream.Stream, name string, ackWait time.Duration) jetstream.Consumer {
	t.Helper()
	cons, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: natsx.BlockSubjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    -1,
	})
	if err != nil {
		t.Fatalf("create durable consumer %s: %v", name, err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(context.Background(), name) })
	return cons
}

// runtimeHandle bundles a running runtime with what tests need to inspect/stop it.
type runtimeHandle struct {
	name    string
	store   *store.Store
	metrics *metrics.Consumer
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

// startRuntime builds a store + NoOp runtime named `name`, binds it to a durable
// on stream, and runs it in a goroutine. Stopped automatically on cleanup.
func startRuntime(t *testing.T, stream jetstream.Stream, name string) *runtimeHandle {
	t.Helper()
	s, err := store.New(context.Background(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	cons := durableConsumer(t, stream, name, 2*time.Second)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := consumer.NewRuntime(consumer.Config{
		Handler:  consumer.NewNoOpHandler(name, "v0.1.0"),
		Store:    s,
		Consumer: cons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h := &runtimeHandle{name: name, store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		_ = rt.Run(ctx)
	}()
	t.Cleanup(h.stop)
	return h
}

// stop cancels the runtime, waits for it to exit, and closes its store. Idempotent.
func (h *runtimeHandle) stop() {
	h.once.Do(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
		}
		h.store.Close()
	})
}

// waitCursor polls a consumer's consolidated_up_to until it reaches want or the
// timeout elapses.
func waitCursor(t *testing.T, s *store.Store, name string, want int64, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	var last int64
	for {
		if cur, err := s.ConsolidatedUpTo(ctx, name); err == nil {
			last = cur
			if cur >= want {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("waitCursor(%s): reached %d, want >= %d within %s", name, last, want, timeout)
		case <-tick.C:
		}
	}
}

// processedCount returns how many processed_heights rows a consumer has.
func processedCount(t *testing.T, name string) int {
	t.Helper()
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name=$1`, name).Scan(&n); err != nil {
		t.Fatalf("processedCount(%s): %v", name, err)
	}
	return n
}
```

> `waitCursor` uses `time.NewTicker`/`time.After` (not `time.Now()`), so it is `forbidigo`-safe even if the integration build tag is ever linted. (By default `golangci-lint run ./...` does not analyze `//go:build integration` files at all.)

- [ ] **Step 2: Write the runtime scenario tests**

`test/integration/runtime_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestE2EContiguousAdvance(t *testing.T) { // spec test 1 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")
	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, h.store, "noop-a", 5, 15*time.Second)
	if got := processedCount(t, "noop-a"); got != 5 {
		t.Fatalf("processed rows = %d, want 5", got)
	}
}

func TestE2EForcedGapRecordedThenFilled(t *testing.T) { // spec tests 2 + 10 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")

	// 1,2 then 4 — height 4 arrives while 3 is missing (out-of-order vs the gap).
	publishHeights(t, 1, 2, 4)
	waitCursor(t, h.store, "noop-a", 2, 15*time.Second)

	// Give the runtime time to process the out-of-order 4 and freeze.
	time.Sleep(750 * time.Millisecond)
	if cur, _ := h.store.ConsolidatedUpTo(context.Background(), "noop-a"); cur != 2 {
		t.Fatalf("cursor = %d, want frozen at 2 (gap at 3)", cur)
	}
	if got := testutil.ToFloat64(h.metrics.GapsTotal.WithLabelValues("noop-a")); got < 1 {
		t.Fatalf("expected a recorded gap, gaps_total = %v", got)
	}
	if ok, _ := h.store.HasProcessed(context.Background(), "noop-a", 4); !ok {
		t.Fatal("height 4 should be recorded despite the gap")
	}

	// Fill the gap → cursor jumps to 4.
	publishHeights(t, 3)
	waitCursor(t, h.store, "noop-a", 4, 15*time.Second)
}

func TestE2EDuplicateMsgIDNoDuplicateRow(t *testing.T) { // spec test 11 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")

	publishHeightTwice(t, 1)
	waitCursor(t, h.store, "noop-a", 1, 15*time.Second)

	// Allow any (improbable) second delivery to land before asserting.
	time.Sleep(500 * time.Millisecond)
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed_heights rows for (noop-a,1) = %d, want 1", n)
	}
}

func TestE2EPerHeightSealOneAndTwoConsumers(t *testing.T) { // spec tests 7 + 8 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	a := startRuntime(t, stream, "noop-a")
	b := startRuntime(t, stream, "noop-b")

	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, a.store, "noop-a", 5, 15*time.Second)
	waitCursor(t, b.store, "noop-b", 5, 15*time.Second)

	// Both active consumers crossed 5 → sealed.
	assertSealed(t, a.store, 5, true)

	// Introduce a third REQUIRED consumer that never processes (cursor stays 0).
	// The required set now includes it, so H=5 is no longer sealed (AND-gating).
	if err := a.store.RegisterConsumer(context.Background(), "noop-c", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	assertSealed(t, a.store, 5, false)
}

func TestE2EKillAndRestartResumes(t *testing.T) { // spec test 3 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)

	h1 := startRuntime(t, stream, "noop-a")
	publishHeights(t, 1, 2, 3)
	waitCursor(t, h1.store, "noop-a", 3, 15*time.Second)
	h1.stop() // kill

	publishHeights(t, 4, 5, 6)

	h2 := startRuntime(t, stream, "noop-a") // same durable + consumer name
	waitCursor(t, h2.store, "noop-a", 6, 15*time.Second)

	if got := processedCount(t, "noop-a"); got != 6 {
		t.Fatalf("processed rows after restart = %d, want 6 (no duplicates, no skips)", got)
	}
}
```

- [ ] **Step 3: Run the suite**

Run: `make test-integration`
Expected: all five tests PASS (alongside the earlier integration tests). If `TestE2EForcedGapRecordedThenFilled` is flaky on the 750ms settle, increase it — the assertion is that the cursor never passes 2 while 3 is absent; the sleep only needs to exceed one process cycle.

- [ ] **Step 4: Commit**

```bash
git add test/integration/nats_helpers_test.go test/integration/runtime_test.go
git commit -m "test(integration): end-to-end runtime — contiguity, gaps, dedup, sealing, restart"
```

---

## Task 15: Resilience integration tests (spec tests 4, 5, 6, 12)

The crash/restart/outage scenarios: ack-after-commit under redelivery (4), a real NATS server bounce with the runtime auto-reconnecting (5), Postgres restart with the consumer waiting and recovering (6), and simultaneous multi-consumer crash with independent recovery and seal restoration (12). Tests 5 and 6 use dedicated fixed-port containers so a stop/start preserves the address; the rest use the shared harness.

**Files:**
- Create: `test/integration/resilience_test.go`

- [ ] **Step 1: Write the resilience tests**

`test/integration/resilience_test.go`:
```go
//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	tc "github.com/pokt-network/pocketscribe/test/testcontainers"
)

func TestAckAfterCommitRedelivery(t *testing.T) { // spec test 4
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	stream := freshStream(t)
	cons := durableConsumer(t, stream, "noop-a", 1*time.Second)

	publishHeights(t, 1)

	// First delivery: commit, then "crash" (do NOT ack).
	batch, err := cons.Fetch(1)
	if err != nil {
		t.Fatalf("fetch #1: %v", err)
	}
	got := 0
	for msg := range batch.Messages() {
		got++
		if _, err := s.ProcessHeight(ctx, "noop-a", 1, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process #1: %v", err)
		}
		// no Ack — simulate crash after commit, before ack
	}
	if got != 1 {
		t.Fatalf("expected 1 message on first fetch, got %d", got)
	}

	// Wait past AckWait so the server redelivers.
	time.Sleep(1500 * time.Millisecond)

	// Redelivery: process again (idempotent) and ack.
	batch2, err := cons.Fetch(1)
	if err != nil {
		t.Fatalf("fetch #2: %v", err)
	}
	redelivered := 0
	for msg := range batch2.Messages() {
		redelivered++
		if _, err := s.ProcessHeight(ctx, "noop-a", 1, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process #2: %v", err)
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if redelivered != 1 {
		t.Fatalf("expected redelivery of the un-acked message, got %d", redelivered)
	}

	// Exactly one row: no duplicate, no skip.
	if c := processedCount(t, "noop-a"); c != 1 {
		t.Fatalf("processed rows = %d, want 1 (ack-after-commit holds)", c)
	}
	if cur, _ := s.ConsolidatedUpTo(ctx, "noop-a"); cur != 1 {
		t.Fatalf("cursor = %d, want 1", cur)
	}
}

func TestNatsRestartConsumerReconnects(t *testing.T) { // spec test 5
	ctx := context.Background()
	pg.Reset(t)

	// Dedicated fixed-port NATS so the same client reconnects after a bounce.
	nc := tc.NATSFixedPort(t, "14222")
	stream, err := nc.Client.EnsureStream(ctx, dedupeWindow)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	// One long-lived runtime bound to this server's durable.
	h := startRuntime(t, stream, "noop-a")
	publishHeightsTo(t, nc.Client.JetStream(), 1, 2, 3)
	waitCursor(t, h.store, "noop-a", 3, 20*time.Second)

	// Bounce the NATS server. A Stop+Start (unlike Terminate) keeps the
	// container filesystem, so the JetStream file storage — stream, durable, and
	// already-published messages — survives.
	stopTimeout := 20 * time.Second
	if err := nc.Container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop nats: %v", err)
	}
	if err := nc.Container.Start(ctx); err != nil {
		t.Fatalf("start nats: %v", err)
	}

	// The runtime's client auto-reconnects (MaxReconnects(-1)) and its reconnect
	// loop re-establishes the subscription. Messages published after the restart
	// are delivered and consolidated — no loss, no duplicates.
	waitConnected(t, nc.Client, 30*time.Second)
	publishHeightsTo(t, nc.Client.JetStream(), 4, 5, 6)
	waitCursor(t, h.store, "noop-a", 6, 45*time.Second)
	if got := processedCount(t, "noop-a"); got != 6 {
		t.Fatalf("processed rows = %d, want 6 (no loss, no dup across NATS restart)", got)
	}
}

func TestPostgresRestartWaitsThenRecovers(t *testing.T) { // spec test 6
	ctx := context.Background()
	// Dedicated fixed-port container so the same pool reconnects after restart.
	fp := tc.PostgresFixedPort(t, "15432")
	s, err := newStoreFromDSN(t, fp.DSN)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	for _, h := range []int64{1, 2, 3} {
		if _, err := s.ProcessHeight(ctx, "noop-a", h, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process %d: %v", h, err)
		}
	}

	// Stop Postgres.
	stopTimeout := 20 * time.Second
	if err := fp.Container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}

	// While down, processing fails: the cursor cannot advance ("the consumer
	// waits"). We assert only the failure here — querying the cursor while
	// Postgres is down would itself error. The recovery assertion below
	// (cursor == 4) is what proves no advance happened during the outage.
	if _, err := s.ProcessHeight(ctx, "noop-a", 4, func(context.Context, pgx.Tx) error { return nil }); err == nil {
		t.Fatal("expected ProcessHeight to fail while Postgres is down")
	}

	// Bring Postgres back on the same port; the pool reconnects.
	if err := fp.Container.Start(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	// Retry until the pool re-establishes a connection.
	recovered := false
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for !recovered {
		if _, err := s.ProcessHeight(ctx, "noop-a", 4, func(context.Context, pgx.Tx) error { return nil }); err == nil {
			recovered = true
			break
		}
		select {
		case <-deadline:
			t.Fatal("Postgres did not recover within 30s")
		case <-tick.C:
		}
	}

	if cur, _ := s.ConsolidatedUpTo(ctx, "noop-a"); cur != 4 {
		t.Fatalf("cursor after recovery = %d, want 4", cur)
	}
	if c := processedCount4(t, s); c != 1 {
		t.Fatalf("processed rows for height 4 = %d, want 1 (no duplicate)", c)
	}
}

func TestMultipleConsumersCrashRecover(t *testing.T) { // spec test 12
	pg.Reset(t)
	stream := freshStream(t)

	a := startRuntime(t, stream, "noop-a")
	b := startRuntime(t, stream, "noop-b")
	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, a.store, "noop-a", 5, 15*time.Second)
	waitCursor(t, b.store, "noop-b", 5, 15*time.Second)
	assertSealed(t, a.store, 5, true)

	// Both crash simultaneously.
	a.stop()
	b.stop()

	publishHeights(t, 6, 7, 8)

	a2 := startRuntime(t, stream, "noop-a")
	b2 := startRuntime(t, stream, "noop-b")
	waitCursor(t, a2.store, "noop-a", 8, 15*time.Second)
	waitCursor(t, b2.store, "noop-b", 8, 15*time.Second)
	assertSealed(t, a2.store, 8, true)

	if got := processedCount(t, "noop-a"); got != 8 {
		t.Fatalf("noop-a processed rows = %d, want 8", got)
	}
	if got := processedCount(t, "noop-b"); got != 8 {
		t.Fatalf("noop-b processed rows = %d, want 8", got)
	}
}
```

Add two small helpers to `test/integration/helpers_test.go` (created in Task 9):
```go
import (
	"context"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// newStoreFromDSN builds a Store against an arbitrary DSN (used by the
// fixed-port Postgres-restart test).
func newStoreFromDSN(t *testing.T, dsn string) (*store.Store, error) {
	t.Helper()
	s, err := store.New(t.Context(), dsn)
	if err != nil {
		return nil, err
	}
	t.Cleanup(s.Close)
	return s, nil
}

// processedCount4 counts processed_heights rows at height 4 via the given store.
func processedCount4(t *testing.T, s *store.Store) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=4`).Scan(&n); err != nil {
		t.Fatalf("processedCount4: %v", err)
	}
	return n
}
```

> `TestPostgresRestartWaitsThenRecovers` does not call `pg.Reset` — it uses its own dedicated container, so it must not touch the shared pool's data. The `container.Stop` signature is `Stop(ctx, *time.Duration)`; `Start(ctx)`. If `Container.Stop`/`Start` are not exposed on `*postgres.PostgresContainer` directly, call them on the embedded container: `fp.Container.Container` is not a field — the module type embeds `testcontainers.Container`, so `fp.Container.Stop(...)`/`Start(...)` resolve through the embedding. The first compile confirms it.

- [ ] **Step 2: Run the suite**

Run: `make test-integration`
Expected: tests 4, 5, 6, 12 PASS. Test 6 is the slowest (a second container + stop/start). If `Stop`/`Start` do not resolve on the module type, adjust to `fp.Container` (it embeds `testcontainers.Container`).

- [ ] **Step 3: Commit**

```bash
git add test/integration/resilience_test.go test/integration/helpers_test.go
git commit -m "test(integration): resilience — ack-after-commit, disconnect, pg restart, multi-crash"
```

---

## Task 16: `ps deregister-consumer` + spec test 13 (decommission)

The decommission CLI and its end-to-end test: a deregistered consumer leaves the required set, so a height that was blocked on it now seals.

**Files:**
- Create: `internal/app/deregister/cmd.go`
- Modify: `internal/app/root.go`
- Create test: `test/integration/deregister_test.go`

- [ ] **Step 1: Write the failing integration test (exercises the real CLI)**

`test/integration/deregister_test.go`:
```go
//go:build integration

package integration

import (
	"bytes"
	"context"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/app"
)

func TestDeregisterConsumerCLIUnblocksSeal(t *testing.T) { // spec test 13
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// Two required consumers; one (noop-b) is behind, so H=5 is not sealed.
	if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterConsumer(ctx, "noop-b", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	setConsolidation(t, "noop-a", 5)
	setConsolidation(t, "noop-b", 2)
	assertSealed(t, s, 5, false)

	// Decommission noop-b via the real `ps deregister-consumer` command.
	root := app.NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"deregister-consumer", "noop-b", "--dsn", pg.DSN})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("deregister-consumer: %v (output: %s)", err, out.String())
	}

	// noop-b is no longer in the required set → H=5 now seals on noop-a alone.
	active, err := s.RequiredSet(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0] != "noop-a" {
		t.Fatalf("RequiredSet = %v, want [noop-a]", active)
	}
	assertSealed(t, s, 5, true)
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `make test-integration`
Expected: FAIL — `unknown command "deregister-consumer"`.

- [ ] **Step 3: Implement the subcommand**

`internal/app/deregister/cmd.go`:
```go
// Package deregister is the composition root for the ps deregister-consumer subcommand.
package deregister

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// defaultDSN matches the Tilt-managed dev Postgres (see Makefile DEV_PG_DSN).
const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds `ps deregister-consumer <name>`.
func NewCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "deregister-consumer <name>",
		Short: "Decommission a consumer: flip active=false and remove it from the required set",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			s, err := store.New(c.Context(), dsn)
			if err != nil {
				return err
			}
			defer s.Close()
			changed, err := s.DeregisterConsumer(c.Context(), args[0])
			if err != nil {
				return err
			}
			if changed {
				fmt.Fprintf(c.OutOrStdout(), "consumer %q deregistered\n", args[0])
			} else {
				fmt.Fprintf(c.OutOrStdout(), "consumer %q was not active; no change\n", args[0])
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

> The 3-line `envOr` + `defaultDSN` are intentionally duplicated from `internal/app/migrate` rather than shared — they are a dev-convenience default, not one of the DRY-protected single-sources (NATS subjects, metric names, config loading). If you prefer, extract them to a tiny `internal/store.DefaultDevDSN` constant and a shared helper; not required for Phase B.

Register it in `internal/app/root.go` — add the import and one `AddCommand`:
```go
import (
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/app/deregister"
	"github.com/pokt-network/pocketscribe/internal/app/migrate"
)
// inside NewRootCmd:
	root.AddCommand(migrate.NewCmd())
	root.AddCommand(deregister.NewCmd())
```

- [ ] **Step 4: Run the test; verify it passes**

Run: `make test-integration`
Expected: `TestDeregisterConsumerCLIUnblocksSeal` PASS. Also confirm the CLI surface: `go run ./cmd/ps deregister-consumer --help`.

- [ ] **Step 5: Commit**

```bash
git add internal/app/deregister/cmd.go internal/app/root.go test/integration/deregister_test.go
git commit -m "feat(cli): ps deregister-consumer + decommission seal test"
```

---

## Task 17: Finalize — tidy, full green suite, mark Phase B complete

Locks `go.mod`/`go.sum`, runs the complete gauntlet (unit + integration), and marks the spec phase done — mirroring how Phase A closed.

**Files:**
- Modify: `go.mod`, `go.sum`, `docs/superpowers/specs/2026-06-08-slice-1-design.md`

- [ ] **Step 1: Tidy the module**

Now that real imports exist and the broken cosmos pin is gone, `go mod tidy` succeeds and finalizes indirect requires + `go.sum`. Run with the integration tag so test-only deps (testcontainers, docker, testify) are retained:
```bash
go mod tidy
go build -tags=integration ./...
```
Expected: tidy completes; the require block now lists the direct deps actually imported (pgx, nats.go, goose, prometheus, cobra, viper, testify, testcontainers ×3, plus `github.com/docker/docker` and `github.com/docker/go-connections` pulled in by the fixed-port test). `go build -tags=integration ./...` is clean.

- [ ] **Step 2: Run the full gauntlet**

Run:
```bash
make ci
make test-integration
```
Expected: `make ci` (vet + fmt-check + lint + unit tests) green; `make test-integration` runs every `test/integration/*` test — all of spec tests 1–13 PASS. If any flake on timing, raise the specific `waitCursor`/sleep budget (never weaken an assertion).

Map of spec test → authoritative integration test (confirm each is present and green):

| Spec §11.1 | Test function | File |
|---|---|---|
| 1 | `TestE2EContiguousAdvance` | runtime_test.go |
| 2 | `TestE2EForcedGapRecordedThenFilled` | runtime_test.go |
| 3 | `TestE2EKillAndRestartResumes` | runtime_test.go |
| 4 | `TestAckAfterCommitRedelivery` | resilience_test.go |
| 5 | `TestNatsRestartConsumerReconnects` | resilience_test.go |
| 6 | `TestPostgresRestartWaitsThenRecovers` | resilience_test.go |
| 7 | `TestSealOneConsumer` + `TestE2EPerHeightSealOneAndTwoConsumers` | seal_test.go / runtime_test.go |
| 8 | `TestSealTwoConsumersAND` + `TestE2EPerHeightSealOneAndTwoConsumers` | seal_test.go / runtime_test.go |
| 9 | `TestSelfRegistrationIdempotent` | registry_test.go |
| 10 | `TestOutOfOrderFreezesAtGap` + `TestE2EForcedGapRecordedThenFilled` | cursor_test.go / runtime_test.go |
| 11 | `TestDuplicateHeightIdempotent` + `TestE2EDuplicateMsgIDNoDuplicateRow` | cursor_test.go / runtime_test.go |
| 12 | `TestMultipleConsumersCrashRecover` | resilience_test.go |
| 13 | `TestDeregisterConsumerCLIUnblocksSeal` | deregister_test.go |

- [ ] **Step 3: Commit the tidy result**

```bash
git add go.mod go.sum
git commit -m "chore(deps): go mod tidy after Phase B implementation"
```

- [ ] **Step 4: Mark Phase B complete in the spec**

Open `docs/superpowers/specs/2026-06-08-slice-1-design.md`, find the `### Phase B — Layer 0 orchestration skeleton` heading in Section 9, and add a status line directly under it, mirroring exactly how Phase A was marked complete (look at the Phase A heading for the format — it is something like `**Status:** ✅ Complete (<date>)` or a `> Completed …` note). Use today's date (2026-06-08). Do not invent a different format — match the Phase A marker verbatim in style.

- [ ] **Step 5: Commit the spec marker**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md
git commit -m "docs(spec): mark Slice 1 Phase B complete"
```

---

## Phase B exit checklist

Confirm every item before declaring Phase B done (mirrors the Phase A closeout):

- [ ] `go.mod` is tidy and builds with and without the `integration` tag; cosmos/proto deps are absent (return in Phase C).
- [ ] `ps version` prints `-ldflags`-injected metadata; `ps migrate up/down/status` and `ps deregister-consumer <name>` work.
- [ ] `schema/migrations/0039_consumer_registry.sql` applies cleanly via `make verify-migrations` (all 39 migrations green).
- [ ] `internal/config` loads `mainnet.yaml`, `beta.yaml`, `localnet.yaml` (incl. `genesis_time: dynamic`) and rejects an invalid file.
- [ ] `make ci` is green (vet + fmt-check + lint + unit tests; no containers).
- [ ] `make test-integration` is green: spec tests 1–13 all pass (see the map in Task 17 Step 2).
- [ ] No invariant violations: no `time.Now()`/`clock_timestamp()` as a query axis; append-only honored (only cursor/registry metadata is updated); all Postgres access via `internal/store`; NATS subjects only in `internal/nats/subjects.go`; metric names only in `internal/metrics/metrics.go`.
- [ ] Spec Section 9 Phase B marked complete.
- [ ] No `Co-Authored-By`/AI-attribution footer in any commit.

---

## Notes for the executor

- **Version-gating is deliberately deferred.** `RequiredSet(ctx, height)` ignores `height` and `FirstValidVersion` is stored but unused for sealing. This matches the spec build order (semver-gated `required_set(H)` is Phase F, tests 22–26). Do not implement it here, and do not add `golang.org/x/mod/semver` — Phase B needs no semver.
- **Gap escalation timers are out of scope.** Spec Section 6's `PS_GAP_WARN_AFTER` / `PS_GAP_ERROR_AFTER` time-based escalation belongs to Phase G hardening; introducing it here would require a wall-clock read. Phase B's passive gap detection = increment `gaps_total` + structured WARN log on detection (sufficient for test 2's "gap recorded").
- **`database/sql` appears only at the goose boundary** (`internal/store/migrate.go`), because goose requires `*sql.DB`. All query work uses pgx/pgxpool. This is not a violation of the "no `database/sql`" guideline, which targets data access.
- **Package comments live in exactly one file per package.** Packages with a Phase A `doc.go` stub (`config`, `consumer`, `log`, `metrics`, `nats`, `store`, `version`, and the `internal/app/*` roots) already own their package comment there — the new `.go` files in this plan deliberately use a bare `package X` clause (or a trailing line comment), so do NOT add a doc comment above the `package` keyword (`revive`'s `package-comments` rejects a second one). New packages with no stub — `schema`, `internal/app/deregister`, `test/fixtures/synthetic`, and the integration-tagged `test/testcontainers` — carry their package comment in exactly one file (as written).
- **Shared containers, per-test isolation.** The integration package boots one Postgres + one NATS via `TestMain`; tests call `pg.Reset(t)` (TRUNCATE) and `freshStream(t)` (Purge) for isolation. The Postgres-restart test is the one exception — it owns a dedicated fixed-port container.
- **If `make test-integration` cannot reach Docker**, the harness `TestMain` exits non-zero with a clear message; that is environmental, not a plan defect.
