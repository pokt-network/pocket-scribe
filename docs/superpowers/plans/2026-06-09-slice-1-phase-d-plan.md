# Slice 1 Phase D — Block Consumer + Sidecar + Router + sync-upgrades + 5 Real Fixtures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flow real chain data end-to-end through the orchestration layer — `ps sync-upgrades` populates the `upgrades` table from mainnet, a DB-driven `router` dispatches the correct decoder per height, `ps fileplugin --bootstrap` republishes captured block-meta bytes to NATS, and `ps consumer block` decodes the header and writes the `block` table — validated by spec §11.1 tests 14–17 against 5 real fixture versions.

**Architecture:** The sidecar is a dumb byte-forwarder: it reads `block-{H}-meta` files from a directory and publishes the **raw bytes verbatim** to `pokt.block.{H}` (height comes from the filename; no decoding, no router, no fan-out — only the block subject in Phase D). The block consumer (a Phase-B `consumer.Handler`) receives the raw meta bytes, calls `router.DecoderFor(H).DecodeBlockHeader(rawMeta)` (consumer-side, version-dispatched per ADR-008/ADR-018), and writes the `block` row inside the existing ack-after-commit transaction. The router maps height→decoder_version by integer comparison against the `upgrades` table (populated by `sync-upgrades` from the mainnet LCD), resolving the version string through an explicit `map[string]decoders.Decoder` registry; below the first upgrade it returns the network's `genesis_decoder_version`. The block header is version-invariant (cometbft ABCI `RequestFinalizeBlock`), so the 5 new version decoders are trivial delegating adapters with **no `gen/` codegen**.

**Tech Stack:** Go 1.26 · cobra (cmd pattern) · viper (config) · pgx/v5 (store) · nats.go/jetstream (Phase B client/runtime) · `github.com/cometbft/cometbft` fork (already in go.mod, used by the shared decoder) · stdlib `net/http` (LCD client, injectable for tests) · stdlib `testing` (NO testify) · testcontainers (Phase B harness) for integration tests · `rclone` (already configured: remote `pocketscribe-hetzner:`) for one-time fixture extraction.

**Spec reference:** `docs/superpowers/specs/2026-06-08-slice-1-design.md` §9 Phase D (lines 448–462), §4.1 (sidecar), §4.3 (router), §4.4 (sync-upgrades), §4.5 (reconciler upgrades-refresh), §4.7 (block consumer), §5.1 (startup order), §11.1 tests 14–17.

**ADR constraints honored:** ADR-003 (FilePlugin+sidecar; sidecar lifecycle-decoupled from decoders — raw bytes on the bus), ADR-005/006 (chain is truth; no derived state), ADR-007 (per-module consumer + ack-after-commit), ADR-008 (per-version decoders; new dir per version; block header version-invariant), ADR-010 (block carries the consensus (height, time) axis), ADR-016 (all Postgres via `internal/store`), ADR-018 (NO hardcoded upgrades — router is DB-driven; `internal/router/upgrades.go` stays a doc stub), ADR-022 (`pokt.block.{H}` is one block-level message per height; metadata only, no external storage refs).

**Key design decisions (resolved during grounding — all evidence-backed):**
- **Decode locus = consumer-side.** Sidecar publishes raw `block-{H}-meta` bytes on `pokt.block.{H}`; the consumer decodes via the router. The decoder lib + router are consumer-facing (ADR-008/ADR-018); raw-on-the-bus enables `ps replay` (design principle 3) and decouples sidecar deploys from decoder-version correctness (ADR-003).
- **NO `internal/proto` / envelope in Phase D.** The raw meta bytes deliver height+time+hash+tx_count for free (the `block` table needs exactly those). `chain_id`/`event_count` (the §4.1 wishlist) have no consumer/column in D — defer the structured envelope to Phase E (tx/event fan-out). YAGNI.
- **`chain_id` is NOT persisted** — the `block` table has no such column; one deployment indexes one chain. The block handler does not write it. No migration.
- **5 new decoder adapters, NO `gen/`.** v0_1_0/10/20/28/29 — the block header is version-invariant (verified: all 5 fixtures decode through the shared `decoders.DecodeBlockHeader`). Each is a ~27-line delegating adapter via `scripts/scaffold_decoder.sh` (Phase C). `gen/` codegen for these versions is deferred to Phase E/F (tx/state/event categories). The cosmos-sdk v0.50.13↔v0.53.0 split across these versions is irrelevant for headers.
- **No semver in the router.** Height→version is integer comparison; version string is a map key. `golang.org/x/mod/semver` belongs to the seal/`required_set` path (Phase F), which is explicitly deferred.
- **External resources reachable, NOT blocked.** Mainnet LCD (`https://sauron-api.infra.pocket.network`) + RPC (`https://sauron-rpc.infra.pocket.network`) are live; the full applied-plan upgrade map was captured during grounding. `rclone` remote `pocketscribe-hetzner:` (bucket `pocketscribe-mainnet-archeology`) is configured; all 5 boundary blocks were extracted + verified decoding.

**Pre-existing artifacts (verified on `main` @ `8b1c5d6`):**
- `internal/decoders/` — `Decoder` interface (`Version()` + `DecodeBlockHeader()`), shared `decoders.DecodeBlockHeader` (cometbft `RequestFinalizeBlock`, uvarint framing), `v0_1_30` adapter. `scripts/scaffold_decoder.sh` emits new delegating adapters.
- `internal/consumer/` — `Runtime` (`NewRuntime(Config{Handler,Store,Consumer,Logger,Metrics}).Run(ctx)`), `Handler` iface (`ID()`,`FirstValidVersion()`,`Handle(ctx,tx,msg)`), `Message{Height,Subject,MsgID,Data}`, `NoOpHandler`. The runtime extracts height from the subject and passes raw `msg.Data` to `Handle`.
- `internal/nats/` — `Connect(ctx,url)`→`Client` (`JetStream()`, `EnsureStream(ctx,dedupe)`), `BlockSubject(h)`, `BlockSubjectFilter = "pokt.block.*"`, `MsgID(subject,height,index)`, `StreamName`, `StreamSubjects`.
- `internal/store/` — `New(ctx,dsn)`→`Store` (`Pool()`), `ProcessHeight(ctx,consumer,height,write func(ctx,tx)error)(int64,error)`, `RegisterConsumer`, `IsSealed(ctx,height)`, registry/consolidation/processed_heights/seal/migrate.
- `internal/config/` — `Load(path)`→`*Config{Network{ID,ChainID,DisplayName,GenesisHeight,GenesisTime,GenesisDecoderVersion,StartHeight},Endpoints{RPC,LCD,GRPC}}`; `validate()` requires ID/ChainID/GenesisDecoderVersion/GenesisHeight≥1/RPC≥1.
- `internal/metrics/` — `NewConsumer(reg prometheus.Registerer)`→`*Consumer{Processed,GapsTotal,Consolidated}`.
- `internal/app/` — `root.go` wires `version`/`migrate`/`deregister`; `migrate/cmd.go` is the `NewCmd()`+`--dsn`/`envOr("PS_DATABASE_DSN",…)` pattern. Doc-stubs exist for `consumer`, `fileplugin`, `sync`, `reconciler`, `indexer`, `inspect`.
- `internal/router/`, `internal/fileplugin/` — `doc.go` stubs only.
- `schema/migrations/0001_init.sql` — `block` table (height PK, time, hash UNIQUE, proposer_address, tx_count, indexed_at) + `upgrades` table (name PK, applied_at_height BIGINT UNIQUE, applied_at_time TIMESTAMPTZ NOT NULL, decoder_version, notes, indexed_at). **Phase D adds NO migration** (both tables exist). Highest migration: `0039_consumer_registry.sql`.
- `configs/networks/{mainnet,localnet,beta}.yaml` — network configs; `mainnet.yaml` has the rpc endpoint; LCD + `upgrade_names` need adding (Task 1).
- `.golangci.yml` excludes `*/gen/`; `make ci` = `vet fmt-check lint test`; integration tests `//go:build integration` via `make test-integration`.

---

## Hard rules for the executor (read once, obey throughout)

1. **No `Co-Authored-By` / AI-attribution footer** in any commit. (memory `feedback_no_claude_signature`)
2. **`HANDOFF.md`, `RESUME.md`, `SESSION-LOG-*.md` are LOCAL-ONLY** — never `git add`/commit. Only `git add` each task's listed files.
3. **No `time.Now()`/`clock_timestamp()` as a queryable axis** (`forbidigo`). Block `time` is the chain consensus time from the decoded header; the upgrades `applied_at_time` is the chain block time fetched from the LCD, never indexer wall-clock.
4. **All Postgres access via `internal/store`** (ADR-016); handlers receive a `pgx.Tx` and never open their own connection or commit/rollback (the runtime owns the tx). Idempotent upserts only (`ON CONFLICT`). (consumers rule)
5. **DRY single sources:** NATS subjects → `internal/nats/subjects.go` (use `BlockSubject`/`BlockSubjectFilter`/`MsgID`; never hardcode `"pokt.block...."`); metric names → `internal/metrics`; config → `internal/config`.
6. **`archeology/` is a separate Go module** — never add its deps to the root `go.mod`, never `go get github.com/pokt-network/poktroll...`, never create a `go.work`. Fixtures are byte files copied into `test/fixtures/` — data, not code. (memory `reference-archeology-isolation-codegen`)
7. **Decoders: a version is a NEW directory, never modified (ADR-008).** The 5 new adapters delegate to the shared decoder; NO `gen/` in Phase D. Keep `bytedance/sonic@v1.15.2` pinned (Go-1.26 compat; memory `project-gomod-pins-phase-c`).
8. **Adding a dependency:** Phase D needs NO new module deps (cometbft fork already present; LCD client is stdlib `net/http`). If a task reaches for a new dep, stop and reconsider — `go get <mod>@<ver>` + `go mod tidy` + verify against the proxy only if genuinely required.
9. **Tests stdlib `testing` only** (no testify), consistent with Phases B/C. Golden/contract tests assert exact values. Integration tests are `//go:build integration`.
10. **`make ci` green at the end of every task** (container-free). Integration-build lint clean too: `golangci-lint run --build-tags=integration ./...` → 0 issues (verified in the finalize task). 100% coverage on decoder adapters; 80% on `internal/`.
11. **Keep Phase B tests 1–13 green.** Phase B's synthetic-fixture integration tests use `NoOpHandler` and publish marker bytes; do not break the harness they depend on (extend it additively).

---

## File structure (what each new file owns)

| File | Responsibility | Task |
|---|---|---|
| `internal/config/types.go` (MODIFY) | add `UpgradeNames []string` to `Network` | 1 |
| `configs/networks/{mainnet,localnet,beta}.yaml` (MODIFY) | add `upgrade_names` only (lcd already exists) | 1 |
| `internal/store/upgrades.go` | `UpsertUpgrade`, `ListUpgrades` | 2 |
| `internal/upgrades/upgrades.go` | LCD client (injectable `HTTPDoer`), applied_plan + block-time fetch, upsert (`package upgrades`) | 2 |
| `internal/upgrades/upgrades_test.go` | `TestFetchAppliedPlans` (httptest, no DB) | 2 |
| `internal/app/sync/cmd.go` | `ps sync-upgrades --config` (`Use: "sync-upgrades"`) | 2 |
| `internal/app/reconciler/cmd.go` | `ps reconciler` (upgrades-refresh ticker loop) | 2 |
| `internal/decoders/v0_1_{0,10,20,28,29}/decoder.go` (+ `_test.go`) | 5 delegating adapters (no gen/, trimmed package comment) | 3 |
| `internal/router/router.go` | `Router`, `Upgrade`, `NewStaticRouter`, `DecoderFor`, lookup (lenient fallback) | 4 |
| `internal/router/registry.go` | `DefaultRegistry() map[string]decoders.Decoder` (6 entries) | 4 |
| `internal/router/db.go` | `NewDBRouter`, `Refresh` (loads `upgrades` table) | 4 |
| `internal/store/block.go` | `InsertBlock(ctx,tx,*types.BlockHeader)` | 5 |
| `internal/consumer/block/handler.go` (+ `_test.go`) | block `Handler` (decode→InsertBlock) | 5 |
| `internal/app/consumer/cmd.go` + `block.go` | `ps consumer block` composition root (import alias `runtime "…/internal/consumer"`) | 5 |
| `internal/fileplugin/bootstrap.go` | read dir → publish raw meta to `pokt.block.{H}` | 6 |
| `test/integration/fileplugin_test.go` | bootstrap component test (`//go:build integration`; runs under `make test-integration`) | 6 |
| `internal/app/fileplugin/cmd.go` | `ps fileplugin --bootstrap` | 6 |
| `test/fixtures/v0_1_*/block-*-{meta}` + `*-expected.json` | 5 boundary + v0.1.0 blocks 2,3 | 7 |
| `test/fixtures/sync-upgrades/mainnet-applied-plans.json` | golden upgrade map | 2/7 |
| `test/integration/block_consumer_test.go` | tests 16a, 16b + 17 | 8 |
| `test/testcontainers/postgres.go` (MODIFY) | extend `PG.Reset` to also `TRUNCATE block, upgrades` | 8 |
| `internal/app/root.go` (MODIFY) | wire `sync`, `reconciler`, `consumer`, `fileplugin` | per task |

---

## Task 1: Network config — add `upgrade_names`

Adds the upgrade-names list (spec §4.4) that the `sync-upgrades` command needs. Config-only + a unit test.

> **Note:** `endpoints.lcd` and `Endpoints.LCD []string` ALREADY EXIST in all three network YAMLs and `internal/config/types.go`. Do NOT re-add the `lcd` key — that would create a duplicate YAML key. Only `upgrade_names` is new.

**Files:** Modify `internal/config/types.go`, `configs/networks/{mainnet,localnet,beta}.yaml`; add a case to `internal/config/config_test.go`.

- [ ] **Step 1: Add `UpgradeNames` to the `Network` struct**

In `internal/config/types.go`, add to `Network` (after `StartHeight`):
```go
	UpgradeNames          []string `mapstructure:"upgrade_names"`          // x/upgrade plan names ps sync-upgrades queries (ADR-018)
```

- [ ] **Step 2: Populate `configs/networks/mainnet.yaml`**

Add an `upgrade_names` list under `network` only (the authoritative mainnet upgrade names captured from the live chain; `sync-upgrades` skips height==0 entries so never-applied names like v0.1.1/v0.1.32 are harmless if listed). Leave the existing `endpoints.lcd` block untouched.
```yaml
network:
  # ... existing fields (id, chain_id, genesis_*, start_height) unchanged ...
  upgrade_names:
    - v0.1.2
    - v0.1.3
    - v0.1.4
    - v0.1.5
    - v0.1.6
    - v0.1.7
    - v0.1.8
    - v0.1.9
    - v0.1.10
    - v0.1.11
    - v0.1.12
    - v0.1.13
    - v0.1.14
    - v0.1.15
    - v0.1.16
    - v0.1.17
    - v0.1.18
    - v0.1.19
    - v0.1.20
    - v0.1.21
    - v0.1.22
    - v0.1.23
    - v0.1.24
    - v0.1.25
    - v0.1.26
    - v0.1.27
    - v0.1.28
    - v0.1.29
    - v0.1.30
    - v0.1.31
    - v0.1.33
    - v0.1.34
```
Add `upgrade_names: []` to `localnet.yaml` and `beta.yaml` (empty list — no upgrades to query on localnet/beta in Phase D). Do NOT touch the `endpoints` sections of any YAML.

- [ ] **Step 3: Unit test — config loads `upgrade_names`**

Add to `internal/config/config_test.go` a check (or extend the mainnet load test) asserting `cfg.Network.UpgradeNames` is non-empty and contains `"v0.1.30"`, and `cfg.Endpoints.LCD[0]` is the sauron-api URL (this already works today — the lcd field exists, the test just confirms round-trip). Run: `go test ./internal/config/` → PASS.

- [ ] **Step 4: `make ci` + commit**

```bash
make ci
git add internal/config/types.go internal/config/config_test.go configs/networks/
git commit -m "feat(config): add upgrade_names for sync-upgrades (ADR-018)"
```

---

## Task 2: `ps sync-upgrades` + reconciler refresh + the upgrades store + golden (test 14)

Implements upgrade discovery from the mainnet LCD into the `upgrades` table, the periodic reconciler refresh, and the golden test. The LCD client is injectable so the test runs hermetically.

> **Package naming:** The engine package is `internal/upgrades` (`package upgrades`) — NOT `internal/sync`. This avoids a name collision with `internal/app/sync` (which is also `package sync`) and with the stdlib `sync` package. All references to the engine use `upgrades.New(...)`, `upgrades.Fetch(...)`, `upgrades.Sync(...)`. The cobra command `Use: "sync-upgrades"` (not `"sync"`) ensures `ps sync-upgrades` as spec §12 requires.

**Files:** Create `internal/store/upgrades.go`, `internal/upgrades/upgrades.go`, `internal/upgrades/upgrades_test.go`, `internal/app/sync/cmd.go`, `internal/app/reconciler/cmd.go`, `test/fixtures/sync-upgrades/mainnet-applied-plans.json`; modify `internal/app/root.go`.

- [ ] **Step 1: Store helpers for the upgrades table**

`internal/store/upgrades.go`:
```go
package store

import (
	"context"
	"fmt"
	"time"
)

// Upgrade is one row of the upgrades table (height → decoder version).
type Upgrade struct {
	Name           string
	AppliedAtHeight int64
	AppliedAtTime  time.Time
	DecoderVersion string
}

// UpsertUpgrade idempotently records an applied upgrade. Keyed by name (PK);
// re-running sync-upgrades produces the same rows (idempotency invariant 4).
func (s *Store) UpsertUpgrade(ctx context.Context, u Upgrade) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO upgrades (name, applied_at_height, applied_at_time, decoder_version)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (name) DO UPDATE SET
		     applied_at_height = EXCLUDED.applied_at_height,
		     applied_at_time   = EXCLUDED.applied_at_time,
		     decoder_version   = EXCLUDED.decoder_version`,
		u.Name, u.AppliedAtHeight, u.AppliedAtTime, u.DecoderVersion)
	if err != nil {
		return fmt.Errorf("upsert upgrade %s: %w", u.Name, err)
	}
	return nil
}

// ListUpgrades returns all upgrades ordered by applied_at_height ASC (for the
// router's height→version mapping).
func (s *Store) ListUpgrades(ctx context.Context) ([]Upgrade, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, applied_at_height, applied_at_time, decoder_version
		 FROM upgrades ORDER BY applied_at_height ASC`)
	if err != nil {
		return nil, fmt.Errorf("list upgrades: %w", err)
	}
	defer rows.Close()
	var out []Upgrade
	for rows.Next() {
		var u Upgrade
		if err := rows.Scan(&u.Name, &u.AppliedAtHeight, &u.AppliedAtTime, &u.DecoderVersion); err != nil {
			return nil, fmt.Errorf("scan upgrade: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
```

- [ ] **Step 2: The sync logic (injectable HTTP, LCD applied_plan + block time)**

`internal/upgrades/upgrades.go` (package `upgrades`). NO separate doc.go needed — put the package comment here once.
```go
// Package upgrades discovers applied chain upgrades from the LCD and records
// them in the upgrades table (ADR-018). It is the engine behind
// `ps sync-upgrades` and the reconciler's periodic refresh.
package upgrades

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// HTTPDoer is the minimal HTTP surface sync needs; tests inject an httptest-backed client.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Syncer queries an LCD base URL for applied upgrade plans + their block times.
type Syncer struct {
	lcd    string // base URL, e.g. https://sauron-api.infra.pocket.network
	client HTTPDoer
}

// New builds a Syncer. If client is nil, http.DefaultClient is used.
func New(lcd string, client HTTPDoer) *Syncer {
	if client == nil {
		client = http.DefaultClient
	}
	return &Syncer{lcd: strings.TrimRight(lcd, "/"), client: client}
}

// appliedPlanResp is the LCD shape: {"height":"<N>"} ("0" = not applied).
type appliedPlanResp struct {
	Height string `json:"height"`
}

// blockResp is the LCD /cosmos/base/tendermint/v1beta1/blocks/{h} shape (subset).
type blockResp struct {
	Block struct {
		Header struct {
			Time time.Time `json:"time"`
		} `json:"header"`
	} `json:"block"`
}

func (s *Syncer) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.lcd+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// versionToDecoder maps an upgrade plan name "v0.1.30" to a decoder version dir "v0_1_30".
func versionToDecoder(name string) string { return strings.ReplaceAll(name, ".", "_") }

// Sync queries each upgrade name; for applied ones (height>0) it fetches the
// block time and upserts the upgrades row. Returns the count upserted.
func (s *Syncer) Sync(ctx context.Context, st *store.Store, names []string) (int, error) {
	n := 0
	for _, name := range names {
		var plan appliedPlanResp
		if err := s.getJSON(ctx, "/cosmos/upgrade/v1beta1/applied_plan/"+name, &plan); err != nil {
			return n, err
		}
		h, err := strconv.ParseInt(plan.Height, 10, 64)
		if err != nil {
			return n, fmt.Errorf("parse height for %s: %w", name, err)
		}
		if h == 0 {
			continue // not applied / skipped
		}
		var blk blockResp
		if err := s.getJSON(ctx, "/cosmos/base/tendermint/v1beta1/blocks/"+strconv.FormatInt(h, 10), &blk); err != nil {
			return n, err
		}
		if err := st.UpsertUpgrade(ctx, store.Upgrade{
			Name:            name,
			AppliedAtHeight: h,
			AppliedAtTime:   blk.Block.Header.Time,
			DecoderVersion:  versionToDecoder(name),
		}); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
```

- [ ] **Step 3: Write the failing golden test (test 14)**

`internal/upgrades/upgrades_test.go` (`package upgrades`, stdlib testing + httptest): serve `test/fixtures/sync-upgrades/mainnet-applied-plans.json` (a `{name: {applied_plan, block_time}}` capture) via `httptest.Server`, run `Sync`, and assert the upserted rows. Since `Sync` writes to a `*store.Store` (Postgres), this test belongs to the component/integration layer (`//go:build integration`) OR refactor `Sync` to return `[]store.Upgrade` and unit-test the pure transform. **Recommended:** split `Sync` into `Fetch(ctx, names) ([]store.Upgrade, error)` (pure HTTP→structs, unit-testable with httptest, no DB) + a thin `Sync` that calls `Fetch` then upserts. Write the unit test against `Fetch`:
```go
func TestFetchAppliedPlans(t *testing.T) {
	mux := http.NewServeMux()
	// serve captured applied_plan + block responses from the golden fixture
	// ... (load test/fixtures/sync-upgrades/mainnet-applied-plans.json) ...
	srv := httptest.NewServer(mux)
	defer srv.Close()
	got, err := New(srv.URL, srv.Client()).Fetch(context.Background(), []string{"v0.1.30", "v0.1.32"})
	if err != nil { t.Fatal(err) }
	// v0.1.32 is height 0 → skipped; v0.1.30 → height 484473, decoder v0_1_30
	if len(got) != 1 { t.Fatalf("got %d upgrades, want 1", len(got)) }
	if got[0].AppliedAtHeight != 484473 || got[0].DecoderVersion != "v0_1_30" {
		t.Fatalf("unexpected: %+v", got[0])
	}
}
```
Run: `go test ./internal/upgrades/` → FAIL (Fetch undefined).

- [ ] **Step 4: Refactor `Sync`→`Fetch`+upsert, capture the golden, see green**

Refactor per Step 3 (add `Fetch(ctx, names) ([]store.Upgrade, error)`; `Sync` = `Fetch` then loop `UpsertUpgrade`). Capture the golden fixture from the live LCD (one-time):
```bash
mkdir -p test/fixtures/sync-upgrades
# For each name in mainnet.yaml upgrade_names: capture applied_plan + (if applied) the block time.
# Build test/fixtures/sync-upgrades/mainnet-applied-plans.json as a map:
#   { "v0.1.30": {"height":"484473","time":"<RFC3339 from /blocks/484473>"}, ... , "v0.1.32": {"height":"0"} }
# Reference heights (captured during grounding): v0.1.2=78621 ... v0.1.30=484473 v0.1.31=635506 v0.1.33=703870 v0.1.34=788945; v0.1.32=0.
# Re-derive every height from the live LCD applied_plan responses at capture time — do not hand-transcribe.
```
Run: `go test ./internal/upgrades/` → PASS. (The test serves the captured JSON via httptest; no live network in CI.)

- [ ] **Step 5: `ps sync-upgrades` + `ps reconciler` commands**

`internal/app/sync/cmd.go` — `NewCmd()` with `--config` + `--dsn` flags (Use: `"sync-upgrades"` — matches spec §12; NOT `"sync"`): `config.Load` → `store.New` → `upgrades.New(cfg.Endpoints.LCD[0], nil)` → `upgrades.Sync(ctx, st, cfg.Network.UpgradeNames)`; print the count. Import the engine as `upgrades "github.com/pokt-network/pocketscribe/internal/upgrades"` (no alias conflict since `internal/app/sync` is package `sync` and the import name becomes `upgrades`). `internal/app/reconciler/cmd.go` — `NewCmd()` with `--config`/`--dsn`/`--interval` (default 5m): a ticker loop calling the same `upgrades.Sync` logic, backoff on error, until ctx canceled. Wire both into `internal/app/root.go` (`root.AddCommand(sync.NewCmd())`, `root.AddCommand(reconciler.NewCmd())`).

- [ ] **Step 6: `make ci` + commit**

```bash
make ci
git add internal/store/upgrades.go internal/upgrades/ internal/app/sync/ internal/app/reconciler/ internal/app/root.go test/fixtures/sync-upgrades/
git commit -m "feat(sync): ps sync-upgrades + reconciler refresh against mainnet LCD (test 14)"
```

---

## Task 3: Five delegating decoder adapters (v0.1.0/10/20/28/29)

The block header is version-invariant, so each adapter is the Phase C v0_1_30 pattern with a different version string. Use the scaffold script. **No `gen/`.**

**Files:** Create `internal/decoders/v0_1_{0,10,20,28,29}/decoder.go` + `decoder_test.go` each.

- [ ] **Step 1: Scaffold the 5 adapters**

```bash
for v in v0_1_0 v0_1_10 v0_1_20 v0_1_28 v0_1_29; do
  mkdir -p internal/decoders/$v
  scripts/scaffold_decoder.sh $v > internal/decoders/$v/decoder.go
done
gofmt -l internal/decoders/v0_1_0 internal/decoders/v0_1_10 internal/decoders/v0_1_20 internal/decoders/v0_1_28 internal/decoders/v0_1_29
```
Expected: gofmt reports nothing (scaffold output is gofmt-clean). Each `decoder.go` is the delegating adapter (package `v0_1_X`, `Decoder` struct, `Version()` returning `"v0_1_X"`, `DecodeBlockHeader` delegating to `decoders.DecodeBlockHeader`).

> **After scaffolding:** `scripts/scaffold_decoder.sh` emits a package comment referencing `gen/` (`"The buf-generated proto bindings live in the gen/ subpackage…"`). These 5 adapters have **no `gen/` subpackage** (block header is version-invariant; all codegen deferred to Phase E). For each of the 5 new `decoder.go` files, edit the package comment to remove the `gen/` sentence and replace it with a note that the block header is version-invariant and this adapter delegates to the shared decoder with no codegen. Example replacement:
> ```go
> // Package v0_1_X is the Phase-D delegating adapter for poktroll v0.1.X.
> // The block header is version-invariant (cometbft ABCI RequestFinalizeBlock);
> // this adapter delegates DecodeBlockHeader to the shared decoder.
> // No gen/ subpackage — codegen is deferred to Phase E (tx/state/event categories).
> ```

- [ ] **Step 2: Add an interface-assertion + version test per package**

For each `v0_1_X`, create `internal/decoders/v0_1_X/decoder_test.go` (`package v0_1_X`, stdlib). **Substitute the real version string for each package** — do not copy-paste the placeholder `v0_1_X` literally. The five files are `v0_1_0`, `v0_1_10`, `v0_1_20`, `v0_1_28`, `v0_1_29`; each test must assert its own version string:
```go
// Example for v0_1_0 — replace "v0_1_0" with the actual version in each file:
package v0_1_0

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_0" {
		t.Fatalf("Version() = %q, want v0_1_0", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}
```
(For `v0_1_10`: `package v0_1_10`, `want "v0_1_10"`. For `v0_1_20`: `package v0_1_20`, `want "v0_1_20"`. Etc.)

- [ ] **Step 3: Run tests (100% coverage) + `make ci` + commit**

```bash
go test -cover ./internal/decoders/...
make ci
git add internal/decoders/v0_1_0 internal/decoders/v0_1_10 internal/decoders/v0_1_20 internal/decoders/v0_1_28 internal/decoders/v0_1_29
git commit -m "feat(decoders): v0.1.0/10/20/28/29 delegating block-header adapters"
```
Expected: each adapter package reports 100.0% coverage.

---

## Task 4: DB-driven router + registry (test 15)

`internal/router` maps height→decoder via the `upgrades` table; tests use `NewStaticRouter`.

**Files:** Create `internal/router/router.go`, `internal/router/registry.go`, `internal/router/db.go`, `internal/router/router_test.go`.

- [ ] **Step 1: Write the failing unit test (test 15)**

`internal/router/router_test.go` (`package router`, stdlib): build a static router from the 5 boundary heights + a registry of the real adapters, assert `DecoderFor` at and around each boundary, and the genesis case. Also test the lenient fallback for an unregistered intermediate version, and the empty-registry rejection.

> **router.go stays 6 entries `{v0_1_0, v0_1_10, v0_1_20, v0_1_28, v0_1_29, v0_1_30}`. Do NOT add `v0_1_33` here.**
```go
package router

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_10 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10"
	v0_1_20 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_20"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	v0_1_29 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_29"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
)

func TestDecoderForBoundaries(t *testing.T) {
	reg := map[string]decoders.Decoder{
		"v0_1_0": v0_1_0.Decoder{}, "v0_1_10": v0_1_10.Decoder{},
		"v0_1_20": v0_1_20.Decoder{}, "v0_1_28": v0_1_28.Decoder{}, "v0_1_29": v0_1_29.Decoder{},
	}
	r, err := NewStaticRouter([]Upgrade{
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
		{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
	}, reg, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		h    int64
		want string
	}{
		{1, "v0_1_0"}, {78682, "v0_1_0"}, {78683, "v0_1_10"},
		{135296, "v0_1_10"}, {135297, "v0_1_20"},
		{287932, "v0_1_28"}, {382250, "v0_1_29"}, {999999, "v0_1_29"},
	}
	for _, tc := range cases {
		d, err := r.DecoderFor(tc.h)
		if err != nil {
			t.Fatalf("DecoderFor(%d): %v", tc.h, err)
		}
		if d.Version() != tc.want {
			t.Fatalf("DecoderFor(%d) = %s, want %s", tc.h, d.Version(), tc.want)
		}
	}
}

// TestDecoderForFallsBackToEarlierRegistered verifies the LENIENT router:
// an upgrade entry whose decoder_version is not in the registry is silently
// skipped and the nearest earlier registered version is returned.
// Here v0_1_31 is in the upgrades list at height 635506 but NOT in the registry;
// the router must return the v0_1_30 decoder (the nearest earlier registered).
func TestDecoderForFallsBackToEarlierRegistered(t *testing.T) {
	reg := map[string]decoders.Decoder{
		"v0_1_30": v0_1_30.Decoder{},
	}
	r, err := NewStaticRouter([]Upgrade{
		{Name: "v0.1.31", AppliedAtHeight: 635506, DecoderVersion: "v0_1_31"}, // NOT in registry
	}, reg, "v0_1_30")
	if err != nil {
		t.Fatal(err)
	}
	d, err := r.DecoderFor(635506)
	if err != nil {
		t.Fatalf("DecoderFor(635506): %v", err)
	}
	if d.Version() != "v0_1_30" {
		t.Fatalf("expected fallback to v0_1_30, got %s", d.Version())
	}
}

// TestNewStaticRouterRejectsEmptyRegistry verifies the only construction-time
// hard failure: a completely empty registry (nothing to fall back to).
func TestNewStaticRouterRejectsEmptyRegistry(t *testing.T) {
	_, err := NewStaticRouter(nil, map[string]decoders.Decoder{}, "v0_1_0")
	if err == nil {
		t.Fatal("expected error: empty decoder registry")
	}
}
```
Run: `go test ./internal/router/` → FAIL (undefined).

- [ ] **Step 2: Implement the router core**

`internal/router/router.go` (NO package comment — `doc.go` has it):
```go
package router

import (
	"fmt"
	"sort"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

// Upgrade is one height→decoder boundary loaded from the upgrades table.
type Upgrade struct {
	Name            string
	AppliedAtHeight int64
	DecoderVersion  string
}

// Router resolves a block height to the decoder version active at that height.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// staticRouter is an in-memory height→decoder map (no DB). Used by NewStaticRouter
// and as the resolved snapshot inside the DB-driven router.
type staticRouter struct {
	upgrades       []Upgrade // sorted ascending by AppliedAtHeight
	registry       map[string]decoders.Decoder
	genesisVersion string
}

// NewStaticRouter builds a router from a per-network upgrade set (data) + a
// version-keyed decoder registry. It does NOT require every upgrade's version to
// be registered — unregistered versions fall back to the nearest registered
// earlier version at lookup time (lenient; correct for the version-invariant
// block header). The only construction-time requirement is a NON-EMPTY registry
// (so DecoderFor can always return something). genesisVersion is per-network DATA
// (the version active from genesis_height), not a network branch.
func NewStaticRouter(upgrades []Upgrade, registry map[string]decoders.Decoder, genesisVersion string) (Router, error) {
	if len(registry) == 0 {
		return nil, fmt.Errorf("router: empty decoder registry")
	}
	sorted := append([]Upgrade(nil), upgrades...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AppliedAtHeight < sorted[j].AppliedAtHeight })
	return &staticRouter{upgrades: sorted, registry: registry, genesisVersion: genesisVersion}, nil
}

// DecoderFor returns the decoder for the protocol version active at height,
// falling back to the nearest EARLIER registered version if the exact version's
// decoder is not yet implemented (LENIENT — correct for the version-invariant
// block header during incremental decoder rollout: the registry holds a
// representative subset of version decoders, while the upgrades table may name
// versions whose decoders arrive in later phases).
//
// This is PURELY version-based: the only per-network input is the upgrades data
// (which version is active at which height); the resolution logic never branches
// on network. The "version active at height" sequence is [genesis_version @
// genesis_height, then upgrades ascending]; we pick the latest entry <= height
// and walk back to the nearest registered version. If nothing at-or-before height
// is registered (e.g. a network whose genesis version we have not implemented),
// we fall back to the EARLIEST registered version — still correct for the
// version-invariant header, still network-agnostic.
//
// NOTE for later phases: version-SPECIFIC categories (tx/state/event) must NOT
// tolerate this fallback — when those land, the registry must cover every version
// in the table (Phase F), or DecoderFor must gain a strict variant.
func (r *staticRouter) DecoderFor(height int64) (decoders.Decoder, error) {
	// chosen tracks the nearest registered version at-or-before height, starting
	// from the genesis version (the height-0 entry) if it is registered.
	var chosen decoders.Decoder
	if d, ok := r.registry[r.genesisVersion]; ok {
		chosen = d
	}
	for _, u := range r.upgrades { // ascending by AppliedAtHeight
		if u.AppliedAtHeight > height {
			break
		}
		if rd, ok := r.registry[u.DecoderVersion]; ok {
			chosen = rd // a nearer registered version <= height
		}
		// unregistered intermediate version → keep the previous chosen (fallback)
	}
	if chosen != nil {
		return chosen, nil
	}
	// Nothing at-or-before height is registered: fall back to the earliest
	// registered version (version-invariant header → any decoder is correct).
	for _, u := range r.upgrades {
		if rd, ok := r.registry[u.DecoderVersion]; ok {
			return rd, nil
		}
	}
	return nil, fmt.Errorf("router: empty decoder registry (height %d)", height)
}
```

- [ ] **Step 3: Registry + DB router**

`internal/router/registry.go`:
```go
package router

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_10 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10"
	v0_1_20 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_20"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	v0_1_29 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_29"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
)

// DefaultRegistry maps canonical decoder-version strings to their (stateless)
// Decoder. A new version's adapter is added here (add-decoder-version step 8).
func DefaultRegistry() map[string]decoders.Decoder {
	return map[string]decoders.Decoder{
		"v0_1_0":  v0_1_0.Decoder{},
		"v0_1_10": v0_1_10.Decoder{},
		"v0_1_20": v0_1_20.Decoder{},
		"v0_1_28": v0_1_28.Decoder{},
		"v0_1_29": v0_1_29.Decoder{},
		"v0_1_30": v0_1_30.Decoder{},
	}
}
```
`internal/router/db.go`:
```go
package router

import (
	"context"
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// DBRouter loads the upgrades table into an in-memory staticRouter snapshot and
// can Refresh it (ADR-018: DB-driven, no hardcoded heights).
type DBRouter struct {
	store          *store.Store
	registry       map[string]decoders.Decoder
	genesisVersion string
	current        Router
}

// NewDBRouter loads the upgrades table once and returns a ready router. It errors
// if any decoder_version in the table is missing from the registry.
func NewDBRouter(ctx context.Context, st *store.Store, registry map[string]decoders.Decoder, genesisVersion string) (*DBRouter, error) {
	r := &DBRouter{store: st, registry: registry, genesisVersion: genesisVersion}
	if err := r.Refresh(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Refresh reloads the upgrades table into a fresh snapshot.
func (r *DBRouter) Refresh(ctx context.Context) error {
	rows, err := r.store.ListUpgrades(ctx)
	if err != nil {
		return err
	}
	ups := make([]Upgrade, 0, len(rows))
	for _, u := range rows {
		ups = append(ups, Upgrade{Name: u.Name, AppliedAtHeight: u.AppliedAtHeight, DecoderVersion: u.DecoderVersion})
	}
	snap, err := NewStaticRouter(ups, r.registry, r.genesisVersion)
	if err != nil {
		return fmt.Errorf("router refresh: %w", err)
	}
	r.current = snap
	return nil
}

// DecoderFor delegates to the current snapshot.
func (r *DBRouter) DecoderFor(height int64) (decoders.Decoder, error) {
	return r.current.DecoderFor(height)
}
```

- [ ] **Step 4: Run tests + `make ci` + commit**

```bash
go test ./internal/router/
make ci
git add internal/router/
git commit -m "feat(router): DB-driven height->decoder dispatch + static test router (test 15)"
```
Expected: PASS. (`db.go` is covered by the integration tests in Task 8; `router.go` core is covered by the unit test here.)

---

## Task 5: `store.InsertBlock` + block consumer handler + `ps consumer block`

**Files:** Create `internal/store/block.go`, `internal/consumer/block/handler.go`, `internal/consumer/block/handler_test.go`, `internal/app/consumer/cmd.go`, `internal/app/consumer/block.go`; modify `internal/app/root.go`.

- [ ] **Step 1: `store.InsertBlock`**

`internal/store/block.go`:
```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// InsertBlock writes one row to the block table inside the caller's transaction.
// Idempotent: ON CONFLICT (height) DO NOTHING — replaying a height is a no-op
// (the sidecar produces deterministic bytes for a given height; invariant 4).
func InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO block (height, time, hash, proposer_address, tx_count)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (height) DO NOTHING`,
		h.Height, h.Time, h.Hash, h.ProposerAddress, h.TxCount)
	if err != nil {
		return fmt.Errorf("insert block at height %d: %w", h.Height, err)
	}
	return nil
}
```
(Package-level function taking `pgx.Tx`, mirroring `insertProcessedHeight`. No `chain_id` — not a column.)

- [ ] **Step 2: Write the failing handler test**

`internal/consumer/block/handler_test.go` (`package block`, stdlib). Use a stub router returning the v0_1_30 decoder and the real Phase C fixture to assert the handler decodes + maps; the InsertBlock DB write is covered by the Task 8 integration test, so here assert the decode/mapping path via a fake `inserter`:
```go
package block

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
	"github.com/pokt-network/pocketscribe/internal/types"
)

type stubRouter struct{ d decoders.Decoder }
func (s stubRouter) DecoderFor(int64) (decoders.Decoder, error) { return s.d, nil }

type fakeInserter struct{ got *types.BlockHeader }
func (f *fakeInserter) InsertBlock(_ context.Context, _ pgx.Tx, h *types.BlockHeader) error { f.got = h; return nil }

func TestHandleDecodesAndInserts(t *testing.T) {
	raw, err := os.ReadFile("../../decoders/testdata/block-190974-meta")
	if err != nil { t.Fatal(err) }
	fi := &fakeInserter{}
	h := New(stubRouter{v0_1_30.Decoder{}}, fi)
	if err := h.Handle(context.Background(), nil, consumer.Message{Height: 190974, Data: raw}); err != nil {
		t.Fatal(err)
	}
	if fi.got == nil || fi.got.Height != 190974 {
		t.Fatalf("inserter got %+v", fi.got)
	}
}
```
Run: `go test ./internal/consumer/block/` → FAIL (undefined New).

- [ ] **Step 3: Implement the handler (router + inserter injected)**

`internal/consumer/block/handler.go` (carries the package comment — it's the package's primary file):
```go
// Package block implements the consumer.Handler that decodes block headers and
// writes the block table. It decodes consumer-side via the router (ADR-008): the
// sidecar publishes raw block-{H}-meta bytes; this handler version-dispatches and
// maps to the canonical types.BlockHeader.
package block

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Router is the subset of router.Router the block handler needs.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// Inserter is the store surface the handler writes through (real: store.InsertBlock).
type Inserter interface {
	InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error
}

// Handler decodes block-{H}-meta payloads and writes the block table.
type Handler struct {
	router   Router
	inserter Inserter
}

// New constructs the block handler.
func New(r Router, inserter Inserter) *Handler { return &Handler{router: r, inserter: inserter} }

func (h *Handler) ID() string                { return "block" }
func (h *Handler) FirstValidVersion() string { return "v0.1.0" }

// Handle decodes the raw meta bytes via the height-selected decoder and inserts
// the block row inside the runtime-managed transaction (invariant 5).
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, msg consumer.Message) error {
	dec, err := h.router.DecoderFor(msg.Height)
	if err != nil {
		return err
	}
	header, err := dec.DecodeBlockHeader(msg.Data)
	if err != nil {
		return err
	}
	return h.inserter.InsertBlock(ctx, tx, header)
}
```
> Note: `store.InsertBlock` is a package-level func; to satisfy the `Inserter` interface, pass a tiny adapter in the composition root: `storeInserter{}` whose method calls `store.InsertBlock(...)`. Define `type storeInserter struct{}; func (storeInserter) InsertBlock(ctx, tx, h) error { return store.InsertBlock(ctx, tx, h) }` in `internal/app/consumer/block.go`. (Or make `InsertBlock` a `*store.Store` method; the package-level func + adapter keeps it consistent with `insertProcessedHeight` while staying testable via the `Inserter` interface.)

- [ ] **Step 4: `ps consumer block` composition root**

`internal/app/consumer/cmd.go` — parent `NewCmd()` (`Use: "consumer"`) that adds `newBlockCmd()`. `internal/app/consumer/block.go` — `newBlockCmd()` with `--config`/`--dsn`/`--nats-url` flags whose `RunE` wires: `config.Load` → `store.New` → `nats.Connect`/`EnsureStream` → (use the returned `jetstream.Stream` to create the consumer via the **2-arg Phase B idiom**): `stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable:"block", FilterSubject: natsx.BlockSubjectFilter, AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy, MaxDeliver: -1, AckWait: 30*time.Second})` → `router.NewDBRouter(ctx, st, router.DefaultRegistry(), cfg.Network.GenesisDecoderVersion)` → `block.New(rtr, storeInserter{})` → `runtime.NewRuntime(runtime.Config{Handler: h, Store: st, Consumer: jsCons, Logger: slog.Default(), Metrics: metrics.NewConsumer(prometheus.DefaultRegisterer)}).Run(ctx)`.

> **Import alias required:** `internal/app/consumer/block.go` is `package consumer`. It must import `internal/consumer` (also `package consumer`) with an explicit alias to avoid the self-name collision: `runtime "github.com/pokt-network/pocketscribe/internal/consumer"`. All calls to the runtime use `runtime.NewRuntime(runtime.Config{...})`.

Wire `root.AddCommand(consumer.NewCmd())` in `root.go`. (Mirror `migrate/cmd.go` flag/`envOr` conventions; read the real Phase B runtime/nats APIs to get signatures exact.)

- [ ] **Step 5: Run tests + `make ci` + commit**

```bash
go test ./internal/consumer/block/ ./internal/store/
make ci
git add internal/store/block.go internal/consumer/block/ internal/app/consumer/ internal/app/root.go
git commit -m "feat(consumer): ps consumer block — decode header + write block table"
```

---

## Task 6: `ps fileplugin --bootstrap` sidecar

A dumb byte-forwarder: scan an input dir for `block-{H}-meta` files, publish raw bytes to `pokt.block.{H}` (height from filename), up to `--max-height`.

**Files:** Create `internal/fileplugin/bootstrap.go`, `test/integration/fileplugin_test.go`, `internal/app/fileplugin/cmd.go`; modify `internal/app/root.go`.

> **Test location:** The integration test lives in `test/integration/fileplugin_test.go` (`package integration`, `//go:build integration`). It must NOT go under `internal/fileplugin/` — `make test-integration` globs only `./test/...`, so a test under `internal/` would NEVER be executed by any make target. The `test/integration/` location reuses the shared NATS singleton started by `TestMain` and is picked up automatically.

- [ ] **Step 1: Write the failing component test**

`test/integration/fileplugin_test.go` (`package integration`, `//go:build integration`): write 3 `block-{H}-meta` files to a temp dir, run `Bootstrap(ctx, natsclient, dir, 0)`, assert 3 messages land on `pokt.block.{H}` with the correct bytes + `Nats-Msg-Id`. Run → FAIL (undefined Bootstrap).

- [ ] **Step 2: Implement bootstrap publish**

`internal/fileplugin/bootstrap.go` (NO package comment — `doc.go` has it): scan dir with `filepath.Glob(dir+"/block-*-meta")`, parse height from each filename, sort ascending, skip `> maxHeight` (0 = no cap), read bytes, publish via the jetstream client with `jetstream.WithMsgID(natsx.MsgID(natsx.BlockSubject(h), h, 0))` to `natsx.BlockSubject(h)`. Use the Phase B `natsx` client (`Connect`/`EnsureStream`/`JetStream`). Return the count published. Parse height with a helper that strips the `block-`/`-meta` affixes (reuse `natsx.HeightFromBlockSubject` shape or a small local parse).

- [ ] **Step 3: `ps fileplugin --bootstrap` command**

`internal/app/fileplugin/cmd.go` — `NewCmd()` with `--bootstrap` (bool), `--input-dir`, `--max-height` (int64, 0=none), `--nats-url`: `RunE` connects NATS, ensures the stream, runs `Bootstrap`. Wire `root.AddCommand(fileplugin.NewCmd())`. (Live mode is Phase E+; for Phase D `--bootstrap` is required — error if not set.)

- [ ] **Step 4: `make ci` + integration test + commit**

```bash
make ci
make test-integration   # runs the bootstrap integration test from test/integration/fileplugin_test.go (Docker)
git add internal/fileplugin/ test/integration/fileplugin_test.go internal/app/fileplugin/ internal/app/root.go
git commit -m "feat(fileplugin): ps fileplugin --bootstrap publishes raw block-meta to NATS"
```

---

## Task 7: Curate the 5 real fixtures (+ v0.1.0 blocks 2,3 for the gap test)

Extract the boundary blocks via `rclone` (already configured) into `test/fixtures/`, plus 3 consecutive v0.1.0 blocks for the self-heal test, and author `expected.json` from the decoded headers. **Meta files only** (block-header consumer; `data` is Phase E).

**Files:** Create `test/fixtures/v0_1_{0,10,20,28,29}/block-*-meta` + `block-*-expected.json`.

- [ ] **Step 1: Extract boundary blocks + v0.1.0 blocks 2,3**

```bash
# Remote: pocketscribe-hetzner: ; bucket: pocketscribe-mainnet-archeology
# Tarballs are {version}-h{H}-fileplugin.tar.xz. List to find exact names:
rclone lsf pocketscribe-hetzner:pocketscribe-mainnet-archeology | grep -E '^(v0.1.0|v0.1.10|v0.1.20|v0.1.28|v0.1.29)-'
# For each version, stream-extract the needed meta file(s) (boundary block; v0.1.0 also blocks 2,3):
#   rclone cat pocketscribe-hetzner:.../<tarball> | xz -d | tar -x -C test/fixtures/<vdir>/ "./block-<H>-meta"
# Heights: v0_1_0 -> 1,2,3 ; v0_1_10 -> 78683 ; v0_1_20 -> 135297 ; v0_1_28 -> 287932 ; v0_1_29 -> 382250
```
(v0.1.28/29 tarballs are multi-GB; `xz -d` streams ~3–4 min each — one-time. The boundary blocks were already extracted to `/tmp/block-{1,78683,135297,287932,382250}-meta` during grounding and can be copied if still present; re-extract via rclone for reproducibility. Drop the `.bin` suffix — match the FilePlugin/Phase-C naming.)

- [ ] **Step 2: Author `expected.json` by decoding each meta**

For each fixture, decode the meta with the shared decoder and write `block-{H}-expected.json` `{height,time,hash,proposer_address,tx_count}`. A throwaway helper or a `go test`-driven generator that calls `decoders.DecodeBlockHeader(os.ReadFile(meta))` and prints the JSON is the reliable way (don't hand-type hashes). Verify each decodes to the expected height (1/2/3, 78683, 135297, 287932, 382250).

- [ ] **Step 3: Commit the fixtures**

```bash
git add test/fixtures/v0_1_0 test/fixtures/v0_1_10 test/fixtures/v0_1_20 test/fixtures/v0_1_28 test/fixtures/v0_1_29
git commit -m "test(fixtures): real v0.1.0/10/20/28/29 block-meta + expected.json"
```
(`make ci` is unaffected — fixtures are data; the integration tests in Task 8 consume them.)

---

## Task 8: Integration tests 16 + 17 (block consumer end-to-end)

**Files:** Create `test/integration/block_consumer_test.go` (`//go:build integration`); extend the Phase B harness: add `startBlockRuntime` helper + `publishFixture` helper; extend `test/testcontainers/postgres.go` `pg.Reset` to also `TRUNCATE block, upgrades`.

**Harness prerequisites (do these first before writing the test bodies):**

- [ ] **Step 0: Extend harness helpers**

  a. **`startBlockRuntime(t, stream, name string, rtr block.Router) *runtimeHandle`** — mirrors the existing `startRuntime` (which is hardcoded to `consumer.NewNoOpHandler`), but wires `block.New(rtr, storeInserter{})` instead. Build the consumer via `stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable:name, FilterSubject:natsx.BlockSubjectFilter, ...})`, a per-runtime `metrics.NewConsumer(prometheus.NewRegistry())` (NOT `prometheus.DefaultRegisterer` — the 2nd `MustRegister` panics in a multi-runtime test process), and `consumer.NewRuntime(...)`. The `storeInserter` adapter (`type storeInserter struct{}; func (storeInserter) InsertBlock(ctx,tx,h) error { return store.InsertBlock(ctx,tx,h) }`) may be defined in the test file or a shared test helper.

  b. **`publishFixture(t, stream, h int64, metaBytes []byte)`** — publishes `metaBytes` to `natsx.BlockSubject(h)` with `Nats-Msg-Id` set via `natsx.MsgID(natsx.BlockSubject(h), h, 0)`.

  c. **Extend `PG.Reset`** in `test/testcontainers/postgres.go` to also `TRUNCATE block, upgrades` (in addition to the existing `consumer_registry, consumer_consolidation, processed_heights`). Call `pg.Reset(t)` at the start of tests 16 and 17 to ensure a clean baseline.

- [ ] **Step 1: Test 16a — block-row correctness (5 boundary fixtures)**

Publish each of the 5 boundary fixture files (heights 1, 78683, 135297, 287932, 382250) via `publishFixture`. Run the real block consumer Runtime using `startBlockRuntime` wired with a `NewStaticRouter` seeded with the 5 boundary upgrades + `DefaultRegistry()`. Poll `SELECT height, time, hash, proposer_address, tx_count FROM block WHERE height=$1` until the row appears (timeout: 10s). Assert the row equals the fixture's `expected.json`. **Do NOT assert `IsSealed` or cursor values for these heights** — the heights are non-contiguous so consolidation will never advance past 1, and `IsSealed(78683)` will never be true.

- [ ] **Step 2: Test 16b — AND-seal with contiguous v0.1.0 blocks 1, 2, 3**

Publish the 3 contiguous v0.1.0 block fixtures (heights 1, 2, 3) via `publishFixture`. Run `startBlockRuntime` (block consumer) + `startRuntime` (NoOp #2) — exactly 2 consumers registered. Wait until both cursors are `consolidated_up_to >= 3`. Assert `store.IsSealed(ctx, 3) == true`. This validates the full ack-after-commit + seal path with real block bytes.

- [ ] **Step 3: Test 17 — self-heal with a real gap**

Call `pg.Reset(t)`. Adapt Phase B's `TestE2EForcedGapRecordedThenFilled` with real bytes: publish v0.1.0 blocks 1 and 3 (omit 2) via `publishFixture`, assert the block cursor freezes at 1 with a gap metric, then publish block 2 and assert the cursor advances to 3 and all three block rows exist. (This needs the 3 consecutive v0.1.0 fixtures from Task 7.)

- [ ] **Step 4: Run + commit**

```bash
make test-integration
git add test/integration/block_consumer_test.go test/testcontainers/postgres.go test/   # + any harness helper changes
git commit -m "test(integration): block consumer + 5 fixtures (test 16) + self-heal gap (test 17)"
```
Expected: tests 14–17 green; Phase B tests 1–13 still green.

---

## Task 9: Finalize — full verification + mark Phase D complete

**Files:** Modify `internal/app/root.go` (confirm all commands wired), `docs/superpowers/specs/2026-06-08-slice-1-design.md`, `docs/architecture/05-versioning.md` (correct the stale router section), go.mod/go.sum (only if tidy changes them).

- [ ] **Step 1: Tidy + full gauntlet**

```bash
go mod tidy && go mod verify
make ci
golangci-lint run --build-tags=integration ./...   # 0 issues
go test -cover ./internal/decoders/... ./internal/router/ ./internal/upgrades/   # adapters 100%
make test-integration                                # tests 1-17 green
go list -m all | grep -iE 'pokt-network/poktroll|pocketscribe/archeology' && echo CONTAMINATED || echo OK
```
Expected: all green; no contamination; sonic still v1.15.2.

- [ ] **Step 2: Correct the stale architecture doc + add ADR-018 forward-pointer**

In `docs/architecture/05-versioning.md`, replace the superseded hardcoded-`DefaultUpgrades`/`panic` router section (lines ~93–152) with the ADR-018 DB-driven design implemented here (`DecoderFor`/`Refresh`/`NewStaticRouter`, registry map, genesis fallback). Brief, factual.

Additionally, add a one-line forward-pointer note near the top of the updated section (or in a callout box) stating: *"ADR-018 supersedes the 'hardcoded fallback' implementation note from ADR-008. The router is DB-driven (`NewDBRouter`/`Refresh`) and version-based; there are no hardcoded heights or network branches in routing logic."* Do NOT edit the ADR bodies themselves — ADRs are immutable. The forward-pointer belongs only in the architecture doc.

- [ ] **Step 3: Mark the spec Phase D complete**

After the `**Phase C complete**:` line, add:
```markdown
**Phase D complete**: branch slice-1/phase-d — ps sync-upgrades populates the upgrades table from mainnet LCD (golden test 14); DB-driven router dispatches decoders by height with NewStaticRouter for unit tests (test 15); ps fileplugin --bootstrap republishes raw block-meta bytes to pokt.block.{H}; ps consumer block decodes consumer-side via the router and writes the block table; 5 real fixtures (v0.1.0/10/20/28/29) drive block rows + AND-seal (test 16) and self-heal gap recovery (test 17). 5 delegating decoder adapters (no gen/, block header version-invariant). make ci + integration-build lint clean; tests 1-17 green. 2026-06-XX.
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md docs/architecture/05-versioning.md go.mod go.sum
git commit -m "docs(spec): mark Slice 1 Phase D complete + correct router architecture doc"
```

---

## Self-review checklist

**Spec coverage (§9 Phase D + tests 14–17):** sidecar bootstrap (Task 6) · router DB-driven (Task 4) · sync-upgrades vs mainnet (Task 2) · block consumer writes block table (Task 5) · 5 real fixtures (Task 7) · NoOp #2 stays wired (Task 8) · tests 14 (Task 2), 15 (Task 4), 16a/16b/17 (Task 8). Decoders for the 5 versions (Task 3).

**Invariants/ADRs:** consumer-side decode via router (ADR-008/018); raw meta on the bus (ADR-003/022, enables replay); ack-after-commit unchanged (ADR-007); InsertBlock idempotent ON CONFLICT (invariant 4); no `time.Now()` (block time = chain header; upgrade time = chain block time); no `internal/proto`/chain_id/gen/ (YAGNI); archeology isolation (fixtures are byte files); no new migration. Router is LENIENT + VERSION-BASED (no `if mainnet`, no network branches); unregistered decoder versions fall back to nearest earlier registered (correct for version-invariant header). The `upgrades` table is the only per-network datum (populated from `ps sync-upgrades`); routing logic never branches on network identity.

**Package naming + import collisions resolved:**
- Engine package: `internal/upgrades` (`package upgrades`) — NOT `internal/sync`.
- `internal/app/consumer/block.go` imports `internal/consumer` as `runtime "…/internal/consumer"` (both are `package consumer`; alias prevents self-name collision).
- `internal/app/sync/cmd.go` imports `internal/upgrades` as `upgrades "…/internal/upgrades"` (no collision — different package names now).
- Cobra `Use: "sync-upgrades"` in `internal/app/sync/cmd.go` (not `"sync"`).

**Prometheus registry isolation:** production `ps consumer block` uses `prometheus.DefaultRegisterer` (single runtime, fine). Integration tests (Task 8) use `metrics.NewConsumer(prometheus.NewRegistry())` per runtime — prevents `MustRegister` panic when two runtimes run in the same test process.

**Fileplugin test lives in `test/integration/fileplugin_test.go`** (NOT `internal/fileplugin/`) — `make test-integration` globs `./test/...` only.

**Test 16 split:** (a) block-row correctness: poll `SELECT FROM block WHERE height=$1` for each of the 5 non-contiguous boundary fixtures — NO seal assertion; (b) AND-seal: contiguous v0.1.0 blocks 1/2/3, wait both cursors to 3, assert `IsSealed(3)==true`.

**`pg.Reset` extended** to also `TRUNCATE block, upgrades`; called at start of tests 16 and 17.

**Task 3 adapters:** package comment trimmed to remove stale `gen/` reference after scaffolding; per-package version string substituted explicitly (no `v0_1_X` placeholder left in committed files).

**Task 9 docs/architecture/05-versioning.md:** forward-pointer notes that ADR-018 supersedes ADR-008's "hardcoded fallback" implementation note. ADR bodies unchanged (immutable).

**Type/name consistency:** `router.Upgrade{Name,AppliedAtHeight,DecoderVersion}` vs `store.Upgrade{...,AppliedAtTime,...}` (store has the time column; router omits it — intentional, note it). `Router.DecoderFor(int64)(decoders.Decoder,error)` identical across router, db, block.Handler's `Router` subset, and the test stub. `versionToDecoder("v0.1.30")=="v0_1_30"` matches the registry keys + decoder `Version()` strings.

**No placeholders:** the wiring tasks (5 Step 4, 6 Step 3) reference the real Phase B runtime/nats/cmd APIs — the implementer reads those files to get signatures exact; all core logic (router, upgrades engine, InsertBlock, adapters, store helpers) is verbatim.
