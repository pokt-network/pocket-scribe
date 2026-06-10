# Slice 1 — Phase F Implementation Plan: Version-aware orchestration + multi-version expansion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement spec §9 Phase F: (1) height-aware `required_set` / sealing with semver gating, dormant consumers, wakeup, multi-network correctness, backfill semantics and the sidecar payload caps (spec tests 22–27), then (2) expand fixture coverage to every mainnet poktroll version with archived FilePlugin data (24 new versions, ~3 fixtures each, spec-literal), with a documented skill to cover v0.1.30/v0.1.31/v0.1.33 when their archeology data lands.

**Architecture:** Part 1 replaces the Phase B stubs in `internal/store` (`RequiredSet`, `IsSealed`) with the spec §4.10 semantics computed in Go (semver comparison is not SQL-friendly): a pure `firstValidHeight` core + a new `internal/protover` package wrapping `golang.org/x/mod/semver`. Consumers gain a dormancy gate at startup. The sidecar gains 256 KiB/1 MiB payload caps. Part 2 promotes the fixture decode/report pipeline from `tools/fixtureextract` into `internal/fixturereport` (single source for generation AND golden verification), adds a `scan` mode + curation script, then curates fixtures per version batch from the already-downloaded Hetzner tarballs in `/tmp`.

**Tech Stack:** Go 1.26, pgx v5, NATS JetStream, testcontainers (existing harness), `golang.org/x/mod/semver` (new dep, Go-team maintained).

---

## Context (read before starting)

| What | Where |
|---|---|
| Spec semantics for required_set / dormant / sealing | `docs/superpowers/specs/2026-06-08-slice-1-design.md:195-220` (§4.10) |
| Spec Phase F + tests 22–27 | same file `:478-492` (§9) and `:571-577` (§11.1) |
| Fixture curation criteria (block types per version) | same file §8.1 (`:356-365`) |
| Verified supplier break map | `docs/research/supplier-shape-breaks.md` (breaks: v0_1_2*, v0_1_8, v0_1_12*, v0_1_27; * = migration-module only, outside supplier closure) |
| Chain-authoritative upgrade heights | `/tmp/pocketscribe-discovery/versions.yaml` (from Hetzner bucket; verified vs Sauron LCD 2026-05-22) |
| Decoder rules (never assume cross-version stability) | `.claude/rules/decoders.md` rules 9–10 |

**Decisions locked with the user (2026-06-10):**
1. Orchestration front first, fixture downloads ran in background (DONE — 27 tarballs in `/tmp`, sha256-verified).
2. Fixtures: spec-literal (~3/version) for the 24 versions with archived tarballs.
3. Spec test 27 (sidecar size caps) is IN Phase F.
4. v0.1.30/v0.1.31/v0.1.33 have no archived FilePlugin output (archeology still running on multi-1): build a skill + docs to curate them when data reaches the bucket; until then they are covered by machine-derived closure evidence (break map shows zero supplier breaks after v0_1_27) and the existing v0_1_30 decoder + router fallback.

**Hard rules (handoff):** no AI footer in commits; no push (user pushes); TDD; lint must ALSO pass with `--build-tags=integration`; decoders 100% coverage, `internal/` ≥90% with real error paths; version-based never network-based; HANDOFF/SESSION-LOG files stay local.

**Numbering trap:** `test/integration/batch_runtime_crash_test.go` uses informal labels "22a/22b/23" that do NOT correspond to spec §11.1 numbering. Every new test in this plan must carry a comment `// spec test N (§11.1)`.

**Version format reality check** (why `internal/protover` exists):
- `consumer_registry.first_valid_version` and `upgrades.name` store dotted form (`"v0.1.20"`).
- `config.Network.GenesisDecoderVersion` and `upgrades.decoder_version` store underscored decoder-dir form (`"v0_1_0"`).
- Spec §4.10: comparisons are semver (`v0.1.10 > v0.1.9`), normalized at the system boundary; internal code never compares raw strings.

---

## Part 1 — Version-aware orchestration (spec tests 22–27)

### Task 0: Branch

- [ ] **Step 1: Create the phase branch from main**

```bash
cd /home/overlordyorch/development/pocketscribe
git checkout main && git checkout -b slice-1/phase-f
```

Expected: branch `slice-1/phase-f` created from `12b6616` (or current main HEAD).

---

### Task 1: `internal/protover` — canonical protocol-version handling

**Files:**
- Create: `internal/protover/protover.go`
- Create: `internal/protover/protover_test.go`
- Modify: `go.mod` / `go.sum` (add `golang.org/x/mod`)

- [ ] **Step 1: Add the dependency**

```bash
go get golang.org/x/mod@latest && go mod tidy
```

Expected: `golang.org/x/mod vX.Y.Z` appears in `go.mod` (direct). Note: memoria `project_gomod_pins_phase_c` pins only sonic/cosmos deps; x/mod has no known Go-1.26 issue.

- [ ] **Step 2: Write the failing test**

`internal/protover/protover_test.go`:

```go
package protover

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "v0.1.30", want: "v0.1.30"},
		{in: "v0_1_30", want: "v0.1.30"},      // decoder-dir spelling accepted
		{in: " v0.1.0 ", want: "v0.1.0"},      // boundary trims whitespace
		// x/mod/semver accepts two-component versions; Canonical pads the
		// patch. Intentional leniency — poktroll tags are always 3-component.
		{in: "v0.1", want: "v0.1.0"},
		{in: "0.1.30", wantErr: true},         // missing v prefix → invalid
		{in: "v0.1.x", wantErr: true},
		{in: "", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("Normalize(%q): want error, got %q", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Normalize(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCompareSemverNotLexicographic(t *testing.T) {
	// The spec §4.10 callout: "v0.1.10" > "v0.1.9" (lexicographic gets this wrong).
	if Compare("v0.1.10", "v0.1.9") <= 0 {
		t.Fatal("v0.1.10 must compare greater than v0.1.9")
	}
	if Compare("v0.1.20", "v0.1.20") != 0 {
		t.Fatal("equal versions must compare 0")
	}
	if Compare("v0.1.2", "v0.2.0") >= 0 {
		t.Fatal("v0.1.2 must compare less than v0.2.0")
	}
}

func TestToDecoderDir(t *testing.T) {
	got, err := ToDecoderDir("v0.1.30")
	if err != nil || got != "v0_1_30" {
		t.Fatalf("ToDecoderDir(v0.1.30) = %q, %v; want v0_1_30", got, err)
	}
	if _, err := ToDecoderDir("nope"); err == nil {
		t.Fatal("ToDecoderDir must reject invalid input")
	}
}
```

- [ ] **Step 3: Run to verify failure**

```bash
go test ./internal/protover/
```

Expected: FAIL (package does not exist / undefined functions).

- [ ] **Step 4: Implement**

`internal/protover/protover.go`:

```go
// Package protover parses and compares poktroll protocol versions. It is the
// SINGLE boundary where version strings are normalized (spec §4.10: semver
// comparison, never lexicographic; internal code never compares raw strings).
// Two spellings exist in the system — dotted ("v0.1.30": upgrades.name,
// consumer_registry.first_valid_version) and underscored decoder-dir form
// ("v0_1_30": network.genesis_decoder_version, upgrades.decoder_version) —
// Normalize accepts both and returns the canonical dotted form.
package protover

import (
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
)

// Normalize returns the canonical dotted form ("vMAJOR.MINOR.PATCH") of a
// version given in dotted or underscored spelling. Errors on anything that is
// not a valid semver tag with a leading "v".
func Normalize(s string) (string, error) {
	c := strings.ReplaceAll(strings.TrimSpace(s), "_", ".")
	if !semver.IsValid(c) {
		return "", fmt.Errorf("invalid protocol version %q", s)
	}
	return semver.Canonical(c), nil
}

// Compare orders two canonical versions: -1 if a < b, 0 if equal, +1 if a > b.
// Inputs MUST come from Normalize (garbage compares as lowest in x/mod/semver,
// silently — hence the boundary discipline).
func Compare(a, b string) int { return semver.Compare(a, b) }

// ToDecoderDir converts any accepted spelling to the decoder directory
// spelling ("v0.1.30" → "v0_1_30").
func ToDecoderDir(s string) (string, error) {
	n, err := Normalize(s)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(n, ".", "_"), nil
}
```

- [ ] **Step 5: Run tests, lint**

```bash
go test ./internal/protover/ -cover && golangci-lint run ./internal/protover/
```

Expected: PASS, coverage 100%, lint clean.

- [ ] **Step 6: Commit**

```bash
git add internal/protover/ go.mod go.sum
git commit -m "feat(protover): canonical protocol-version normalization and semver compare

Single boundary for version-string handling per spec §4.10. Accepts dotted
and decoder-dir spellings; comparison via golang.org/x/mod/semver."
```

---

### Task 2: Pure `firstValidHeight` core in store

**Files:**
- Create: `internal/store/versiongate.go`
- Create: `internal/store/versiongate_test.go` (unit — NO build tag, no DB)

- [ ] **Step 1: Write the failing test**

`internal/store/versiongate_test.go`:

```go
package store

import (
	"math"
	"testing"
)

func TestFirstValidHeight(t *testing.T) {
	upgrades := map[string]int64{
		"v0.1.20": 135297,
		"v0.1.27": 247893,
	}
	cases := []struct {
		name, firstValid, genesis string
		ups                       map[string]int64
		want                      int64
		wantErr                   bool
	}{
		// V <= genesis → valid from height 1 (spec §4.10 first branch).
		{name: "equal to genesis", firstValid: "v0.1.0", genesis: "v0.1.0", ups: upgrades, want: 1},
		{name: "below genesis", firstValid: "v0.1.0", genesis: "v0_1_33", ups: nil, want: 1},
		{name: "underscored both", firstValid: "v0_1_20", genesis: "v0_1_33", ups: nil, want: 1},
		// V in upgrades → the applied height.
		{name: "upgrade member", firstValid: "v0.1.20", genesis: "v0.1.0", ups: upgrades, want: 135297},
		{name: "upgrade member underscored", firstValid: "v0_1_27", genesis: "v0_1_0", ups: upgrades, want: 247893},
		// V > genesis and not applied → dormant.
		{name: "dormant", firstValid: "v0.2.0", genesis: "v0.1.0", ups: upgrades, want: DormantHeight},
		{name: "dormant empty upgrades", firstValid: "v0.1.20", genesis: "v0.1.0", ups: nil, want: DormantHeight},
		// Error paths (real, not padding).
		{name: "bad first_valid", firstValid: "nope", genesis: "v0.1.0", ups: nil, wantErr: true},
		{name: "bad genesis", firstValid: "v0.1.0", genesis: "nope", ups: nil, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := firstValidHeight(c.firstValid, c.genesis, c.ups)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestDormantHeightIsMaxInt64(t *testing.T) {
	// The sentinel must be unreachable by any real chain height.
	if DormantHeight != math.MaxInt64 {
		t.Fatalf("DormantHeight = %d", DormantHeight)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test ./internal/store/ -run 'TestFirstValidHeight|TestDormantHeight'
```

Expected: FAIL (undefined: firstValidHeight, DormantHeight).

- [ ] **Step 3: Implement**

`internal/store/versiongate.go`:

```go
package store

import (
	"fmt"
	"math"

	"github.com/pokt-network/pocketscribe/internal/protover"
)

// DormantHeight is the spec §4.10 INFINITY sentinel: a consumer whose
// first_valid_version is above the network genesis and not present in the
// upgrades table is dormant on this network — required at no height.
const DormantHeight int64 = math.MaxInt64

// firstValidHeight implements consumer_first_valid_height(c, network) from
// spec §4.10. upgradeHeights is keyed by protover-Normalized upgrade name.
func firstValidHeight(firstValid, genesis string, upgradeHeights map[string]int64) (int64, error) {
	v, err := protover.Normalize(firstValid)
	if err != nil {
		return 0, fmt.Errorf("first_valid_version: %w", err)
	}
	g, err := protover.Normalize(genesis)
	if err != nil {
		return 0, fmt.Errorf("genesis_decoder_version: %w", err)
	}
	if protover.Compare(v, g) <= 0 {
		return 1, nil
	}
	if h, ok := upgradeHeights[v]; ok {
		return h, nil
	}
	return DormantHeight, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/store/ -run 'TestFirstValidHeight|TestDormantHeight' -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/versiongate.go internal/store/versiongate_test.go
git commit -m "feat(store): pure consumer_first_valid_height core per spec §4.10"
```

---

### Task 3: Store wiring — `ConsumerFirstValidHeight`, `FirstValidHeights`, boundary normalization in `RegisterConsumer`

**Files:**
- Modify: `internal/store/versiongate.go` (add the two Store methods + upgrades map loader)
- Modify: `internal/store/consumer_registry.go:10-20` (`RegisterConsumer` normalizes at the boundary)
- Create: `test/integration/versiongate_test.go`

- [ ] **Step 1: Write the failing integration test**

`test/integration/versiongate_test.go` (mirror harness usage from `test/integration/registry_test.go:10-20`; `setConsolidation` already exists in `seal_test.go`):

```go
//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/store"
)

func seedUpgrade(t *testing.T, s *store.Store, name string, height int64, decoderVersion string) {
	t.Helper()
	err := s.UpsertUpgrade(context.Background(), store.Upgrade{
		Name:            name,
		AppliedAtHeight: height,
		AppliedAtTime:   time.Date(2025, 6, 17, 16, 15, 0, 0, time.UTC),
		DecoderVersion:  decoderVersion,
	})
	if err != nil {
		t.Fatalf("seed upgrade %s: %v", name, err)
	}
}

func TestConsumerFirstValidHeight(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	h, err := s.ConsumerFirstValidHeight(ctx, "v0.1.20", "v0_1_0")
	if err != nil || h != 135297 {
		t.Fatalf("got %d, %v; want 135297", h, err)
	}
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.1.0", "v0_1_0")
	if err != nil || h != 1 {
		t.Fatalf("got %d, %v; want 1", h, err)
	}
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.2.0", "v0_1_0")
	if err != nil || h != store.DormantHeight {
		t.Fatalf("got %d, %v; want DormantHeight", h, err)
	}
	// Error path: garbage genesis from a broken config must fail loud.
	if _, err := s.ConsumerFirstValidHeight(ctx, "v0.1.0", "garbage"); err == nil {
		t.Fatal("want error on invalid genesis version")
	}
}

func TestFirstValidHeightsMap(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")
	mustRegister(t, s, "blocklike", "v0.1.0")
	mustRegister(t, s, "late", "v0.1.20")
	mustRegister(t, s, "phantom", "v0.2.0")
	// Deregistered consumers are excluded.
	mustRegister(t, s, "gone", "v0.1.0")
	if _, err := s.DeregisterConsumer(ctx, "gone"); err != nil {
		t.Fatal(err)
	}

	m, err := s.FirstValidHeights(ctx, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"blocklike": 1, "late": 135297, "phantom": store.DormantHeight}
	if len(m) != len(want) {
		t.Fatalf("got %v, want %v", m, want)
	}
	for k, v := range want {
		if m[k] != v {
			t.Fatalf("m[%s] = %d, want %d", k, m[k], v)
		}
	}
}

func TestRegisterConsumerNormalizesVersion(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	// Underscored input is normalized to canonical dotted form at the boundary.
	if err := s.RegisterConsumer(ctx, "u", "v0_1_20"); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT first_valid_version FROM consumer_registry WHERE consumer_name='u'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "v0.1.20" {
		t.Fatalf("stored %q, want v0.1.20", stored)
	}
	// Garbage is rejected before it reaches the table.
	if err := s.RegisterConsumer(ctx, "bad", "not-a-version"); err == nil {
		t.Fatal("want error registering invalid version")
	}
}

func TestLegacyUnderscoredRowStillResolves(t *testing.T) {
	// Rows written before write-side normalization existed must keep working:
	// the read path normalizes again (versiongate.go).
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_, err := pg.Pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version) VALUES ('legacy', 'v0_1_0')`)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.FirstValidHeights(ctx, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	if m["legacy"] != 1 {
		t.Fatalf("legacy underscored row resolved to %d, want 1", m["legacy"])
	}
}

func mustRegister(t *testing.T, s *store.Store, name, v string) {
	t.Helper()
	if err := s.RegisterConsumer(context.Background(), name, v); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test -tags integration ./test/integration/ -run 'TestConsumerFirstValidHeight|TestFirstValidHeightsMap|TestRegisterConsumerNormalizes' -v
```

Expected: FAIL (undefined Store methods / normalization absent).

- [ ] **Step 3: Implement the Store methods**

Append to `internal/store/versiongate.go`:

```go
// upgradeHeightsByVersion loads the upgrades table keyed by normalized
// upgrade name (upgrades.name is the chain's dotted tag, e.g. "v0.1.20").
func (s *Store) upgradeHeightsByVersion(ctx context.Context) (map[string]int64, error) {
	ups, err := s.ListUpgrades(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]int64, len(ups))
	for _, u := range ups {
		n, err := protover.Normalize(u.Name)
		if err != nil {
			return nil, fmt.Errorf("upgrades row %q: %w", u.Name, err)
		}
		m[n] = u.AppliedAtHeight
	}
	return m, nil
}

// ConsumerFirstValidHeight resolves spec §4.10 consumer_first_valid_height
// for one version against this network (genesis + upgrades table). Returns
// DormantHeight when the version was never applied and is above genesis.
func (s *Store) ConsumerFirstValidHeight(ctx context.Context, firstValidVersion, genesisVersion string) (int64, error) {
	ups, err := s.upgradeHeightsByVersion(ctx)
	if err != nil {
		return 0, err
	}
	return firstValidHeight(firstValidVersion, genesisVersion, ups)
}

// FirstValidHeights resolves consumer_first_valid_height for every ACTIVE
// consumer. Computed at query time — no materialization (spec §4.10).
func (s *Store) FirstValidHeights(ctx context.Context, genesisVersion string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT consumer_name, first_valid_version FROM consumer_registry WHERE active = true`)
	if err != nil {
		return nil, fmt.Errorf("query active consumers: %w", err)
	}
	defer rows.Close()
	type rc struct{ name, version string }
	var cons []rc
	for rows.Next() {
		var c rc
		if err := rows.Scan(&c.name, &c.version); err != nil {
			return nil, fmt.Errorf("scan consumer: %w", err)
		}
		cons = append(cons, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ups, err := s.upgradeHeightsByVersion(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(cons))
	for _, c := range cons {
		h, err := firstValidHeight(c.version, genesisVersion, ups)
		if err != nil {
			return nil, fmt.Errorf("consumer %q: %w", c.name, err)
		}
		out[c.name] = h
	}
	return out, nil
}
```

The import block of `internal/store/versiongate.go` after this step MUST be:

```go
import (
	"context"
	"fmt"
	"math"

	"github.com/pokt-network/pocketscribe/internal/protover"
)
```

In `internal/store/consumer_registry.go:10` change `RegisterConsumer` to normalize first (keep the existing INSERT; reference current body at `internal/store/consumer_registry.go:10-20`). Note for the doc comment: rows written BEFORE this change may hold non-canonical spellings — that is safe because every read path (`FirstValidHeights` via `firstValidHeight`) normalizes again; write-side normalization is hygiene, not a correctness dependency:

```go
func (s *Store) RegisterConsumer(ctx context.Context, name, firstValidVersion string) error {
	v, err := protover.Normalize(firstValidVersion)
	if err != nil {
		return fmt.Errorf("register consumer %q: %w", name, err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name) DO NOTHING`,
		name, v)
	if err != nil {
		return fmt.Errorf("register consumer %q: %w", name, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test -tags integration ./test/integration/ -run 'TestConsumerFirstValidHeight|TestFirstValidHeightsMap|TestRegisterConsumerNormalizes' -v
```

Expected: PASS. Also run `go test ./internal/store/` (unit) — still PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/ test/integration/versiongate_test.go
git commit -m "feat(store): height resolution for consumers vs network (spec §4.10)

ConsumerFirstValidHeight + FirstValidHeights compute first-valid heights from
genesis_decoder_version and the upgrades table; RegisterConsumer normalizes
first_valid_version at the boundary."
```

---

### Task 4: `RequiredSet(H)` and `IsSealed(H)` become version-aware

**Files:**
- Modify: `internal/store/consumer_registry.go:37-60` (`RequiredSet`)
- Modify: `internal/store/seal.go` (`IsSealed`)
- Modify (signature updates, mechanical): every caller found by
  `grep -rn "RequiredSet(\|IsSealed(" --include="*_test.go" test/ internal/`
  — known: `test/integration/registry_test.go:45`, `test/integration/deregister_test.go:40`,
  `test/integration/store_error_paths_test.go:150`, `test/integration/seal_test.go` (helper
  `assertSealed` + all calls), plus any use inside `block_consumer_test.go`,
  `supplier_consumer_test.go`, `batch_runtime_crash_test.go`.
- Create: `test/integration/orchestration_test.go` (starts with spec test 23)

- [ ] **Step 1: Write the failing test (spec test 23)**

`test/integration/orchestration_test.go`:

```go
//go:build integration

package integration

import (
	"context"
	"slices"
	"testing"
)

// genesisV0_1_0 is the mainnet genesis decoder version, in decoder-dir
// spelling on purpose — exercises protover normalization at every call site.
const genesisV0_1_0 = "v0_1_0"

func requiredSet(t *testing.T, h int64, genesis string) []string {
	t.Helper()
	s := storeFrom(t)
	names, err := s.RequiredSet(context.Background(), h, genesis)
	if err != nil {
		t.Fatalf("RequiredSet(%d): %v", h, err)
	}
	return names
}

func TestDynamicRequiredSetPerHeight(t *testing.T) { // spec test 23 (§11.1)
	pg.Reset(t)
	s := storeFrom(t)
	mustRegister(t, s, "blocklike", "v0.1.0")
	mustRegister(t, s, "late", "v0.1.20")
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	// H < first_valid: the late consumer is NOT in required_set…
	if got := requiredSet(t, 135296, genesisV0_1_0); !slices.Equal(got, []string{"blocklike"}) {
		t.Fatalf("required_set(135296) = %v, want [blocklike]", got)
	}
	// …and H ≥ first_valid: it is.
	if got := requiredSet(t, 135297, genesisV0_1_0); !slices.Equal(got, []string{"blocklike", "late"}) {
		t.Fatalf("required_set(135297) = %v, want [blocklike late]", got)
	}

	// Sealing follows: H seals WITHOUT the late consumer below its first_valid…
	setConsolidation(t, "blocklike", 200000)
	assertSealed(t, s, 135296, genesisV0_1_0, true)
	// …but not at/after it until the late consumer catches up.
	assertSealed(t, s, 135297, genesisV0_1_0, false)
	setConsolidation(t, "late", 135297)
	assertSealed(t, s, 135297, genesisV0_1_0, true)
}
```

This test reuses `assertSealed` from `test/integration/seal_test.go` — Step 5 updates
that helper to the 4-arg form (DRY: ONE seal assertion helper in the package):

```go
func assertSealed(t *testing.T, s interface {
	IsSealed(context.Context, int64, string) (bool, error)
}, h int64, genesis string, want bool) {
	t.Helper()
	got, err := s.IsSealed(context.Background(), h, genesis)
	if err != nil {
		t.Fatalf("IsSealed(%d): %v", h, err)
	}
	if got != want {
		t.Fatalf("IsSealed(%d) = %v, want %v", h, got, want)
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test -tags integration ./test/integration/ -run TestDynamicRequiredSetPerHeight -v
```

Expected: FAIL (compile error: too many arguments to RequiredSet/IsSealed).

- [ ] **Step 3: Rewrite `RequiredSet`**

Replace `internal/store/consumer_registry.go:37-60` with:

```go
// RequiredSet returns the consumers whose sign-off height H must wait on:
// active consumers whose consumer_first_valid_height (spec §4.10) is <= H.
// genesisVersion is network.genesis_decoder_version from the network config.
// Sorted by name for deterministic output.
func (s *Store) RequiredSet(ctx context.Context, height int64, genesisVersion string) ([]string, error) {
	fvh, err := s.FirstValidHeights(ctx, genesisVersion)
	if err != nil {
		return nil, fmt.Errorf("required_set(%d): %w", height, err)
	}
	var names []string
	for name, h := range fvh {
		if h <= height {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}
```

The import block of `internal/store/consumer_registry.go` after this step MUST be:

```go
import (
	"context"
	"fmt"
	"sort"

	"github.com/pokt-network/pocketscribe/internal/protover"
)
```

(the old direct query is gone — `FirstValidHeights` owns the registry read. Cost note: RequiredSet/IsSealed now make 2 small queries (registry + upgrades) instead of 1 — deliberate, spec §4.10 mandates query-time derivation with no materialization in Slice 1; both tables are tiny.)

- [ ] **Step 4: Rewrite `IsSealed`**

Replace `internal/store/seal.go` body with:

```go
// IsSealed reports whether height is sealed: every consumer in
// required_set(height) has consolidated_up_to >= height, and the required set
// is non-empty. The non-empty guard is kept from Phase B deliberately (a
// height nobody is required to process must not read as "sealed" on a fresh
// database) — a divergence from a vacuous-truth reading of spec §4.10,
// matching spec tests 7/8 behavior. Derived at query time — no materialized
// seal row in Slice 1.
func (s *Store) IsSealed(ctx context.Context, height int64, genesisVersion string) (bool, error) {
	required, err := s.RequiredSet(ctx, height, genesisVersion)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	if len(required) == 0 {
		return false, nil
	}
	var lagging int
	err = s.pool.QueryRow(ctx,
		`SELECT count(*)
		 FROM unnest($1::text[]) AS r(consumer_name)
		 LEFT JOIN consumer_consolidation c USING (consumer_name)
		 WHERE COALESCE(c.consolidated_up_to, 0) < $2`,
		required, height).Scan(&lagging)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	return lagging == 0, nil
}
```

- [ ] **Step 5: Update every existing call site (mechanical)**

```bash
grep -rn "RequiredSet(\|IsSealed(" --include="*.go" internal/ test/ | grep -v versiongate
```

In `test/integration/seal_test.go` change the `assertSealed` helper to the 4-arg form shown in Step 1 (it is the package's ONE seal assertion helper — orchestration_test.go uses it too; do NOT introduce a second helper). Then update EVERY existing call site, passing the `genesisV0_1_0` constant (defined in orchestration_test.go, same package) — full enumeration, verified by grep during planning:

- `assertSealed` 3-arg → 4-arg calls: `seal_test.go:30,31,34,47,49,53`, `deregister_test.go:27,47`, `runtime_test.go:81,88`, `resilience_test.go:179,191`, `block_consumer_test.go:237`, `batch_runtime_crash_test.go:231`, `supplier_consumer_test.go:589,600`
- direct `RequiredSet(ctx, N)` calls: `registry_test.go:45`, `deregister_test.go:40`, `store_error_paths_test.go:150`

(Line numbers are pre-change references — re-grep `assertSealed(\|RequiredSet(\|IsSealed(` if drifted.) These consumers all register with `"v0.1.0"` and `genesisV0_1_0` normalizes to the same version, so behavior is unchanged. In `test/integration/store_error_paths_test.go:150` keep the cancelled-context error assertion (signature gains `genesisV0_1_0`), and ADD two real error paths:

```go
// invalid genesis version surfaces as an error, not a silent empty set
if _, err := s.RequiredSet(ctx, 1, "garbage"); err == nil {
	t.Fatal("want error for invalid genesis version")
}
if _, err := s.IsSealed(ctx, 1, "garbage"); err == nil {
	t.Fatal("want error for invalid genesis version")
}
```

- [ ] **Step 6: Run the full integration suite**

```bash
go test -tags integration ./test/integration/ -v -count=1 2>&1 | tail -40
```

Expected: ALL PASS including the new `TestDynamicRequiredSetPerHeight` and the pre-existing seal/registry/deregister tests (spec tests 7, 8, 9, 13 stay green — their consumers register with v0.1.0 ≤ genesis so behavior is unchanged).

- [ ] **Step 7: Commit**

```bash
git add internal/store/ test/integration/
git commit -m "feat(store): version-aware RequiredSet and IsSealed (spec test 23)

required_set(H) gates membership by consumer_first_valid_height; sealing at H
no longer waits on consumers not yet valid at H. Phase B non-empty guard kept."
```

---

### Task 5: Dormancy gate in Runtime + BatchRuntime (spec tests 22, 24)

**Files:**
- Create: `internal/consumer/dormancy.go`
- Modify: `internal/consumer/runtime.go:29-36` (Config), `:56-59` (Run gate)
- Modify: `internal/consumer/batch.go` (BatchConfig + same gate after `batch.go:61-65`)
- Modify: `test/integration/orchestration_test.go` (add tests 22, 24)

- [ ] **Step 1: Write the failing tests (spec tests 22 + 24)**

Append to `test/integration/orchestration_test.go` (the JetStream-consumer helper mirrors `internal/app/consumer/block.go:62-69`; check `test/integration/fileplugin_test.go:28` for the harness `nats.Client` spelling and reuse it):

```go
func orchJSConsumer(t *testing.T, durable string) jetstream.Consumer {
	t.Helper()
	ctx := context.Background()
	stream, err := nats.Client.EnsureStream(ctx, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	c, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: natsx.BlockSubjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    -1,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	return c
}

func newOrchRuntime(t *testing.T, s *store.Store, id, firstValid, genesis string) *runtime.Runtime {
	t.Helper()
	return runtime.NewRuntime(runtime.Config{
		Handler:        runtime.NewNoOpHandler(id, firstValid),
		Store:          s,
		Consumer:       orchJSConsumer(t, id),
		Logger:         slog.Default(),
		Metrics:        metrics.NewConsumer(prometheus.NewRegistry()),
		GenesisVersion: genesis,
	})
}

func TestDormantConsumer(t *testing.T) { // spec test 22 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	mustRegister(t, s, "blocklike", "v0.1.0")

	// Fictitious consumer: v0.2.0 was never applied on this network.
	rt := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rt.Run(runCtx); err != nil {
		t.Fatalf("dormant consumer must exit cleanly, got %v", err)
	}
	if runCtx.Err() != nil {
		t.Fatal("Run consumed until timeout — dormancy gate did not fire")
	}

	// It registered…
	var active bool
	if err := pg.Pool.QueryRow(ctx,
		`SELECT active FROM consumer_registry WHERE consumer_name='phantom'`).Scan(&active); err != nil {
		t.Fatalf("phantom not registered: %v", err)
	}
	if !active {
		t.Fatal("phantom must register active (dormancy is height-derived, not a flag)")
	}
	// …but affects no height's required_set.
	for _, h := range []int64{1, 1_000_000} {
		if got := requiredSet(t, h, genesisV0_1_0); slices.Contains(got, "phantom") {
			t.Fatalf("required_set(%d) contains dormant phantom: %v", h, got)
		}
	}
}

func TestConsumerWakeup(t *testing.T) { // spec test 24 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// CAUTION: Runtime.Run returns nil BOTH on dormancy and on clean ctx
	// cancellation (internal/consumer/runtime.go:78) — dormant vs awake is
	// distinguished by ELAPSED TIME, not by the returned error.

	// Run 1: dormant — must return well before the deadline.
	rt := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	run1Ctx, cancel1 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel1()
	if err := rt.Run(run1Ctx); err != nil {
		t.Fatalf("run 1 (dormant): %v", err)
	}
	if run1Ctx.Err() != nil {
		t.Fatal("run 1 consumed until the deadline — dormancy gate did not fire")
	}

	// sync-upgrades lands the new version (different router/upgrades state
	// between runs, per the spec test note).
	seedUpgrade(t, s, "v0.2.0", 500000, "v0_2_0")

	// required_set flips exactly at the applied height.
	if got := requiredSet(t, 499999, genesisV0_1_0); slices.Contains(got, "phantom") {
		t.Fatalf("phantom required before first_valid: %v", got)
	}
	if got := requiredSet(t, 500000, genesisV0_1_0); !slices.Contains(got, "phantom") {
		t.Fatalf("phantom missing from required_set(500000): %v", got)
	}

	// Run 2: awake — the gate passes and the runtime consumes (idle) until
	// the deadline. Awake ⇒ Run occupies (nearly) the whole window.
	rt2 := newOrchRuntime(t, s, "phantom", "v0.2.0", genesisV0_1_0)
	const window = 3 * time.Second
	run2Ctx, cancel2 := context.WithTimeout(ctx, window)
	defer cancel2()
	start := time.Now()
	if err := rt2.Run(run2Ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run 2 (awake): unexpected error %v", err)
	}
	// Generous margin: an awake Run cannot return until the ctx fires (it only
	// returns early on dormancy or a non-ctx error, both caught above), so any
	// elapsed >= window-1s proves the gate passed; a dormant exit takes ~ms.
	if elapsed := time.Since(start); elapsed < window-time.Second {
		t.Fatalf("run 2 returned after %v — consumer did not wake (exited as dormant)", elapsed)
	}
}
```

Add the needed imports (`errors`, `slices`, `time`, `slog`, `jetstream`, `prometheus`, `runtime "github.com/pokt-network/pocketscribe/internal/consumer"`, `"github.com/pokt-network/pocketscribe/internal/metrics"`, `natsx "github.com/pokt-network/pocketscribe/internal/nats"`).

- [ ] **Step 2: Run to verify failure**

```bash
go test -tags integration ./test/integration/ -run 'TestDormantConsumer|TestConsumerWakeup' -v
```

Expected: FAIL (Config has no field GenesisVersion).

- [ ] **Step 3: Implement the shared gate**

`internal/consumer/dormancy.go`:

```go
package consumer

import (
	"context"
	"log/slog"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// dormant reports whether a consumer is dormant on this network (spec §4.10:
// first_valid_version above genesis and never applied → INFINITY). An empty
// genesisVersion disables the gate — network-agnostic callers (and pre-Phase-F
// tests) keep their behavior.
//
// Dormancy is resolved ONCE at startup: wakeup is restart-based (spec test
// 24). Operationally: after `ps sync-upgrades` lands a version that wakes a
// dormant consumer, that consumer must be (re)started to begin consuming.
func dormant(ctx context.Context, st *store.Store, id, firstValid, genesisVersion string, logger *slog.Logger) (bool, error) {
	if genesisVersion == "" {
		return false, nil
	}
	h, err := st.ConsumerFirstValidHeight(ctx, firstValid, genesisVersion)
	if err != nil {
		return false, err
	}
	if h != store.DormantHeight {
		return false, nil
	}
	logger.Info("consumer dormant on this network; exiting cleanly",
		"consumer", id,
		"first_valid_version", firstValid,
		"genesis_decoder_version", genesisVersion)
	return true, nil
}
```

In `internal/consumer/runtime.go`: add `GenesisVersion string` to `Config` (after `Metrics`, with comment `// network.genesis_decoder_version; empty disables the dormancy gate`), add `genesisVersion string` field to `Runtime`, set it in `NewRuntime`, and insert after the `RegisterConsumer` call at `runtime.go:57-59`:

```go
	if d, err := dormant(ctx, r.store, r.handler.ID(), r.handler.FirstValidVersion(), r.genesisVersion, r.logger); err != nil {
		return err
	} else if d {
		return nil
	}
```

Apply the identical change to `internal/consumer/batch.go`: `BatchConfig` gains `GenesisVersion string`, `BatchRuntime` gains the field, and the same 5 lines go right after the `RegisterConsumer` call at `batch.go:61-65`.

- [ ] **Step 4: Run tests**

```bash
go test -tags integration ./test/integration/ -run 'TestDormantConsumer|TestConsumerWakeup' -v
go test ./internal/consumer/
```

Expected: PASS (both new tests; existing unit tests unaffected — gate disabled with empty GenesisVersion).

- [ ] **Step 5: Commit**

```bash
git add internal/consumer/ test/integration/orchestration_test.go
git commit -m "feat(consumer): dormancy gate at startup (spec tests 22, 24)

A consumer whose first_valid_version resolves to DormantHeight registers,
logs, and exits cleanly; it wakes on restart once sync-upgrades lands the
version. Empty GenesisVersion disables the gate (network-agnostic callers)."
```

---

### Task 6: Wire the gate into `ps consumer supplier`

**Files:**
- Modify: `internal/app/consumer/supplier.go` (BatchConfig literal, see `:83-101`)

- [ ] **Step 1: Add the field**

In the `runtime.NewBatchRuntime(runtime.BatchConfig{...})` literal in `internal/app/consumer/supplier.go` (around `:95`), add:

```go
				GenesisVersion: cfg.Network.GenesisDecoderVersion,
```

(`cfg` is already loaded at `supplier.go:34` for the router.) `ps consumer block` (`internal/app/consumer/block.go`) deliberately does NOT get the gate: it takes no network config flag today, and its handler's FirstValidVersion is "v0.1.0" — never dormant on any network whose genesis is ≥ v0.1.0. Document that with a one-line comment in `block.go` next to `runtime.Config{`:

```go
				// No GenesisVersion: block (v0.1.0) can never be dormant; the
				// block consumer stays network-config-free (see Phase F plan).
```

- [ ] **Step 2: Build + run unit tests**

```bash
go build ./... && go test ./internal/... 
```

Expected: clean build, PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/app/consumer/
git commit -m "feat(app): supplier consumer passes genesis_decoder_version to the dormancy gate"
```

---

### Task 7: Multi-network + backfill semantics (spec tests 25, 26)

**Files:**
- Modify: `test/integration/orchestration_test.go` (two new tests)

- [ ] **Step 1: Write the failing tests**

Append to `test/integration/orchestration_test.go`:

```go
func TestMultiNetworkRequiredSet(t *testing.T) { // spec test 25 (§11.1)
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// The REAL network configs — if their genesis versions drift this test
	// must fail loud, not silently keep passing.
	mainnet, err := config.Load("../../configs/networks/mainnet.yaml")
	if err != nil {
		t.Fatalf("load mainnet.yaml: %v", err)
	}
	localnet, err := config.Load("../../configs/networks/localnet.yaml")
	if err != nil {
		t.Fatalf("load localnet.yaml: %v", err)
	}
	if mainnet.Network.GenesisDecoderVersion == localnet.Network.GenesisDecoderVersion {
		t.Fatal("test premise broken: mainnet and localnet genesis versions are equal")
	}

	// Same consumer code, same registration…
	mustRegister(t, s, "midver", "v0.1.20")
	// …mainnet state: v0.1.20 applied at 135297 (what ps sync-upgrades writes).
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	// Mainnet (genesis v0_1_0): valid only from the upgrade height.
	h, err := s.ConsumerFirstValidHeight(ctx, "v0.1.20", mainnet.Network.GenesisDecoderVersion)
	if err != nil || h != 135297 {
		t.Fatalf("mainnet first_valid = %d, %v; want 135297", h, err)
	}
	if got := requiredSet(t, 1, mainnet.Network.GenesisDecoderVersion); slices.Contains(got, "midver") {
		t.Fatalf("mainnet required_set(1) must exclude midver: %v", got)
	}

	// Localnet (genesis v0_1_33 ≥ v0.1.20): valid from height 1, no upgrade row needed.
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.1.20", localnet.Network.GenesisDecoderVersion)
	if err != nil || h != 1 {
		t.Fatalf("localnet first_valid = %d, %v; want 1", h, err)
	}
	if got := requiredSet(t, 1, localnet.Network.GenesisDecoderVersion); !slices.Contains(got, "midver") {
		t.Fatalf("localnet required_set(1) must include midver: %v", got)
	}
}

func TestBackfillSemantics(t *testing.T) { // spec test 26 (§11.1)
	pg.Reset(t)
	s := storeFrom(t)

	// Established network state: one consumer, consolidated far ahead.
	mustRegister(t, s, "blocklike", "v0.1.0")
	setConsolidation(t, "blocklike", 150000)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")
	assertSealed(t, s, 150000, genesisV0_1_0, true)

	// A consumer added "after the fact": its duty starts at first_valid_height
	// (135297) — it has no consolidation row yet (cursor effectively starts there).
	mustRegister(t, s, "late", "v0.1.20")

	// Seals before its first_valid are unaffected…
	assertSealed(t, s, 135296, genesisV0_1_0, true)
	// …seals at/after pause until the backfill catches up…
	assertSealed(t, s, 135297, genesisV0_1_0, false)
	assertSealed(t, s, 150000, genesisV0_1_0, false)
	// …and resume exactly as far as the late consumer has consolidated.
	setConsolidation(t, "late", 140000)
	assertSealed(t, s, 140000, genesisV0_1_0, true)
	assertSealed(t, s, 150000, genesisV0_1_0, false)
	setConsolidation(t, "late", 150000)
	assertSealed(t, s, 150000, genesisV0_1_0, true)
}
```

Add import `"github.com/pokt-network/pocketscribe/internal/config"`.

- [ ] **Step 2: Run to verify they fail / pass for the right reason**

```bash
go test -tags integration ./test/integration/ -run 'TestMultiNetwork|TestBackfill' -v
```

Expected: PASS immediately (the store logic from Tasks 3–4 already implements this). If either FAILS, the store logic is wrong — fix THERE, do not bend the test. These two tests are behavioral specs, written after the code exists but asserting independent scenarios.

- [ ] **Step 3: Commit**

```bash
git add test/integration/orchestration_test.go
git commit -m "test(integration): multi-network required_set and backfill seal semantics (spec tests 25, 26)"
```

---

### Task 8: Sidecar payload caps (spec test 27)

**Files:**
- Modify: `internal/metrics/metrics.go` (add `FilePlugin` struct + constructor)
- Create: `internal/fileplugin/sizecap.go`
- Create: `internal/fileplugin/sizecap_test.go` (unit — no containers)
- Modify: `internal/fileplugin/bootstrap.go:31,56-59` (signature + wrap publish)
- Modify callers: `internal/app/fileplugin/cmd.go:48`, `test/integration/block_consumer_test.go:103`, `test/integration/fileplugin_test.go:28,145,165,193` (these 6 are ALL the call sites)

- [ ] **Step 1: Write the failing unit test**

`internal/fileplugin/sizecap_test.go`:

```go
package fileplugin

import (
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCapPublishPolicy(t *testing.T) { // spec test 27 (§11.1)
	reg := prometheus.NewRegistry()
	fpm := metrics.NewFilePlugin(reg)
	var published int
	rec := func(_ string, _ []byte, _ string) error { published++; return nil }
	p := capPublish(rec, discardLogger(), fpm)

	// Small payload: published, no counters.
	if err := p("pokt.tx.1.0", make([]byte, 1024), "a"); err != nil {
		t.Fatal(err)
	}
	// Exactly AT the soft cap: still silent (cap is exclusive).
	if err := p("pokt.tx.1.1", make([]byte, SoftCapBytes), "b"); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(fpm.OversizeSoft); got != 0 {
		t.Fatalf("soft counter = %v after at-cap payload, want 0", got)
	}
	// Above soft cap: WARN + counter, still published.
	if err := p("pokt.tx.1.2", make([]byte, SoftCapBytes+1), "c"); err != nil {
		t.Fatalf("soft-cap payload must still publish: %v", err)
	}
	if got := testutil.ToFloat64(fpm.OversizeSoft); got != 1 {
		t.Fatalf("soft counter = %v, want 1", got)
	}
	// Above hard cap: refused with error, counter, NOT published.
	before := published
	if err := p("pokt.tx.1.3", make([]byte, HardCapBytes+1), "d"); err == nil {
		t.Fatal("hard-cap payload must be refused")
	}
	if published != before {
		t.Fatal("hard-cap payload must not reach the inner publish")
	}
	if got := testutil.ToFloat64(fpm.OversizeRefused); got != 1 {
		t.Fatalf("refused counter = %v, want 1", got)
	}
}

func TestCapPublishNilMetrics(t *testing.T) {
	// Tests pass nil metrics — the wrapper must not panic.
	p := capPublish(func(_ string, _ []byte, _ string) error { return nil }, discardLogger(), nil)
	if err := p("s", make([]byte, SoftCapBytes+1), "m"); err != nil {
		t.Fatal(err)
	}
	if err := p("s", make([]byte, HardCapBytes+1), "m"); err == nil {
		t.Fatal("want refusal")
	}
}
```

- [ ] **Step 2: Run to verify failure**

```bash
go test ./internal/fileplugin/ -run TestCapPublish
```

Expected: FAIL (undefined capPublish, SoftCapBytes, metrics.NewFilePlugin).

- [ ] **Step 3: Implement metrics + wrapper**

Append to `internal/metrics/metrics.go` (mirror the `Consumer` pattern at `metrics.go:8-40`):

```go
// FilePlugin holds the metrics emitted by the fileplugin sidecar.
type FilePlugin struct {
	OversizeSoft    prometheus.Counter // payloads above the 256 KiB soft cap (still published)
	OversizeRefused prometheus.Counter // payloads above the 1 MiB hard cap (refused)
}

// NewFilePlugin constructs and registers the sidecar metric set on reg.
func NewFilePlugin(reg prometheus.Registerer) *FilePlugin {
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "fileplugin", Name: name, Help: help,
		})
		reg.MustRegister(c)
		return c
	}
	return &FilePlugin{
		OversizeSoft:    counter("oversize_soft_total", "Payloads above the 256 KiB soft cap (published anyway)."),
		OversizeRefused: counter("oversize_refused_total", "Payloads above the 1 MiB hard cap (refused at the source)."),
	}
}
```

`internal/fileplugin/sizecap.go`:

```go
package fileplugin

import (
	"fmt"
	"log/slog"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

const (
	// SoftCapBytes — payloads above this are logged (WARN) but still
	// published (spec §11.1 test 27).
	SoftCapBytes = 256 << 10
	// HardCapBytes — payloads above this are REFUSED: the NATS server's
	// default max_payload is 1 MiB, so the publish would fail server-side
	// anyway; refusing at the source keeps the failure explicit and the
	// height un-acked (no silent message loss).
	HardCapBytes = 1 << 20
)

type publishFn func(subj string, data []byte, msgID string) error

// capPublish decorates publish with the payload size policy. The returned
// error on a hard-cap violation aborts the whole height (Bootstrap stops at
// that height) — the sidecar must never skip a message inside a block.
// fpm may be nil (tests without a registry).
func capPublish(publish publishFn, logger *slog.Logger, fpm *metrics.FilePlugin) publishFn {
	return func(subj string, data []byte, msgID string) error {
		switch {
		case len(data) > HardCapBytes:
			if fpm != nil {
				fpm.OversizeRefused.Inc()
			}
			logger.Error("payload exceeds 1 MiB hard cap; refusing to publish",
				"subject", subj, "bytes", len(data), "msg_id", msgID)
			return fmt.Errorf("publish %s: payload %d bytes exceeds %d-byte hard cap", subj, len(data), HardCapBytes)
		case len(data) > SoftCapBytes:
			if fpm != nil {
				fpm.OversizeSoft.Inc()
			}
			logger.Warn("payload exceeds 256 KiB soft cap",
				"subject", subj, "bytes", len(data), "msg_id", msgID)
		}
		return publish(subj, data, msgID)
	}
}
```

- [ ] **Step 4: Wire into Bootstrap**

In `internal/fileplugin/bootstrap.go:31` extend the signature:

```go
func Bootstrap(ctx context.Context, client *natsx.Client, dir string, maxHeight int64, chainID string, fpm *metrics.FilePlugin) (int, int, error) {
```

and wrap the closure at `bootstrap.go:56-59`:

```go
	js := client.JetStream()
	publish := capPublish(func(subj string, data []byte, msgID string) error {
		_, err := js.Publish(ctx, subj, data, jetstream.WithMsgID(msgID))
		return err
	}, slog.Default(), fpm)
```

Update the doc comment to mention the caps. Update the EXACT 6 call sites (verified by grep during planning; `supplier_consumer_test.go` has NO Bootstrap call): production `internal/app/fileplugin/cmd.go:48` passes `metrics.NewFilePlugin(prometheus.DefaultRegisterer)` (construct it once above the call); test callers `test/integration/fileplugin_test.go:28,145,165,193` and `test/integration/block_consumer_test.go:103` pass `nil`.

- [ ] **Step 5: End-to-end-ish unit check through fanOutHeight**

Append to `internal/fileplugin/sizecap_test.go` a test that drives the real fan-out path (`fanOutHeight` at `bootstrap.go:74` takes the injected publish — no NATS needed). REUSE the existing same-package helpers `buildThreeRecordMetaWithPayloads(t, rec0, rec1 []byte) []byte` (`internal/fileplugin/bootstrap_test.go:423` — uvarint-framed records + trailing 0-byte record) and `mustMarshalResp` (`bootstrap_test.go:438`):

```go
func TestFanOutHeightRefusesOversizeTx(t *testing.T) { // spec test 27, fan-out path
	dir := t.TempDir()
	// RequestFinalizeBlock with a 1 MiB+1 tx; minimal valid header fields.
	req := &abci.RequestFinalizeBlock{
		Height: 42,
		Time:   time.Date(2025, 6, 17, 0, 0, 0, 0, time.UTC),
		Hash:   make([]byte, 32),
		Txs:    [][]byte{make([]byte, HardCapBytes+1)},
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	resp := &abci.ResponseFinalizeBlock{TxResults: []*abci.ExecTxResult{{Code: 0}}}
	meta := buildThreeRecordMetaWithPayloads(t, reqBytes, mustMarshalResp(t, resp))
	metaPath := filepath.Join(dir, "block-42-meta")
	if err := os.WriteFile(metaPath, meta, 0o644); err != nil {
		t.Fatal(err)
	}
	p := capPublish(func(_ string, _ []byte, _ string) error { return nil }, discardLogger(), nil)
	if _, err := fanOutHeight(context.Background(), p, 42, metaPath, "pocket"); err == nil {
		t.Fatal("oversize tx must abort the height")
	}
}
```

Imports for this test: `abci "github.com/cometbft/cometbft/abci/types"`, `context`, `os`, `path/filepath`, `time`. If `fanOutHeight` errors BEFORE reaching the publish (e.g. it also reads a `block-42-data` file), create an empty data file too — check `fanOutHeight`'s body (`bootstrap.go:74-188`) and satisfy its preconditions; the assertion that matters is that the error comes from the cap (`strings.Contains(err.Error(), "hard cap")` is an acceptable sharpening).

- [ ] **Step 6: Run everything**

```bash
go test ./internal/fileplugin/ ./internal/metrics/ -v
go test -tags integration ./test/integration/ -count=1 2>&1 | tail -20
```

Expected: PASS (integration suite recompiles with the new Bootstrap signature).

- [ ] **Step 7: Commit**

```bash
git add internal/fileplugin/ internal/metrics/ internal/app/fileplugin/ test/integration/
git commit -m "feat(fileplugin): 256 KiB soft / 1 MiB hard payload caps (spec test 27)

Soft cap warns and counts; hard cap refuses at the source (NATS max_payload
would reject it anyway) and aborts the height so no message is skipped."
```

---

### Task 9: Part 1 gate — full CI + integration + lint with tags

- [ ] **Step 1: Run the full gauntlet**

```bash
make ci
golangci-lint run --build-tags=integration ./...
go test -tags integration ./test/integration/ -count=1
go test ./internal/... -coverprofile=/tmp/cover-f1.out && go tool cover -func=/tmp/cover-f1.out | tail -5
```

Expected: all green; total `internal/` coverage ≥90%; `internal/protover` 100%. If `make ci` includes `gen-check`, it must stay green (no generated code touched in Part 1).

- [ ] **Step 2: Commit any straggler fixes**

```bash
git status --short   # fix lint findings if any, then:
git add -A && git commit -m "chore: phase F part 1 gate — lint+integration green" || echo "nothing to commit"
```

---

## Part 2 — Multi-version expansion (24 versions, spec-literal fixtures)

### Reference: chain-authoritative version map (from bucket `versions.yaml`)

| Version | applied_height | runs_until | fileplugin tarball (in `/tmp`) | decoder era (expected `DecoderFor`) | fixture dir |
|---|---|---|---|---|---|
| v0.1.2 | 78621 | 78631 | `v0.1.2-h78631-fileplugin.tar.xz` | v0_1_0 | `test/fixtures/v0_1_0/` |
| v0.1.3 | 78632 | 78640 | `v0.1.3-h78640-…` | v0_1_0 | `v0_1_0/` |
| v0.1.4 | 78641 | 78653 | `v0.1.4-h78653-…` | v0_1_0 | `v0_1_0/` |
| v0.1.5 | 78654 | 78658 | `v0.1.5-h78658-…` | v0_1_0 | `v0_1_0/` |
| v0.1.6 | 78659 | 78664 | `v0.1.6-h78664-…` | v0_1_0 | `v0_1_0/` |
| v0.1.7 | 78665 | 78670 | `v0.1.7-h78670-…` | v0_1_0 | `v0_1_0/` |
| v0.1.8 | 78671 | 78677 | `v0.1.8-h78677-…` | v0_1_8 | `v0_1_8/` (new dir) |
| v0.1.9 | 78678 | 78682 | `v0.1.9-h78682-…` | v0_1_8 | `v0_1_8/` |
| v0.1.11 | 78689 | 78696 | `v0.1.11-h78696-…` | v0_1_10 | `v0_1_10/` |
| v0.1.12 | 78697 | 80509 | `v0.1.12-h80509-…` | v0_1_10 | `v0_1_10/` |
| v0.1.13 | 80510 | 93824 | `v0.1.13-h93824-…` | v0_1_10 | `v0_1_10/` |
| v0.1.14 | 93825 | 94369 | `v0.1.14-h94369-…` | v0_1_10 | `v0_1_10/` |
| v0.1.15 | 94370 | 99292 | `v0.1.15-h99292-…` | v0_1_10 | `v0_1_10/` |
| v0.1.16 | 99293 | 102141 | `v0.1.16-h102141-…` | v0_1_10 | `v0_1_10/` |
| v0.1.17 | 102142 | 116099 | `v0.1.17-h116099-…` | v0_1_10 | `v0_1_10/` |
| v0.1.18 | 116100 | 117453 | `v0.1.18-h117453-…` | v0_1_10 | `v0_1_10/` |
| v0.1.19 | 117454 | 135296 | `v0.1.19-h135296-…` | v0_1_10 | `v0_1_10/` |
| v0.1.21 | 138931 | 155172 | `v0.1.21-h155172-…` | v0_1_20 | `v0_1_20/` |
| v0.1.22 | 155173 | 161108 | `v0.1.22-h161108-…` | v0_1_20 | `v0_1_20/` |
| v0.1.23 | 161109 | 161168 | `v0.1.23-h161168-…` | v0_1_20 | `v0_1_20/` |
| v0.1.24 | 161169 | 190973 | `v0.1.24-h190973-…` | v0_1_20 | `v0_1_20/` |
| v0.1.25 | 190974 | 190978 | `v0.1.25-h190978-…` | v0_1_20 | `v0_1_20/` |
| v0.1.26 | 190979 | 247892 | `v0.1.26-h247892-…` | v0_1_20 | `v0_1_20/` |
| v0.1.27 | 247893 | 287931 | `v0.1.27-h287931-…` | v0_1_27 | `v0_1_27/` (new dir) |

Already covered (Phase D/E): v0.1.0, v0.1.10, v0.1.20, v0.1.28, v0.1.29. Not archivable yet: v0.1.30, v0.1.31, v0.1.33 (Task 17). Never applied on mainnet: v0.1.1, v0.1.32 (excluded by definition).

Fixture dirs follow the EXISTING convention: directory = decoder era that must decode the block (precedent: `test/fixtures/v0_1_10/block-102542-*` is a v0.1.16-era block, see `test/integration/supplier_consumer_test.go:306` comment). Provenance lives in the README matrix (Task 16).

**Selection criteria per version (spec §8.1, ~3 fixtures each):**
1. The boundary block (`applied_height` exactly — first block under the new binary).
2. The highest-supplier-activity block in the era (msg_stake > 0, or events > 0, or supplier KV > 0 — prefer one with all three). If the era has ZERO supplier activity (likely for the 5–13-block eras), pick a tx-bearing block instead and note it in the README (negative fixture, precedent: v0_1_0).
3. A quiet block (tx_count == 0, no supplier KV) — exercises the quiet-height path.

---

### Task 10: Promote the report pipeline to `internal/fixturereport`

**Files:**
- Create: `internal/fixturereport/doc.go`, `internal/fixturereport/report.go`, `internal/fixturereport/mainnet.go`
- Create: `internal/fixturereport/report_test.go`
- Modify: `tools/fixtureextract/main.go` (becomes a thin wrapper; delete the moved logic)

- [ ] **Step 1: Write the failing test**

`internal/fixturereport/report_test.go` — drives the package against the EXISTING fixtures (golden source of truth):

```go
package fixturereport

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/router"
)

func mustRouter(t *testing.T) router.Router {
	t.Helper()
	r, err := router.NewStaticRouter(MainnetUpgrades(), router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReportMatchesExistingExpected(t *testing.T) {
	r := mustRouter(t)
	// One quiet block and one busy supplier block from the existing corpus.
	for _, fx := range []struct{ dir string; height int64 }{
		{"../../test/fixtures/v0_1_20", 135297},
		{"../../test/fixtures/v0_1_28", 290584},
	} {
		meta, err := os.ReadFile(fxPath(fx.dir, fx.height, "meta"))
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fxPath(fx.dir, fx.height, "data"))
		if err != nil {
			t.Fatal(err)
		}
		got, err := Report(r, meta, data)
		if err != nil {
			t.Fatalf("Report(%d): %v", fx.height, err)
		}
		raw, err := os.ReadFile(fxPath(fx.dir, fx.height, "expected.json"))
		if err != nil {
			t.Fatal(err)
		}
		var want Result
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("Report(%d) mismatch:\ngot  %+v\nwant %+v", fx.height, *got, want)
		}
	}
}

func TestReportErrorPaths(t *testing.T) {
	r := mustRouter(t)
	if _, err := Report(r, []byte("garbage"), nil); err == nil {
		t.Fatal("corrupt meta must error")
	}
	if _, err := Report(r, nil, nil); err == nil {
		t.Fatal("empty meta must error")
	}
}

func TestMainnetUpgradesComplete(t *testing.T) {
	ups := MainnetUpgrades()
	if len(ups) != 31 {
		t.Fatalf("MainnetUpgrades has %d entries, want 31 (v0.1.2..v0.1.31, v0.1.33; v0.1.1 and v0.1.32 never applied)", len(ups))
	}
	// Heights strictly increasing (sanity vs versions.yaml transcription).
	for i := 1; i < len(ups); i++ {
		if ups[i].AppliedAtHeight <= ups[i-1].AppliedAtHeight {
			t.Fatalf("non-monotonic heights at %s", ups[i].Name)
		}
	}
}
```

with helper `func fxPath(dir string, h int64, suffix string) string` (fmt.Sprintf `%s/block-%d-%s`).

- [ ] **Step 2: Run to verify failure**

```bash
go test ./internal/fixturereport/
```

Expected: FAIL (package missing).

- [ ] **Step 3: Implement**

`internal/fixturereport/doc.go`:

```go
// Package fixturereport decodes a captured FilePlugin fixture (the
// block-{H}-meta / block-{H}-data pair) through the version router and
// summarizes block + supplier activity. It is the SINGLE source for fixture
// expected.json generation (tools/fixtureextract) and verification
// (golden_walk_test.go) — generator and checker can never drift.
package fixturereport
```

`internal/fixturereport/report.go`: port `runReport` from `tools/fixtureextract/main.go:121-283` VERBATIM in behavior, with these mechanical changes:
- Exported types replacing the tool's locals — keep the json tags IDENTICAL (existing expected.json files must keep parsing):

```go
type MsgStakeResult struct {
	TxIndex         int    `json:"tx_index"`
	OperatorAddress string `json:"operator_address"`
	StakeAmount     int64  `json:"stake_amount"`
	StakeDenom      string `json:"stake_denom"`
}

type EventStakedResult struct {
	TxIndex          int   `json:"tx_index"`
	SessionEndHeight int64 `json:"session_end_height"`
}

type SupplierResult struct {
	MsgStake         []MsgStakeResult    `json:"msg_stake,omitempty"`
	EventsStaked     []EventStakedResult `json:"events_staked,omitempty"`
	HistoryOperators []string            `json:"history_operators,omitempty"`
	SCURowsMin       int                 `json:"scu_rows_min"`
}

type Result struct {
	Height          int64           `json:"height"`
	Time            string          `json:"time"`
	Hash            string          `json:"hash"`
	ProposerAddress string          `json:"proposer_address"`
	TxCount         int             `json:"tx_count"`
	Supplier        *SupplierResult `json:"supplier,omitempty"`
}

// Report decodes one captured block. The height is taken from the decoded
// header (the caller's filename is not trusted). Per-item decode failures
// inside txs/events/KV are skipped (same behavior the curation tool always
// had); structural failures (header, meta framing) return an error.
func Report(r router.Router, metaBytes, dataBytes []byte) (*Result, error)
```

- `fmt.Fprintf(os.Stderr, …)` + `os.Exit(1)` on structural failures become returned errors; the per-item `continue` paths stay `continue` (drop the stderr noise — determinism for the golden walker).
- Height for `r.DecoderFor` comes from `header.Height` (was the CLI arg).

`internal/fixturereport/mainnet.go` — the full table transcribed from `/tmp/pocketscribe-discovery/versions.yaml` (chain-authoritative, verified vs Sauron LCD 2026-05-22; also keep a copy of that yaml as the comment's cited source):

```go
// MainnetUpgrades is the chain-authoritative mainnet upgrade table (source:
// pocketscribe-mainnet-archeology bucket versions.yaml, verified against the
// Sauron LCD applied_plan endpoints 2026-05-22). DecoderVersion is the
// uniform decoder-dir spelling of the tag; the router's lenient fallback maps
// unregistered versions to the nearest earlier registered decoder, which the
// break map (docs/research/supplier-shape-breaks.md) proves shape-safe.
// v0.1.1 and v0.1.32 were never applied on mainnet and are deliberately absent.
func MainnetUpgrades() []router.Upgrade {
	return []router.Upgrade{
		{Name: "v0.1.2", AppliedAtHeight: 78621, DecoderVersion: "v0_1_2"},
		{Name: "v0.1.3", AppliedAtHeight: 78632, DecoderVersion: "v0_1_3"},
		{Name: "v0.1.4", AppliedAtHeight: 78641, DecoderVersion: "v0_1_4"},
		{Name: "v0.1.5", AppliedAtHeight: 78654, DecoderVersion: "v0_1_5"},
		{Name: "v0.1.6", AppliedAtHeight: 78659, DecoderVersion: "v0_1_6"},
		{Name: "v0.1.7", AppliedAtHeight: 78665, DecoderVersion: "v0_1_7"},
		{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
		{Name: "v0.1.9", AppliedAtHeight: 78678, DecoderVersion: "v0_1_9"},
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.11", AppliedAtHeight: 78689, DecoderVersion: "v0_1_11"},
		{Name: "v0.1.12", AppliedAtHeight: 78697, DecoderVersion: "v0_1_12"},
		{Name: "v0.1.13", AppliedAtHeight: 80510, DecoderVersion: "v0_1_13"},
		{Name: "v0.1.14", AppliedAtHeight: 93825, DecoderVersion: "v0_1_14"},
		{Name: "v0.1.15", AppliedAtHeight: 94370, DecoderVersion: "v0_1_15"},
		{Name: "v0.1.16", AppliedAtHeight: 99293, DecoderVersion: "v0_1_16"},
		{Name: "v0.1.17", AppliedAtHeight: 102142, DecoderVersion: "v0_1_17"},
		{Name: "v0.1.18", AppliedAtHeight: 116100, DecoderVersion: "v0_1_18"},
		{Name: "v0.1.19", AppliedAtHeight: 117454, DecoderVersion: "v0_1_19"},
		{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
		{Name: "v0.1.21", AppliedAtHeight: 138931, DecoderVersion: "v0_1_21"},
		{Name: "v0.1.22", AppliedAtHeight: 155173, DecoderVersion: "v0_1_22"},
		{Name: "v0.1.23", AppliedAtHeight: 161109, DecoderVersion: "v0_1_23"},
		{Name: "v0.1.24", AppliedAtHeight: 161169, DecoderVersion: "v0_1_24"},
		{Name: "v0.1.25", AppliedAtHeight: 190974, DecoderVersion: "v0_1_25"},
		{Name: "v0.1.26", AppliedAtHeight: 190979, DecoderVersion: "v0_1_26"},
		{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
		{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
		{Name: "v0.1.30", AppliedAtHeight: 484473, DecoderVersion: "v0_1_30"},
		{Name: "v0.1.31", AppliedAtHeight: 635506, DecoderVersion: "v0_1_31"},
		{Name: "v0.1.33", AppliedAtHeight: 703870, DecoderVersion: "v0_1_33"},
	}
}
```

Thin the tool: `tools/fixtureextract/main.go` keeps `main`, `parseHeight`, `readFixture`, `runGolden` (golden blob extraction is tool-only) and `runReport` becomes:

```go
func runReport(heightStr, dir string) {
	height := parseHeight(heightStr)
	metaBytes, dataBytes := readFixture(dir, height)
	r := buildRouter()
	res, err := fixturereport.Report(r, metaBytes, dataBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "report: %v\n", err)
		os.Exit(1)
	}
	if res.Height != height {
		fmt.Fprintf(os.Stderr, "WARNING: decoded height %d != filename height %d\n", res.Height, height)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
		os.Exit(1)
	}
}
```

and `buildRouter()` uses `fixturereport.MainnetUpgrades()` (delete the local `mainnetUpgrades` var at `main.go:30-38`). `runGolden` keeps working unchanged apart from `buildRouter`.

- [ ] **Step 4: Run tests + regenerate one expected.json to prove output identity**

```bash
go test ./internal/fixturereport/ -v -cover
go run ./tools/fixtureextract 135297 test/fixtures/v0_1_20 > /tmp/check-135297.json
diff <(python3 -m json.tool /tmp/check-135297.json) <(python3 -m json.tool test/fixtures/v0_1_20/block-135297-expected.json)
```

Expected: tests PASS (coverage ≥90% for the package — the error-path tests carry it); diff EMPTY.

- [ ] **Step 5: Commit**

```bash
git add internal/fixturereport/ tools/fixtureextract/
git commit -m "refactor(fixtures): promote report pipeline to internal/fixturereport

Single source for expected.json generation and golden verification; full
chain-authoritative mainnet upgrade table (31 applied versions) replaces the
7-entry tool-local map."
```

**ORDERING REQUIREMENT:** execute Task 14 (router boundary matrix test) IMMEDIATELY after this task and BEFORE any fixture batch — `MainnetUpgrades()` must not sit unvalidated in CI while batches land. Include Task 14's test in this same commit if convenient. (The lenient fallback for unregistered `DecoderVersion` strings is documented behavior — `internal/router/db.go:21-24` and `TestDecoderForFallsBackToEarlierRegistered` — the boundary matrix pins every era resolution.)

---

### Task 11: `scan` mode + golden walker

**Files:**
- Modify: `tools/fixtureextract/main.go` (add `scan` subcommand)
- Create: `internal/fixturereport/golden_walk_test.go`

- [ ] **Step 1: Add scan mode**

In `tools/fixtureextract/main.go` `main()` add before the report branch:

```go
	if len(os.Args) >= 2 && os.Args[1] == "scan" {
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: fixtureextract scan <fixture-dir>")
			os.Exit(1)
		}
		runScan(os.Args[2])
		return
	}
```

and implement:

```go
// runScan walks every block-{H}-meta in dir and prints one JSON line per
// height summarizing activity — the curation index for picking fixtures.
func runScan(dir string) {
	r := buildRouter()
	metas, err := filepath.Glob(filepath.Join(dir, "block-*-meta"))
	if err != nil || len(metas) == 0 {
		fmt.Fprintf(os.Stderr, "no block-*-meta files in %s (err=%v)\n", dir, err)
		os.Exit(1)
	}
	heights := make([]int64, 0, len(metas))
	for _, m := range metas {
		var h int64
		if _, err := fmt.Sscanf(filepath.Base(m), "block-%d-meta", &h); err == nil {
			heights = append(heights, h)
		}
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	enc := json.NewEncoder(os.Stdout)
	for _, h := range heights {
		metaBytes, dataBytes := readFixture(dir, h)
		res, err := fixturereport.Report(r, metaBytes, dataBytes)
		if err != nil {
			_ = enc.Encode(map[string]any{"height": h, "error": err.Error()})
			continue
		}
		line := map[string]any{"height": h, "tx_count": res.TxCount}
		if s := res.Supplier; s != nil {
			line["msg_stake"] = len(s.MsgStake)
			line["events_staked"] = len(s.EventsStaked)
			line["kv_operators"] = len(s.HistoryOperators)
			line["scu_rows"] = s.SCURowsMin
		}
		_ = enc.Encode(line)
	}
}
```

(add `path/filepath` import; update the package doc comment usage block.)

- [ ] **Step 2: Smoke-test scan against existing fixtures**

```bash
go run ./tools/fixtureextract scan test/fixtures/v0_1_20
```

Expected: 3 JSON lines (135297, 135836, 135837), the latter two showing supplier activity.

- [ ] **Step 3: Write the golden walker (failing only if fixtures are broken)**

`internal/fixturereport/golden_walk_test.go`:

```go
package fixturereport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestGoldenWalkAllFixtures re-derives every fixture's expected.json through
// the live decode pipeline. Adding a fixture triplet automatically enrolls it
// — this is the "N versions × categories green" enforcement for Phase F.
func TestGoldenWalkAllFixtures(t *testing.T) {
	r := mustRouter(t)
	pattern := "../../test/fixtures/v0_1_*/block-*-expected.json"
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 13 {
		t.Fatalf("found only %d expected.json files (%s) — corpus missing?", len(files), pattern)
	}
	for _, ef := range files {
		ef := ef
		name := filepath.Base(filepath.Dir(ef)) + "/" + filepath.Base(ef)
		t.Run(name, func(t *testing.T) {
			var h int64
			if _, err := fmt.Sscanf(filepath.Base(ef), "block-%d-expected.json", &h); err != nil {
				t.Fatalf("unparseable fixture name: %v", err)
			}
			dir := filepath.Dir(ef)
			meta, err := os.ReadFile(fxPath(dir, h, "meta"))
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(fxPath(dir, h, "data"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := Report(r, meta, data)
			if err != nil {
				t.Fatalf("Report: %v", err)
			}
			raw, err := os.ReadFile(ef)
			if err != nil {
				t.Fatal(err)
			}
			var want Result
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, want) {
				t.Fatalf("mismatch:\ngot  %+v\nwant %+v", *got, want)
			}
		})
	}
}
```

- [ ] **Step 4: Run**

```bash
go test ./internal/fixturereport/ -run TestGoldenWalk -v
```

Expected: PASS with 13 subtests (the existing corpus).

- [ ] **Step 5: Commit**

```bash
git add tools/fixtureextract/ internal/fixturereport/
git commit -m "feat(fixtures): scan mode for curation + golden walker over the whole corpus"
```

---

### Task 12: `scripts/curate_fixtures.sh`

**Files:**
- Create: `scripts/curate_fixtures.sh` (chmod +x)

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# curate_fixtures.sh — stage one poktroll version's archived FilePlugin output
# for fixture curation.
#
# Usage: scripts/curate_fixtures.sh <version-tag>        # e.g. v0.1.13
#
# 1. Finds <ver>-h*-fileplugin.tar.xz in /tmp; downloads from the Hetzner
#    archeology bucket (+sha256 verify) if absent.
# 2. Extracts to /tmp/fixtures-<ver>/ (flattened).
# 3. Prints the per-height activity index (`fixtureextract scan`).
#
# Pick heights per spec §8.1 (boundary / max-activity / quiet), then for each:
#   cp /tmp/fixtures-<ver>/block-<H>-{meta,data} test/fixtures/<era-dir>/
#   go run ./tools/fixtureextract <H> test/fixtures/<era-dir> \
#       > test/fixtures/<era-dir>/block-<H>-expected.json
# Era dirs: see the table in docs/superpowers/plans/2026-06-10-slice-1-phase-f-plan.md
# and test/fixtures/README.md.
set -euo pipefail
VER="${1:?usage: curate_fixtures.sh <version-tag>}"
BUCKET="pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet"

TARBALL=$(ls /tmp/"${VER}"-h*-fileplugin.tar.xz 2>/dev/null | head -1 || true)
if [ -z "$TARBALL" ]; then
  REMOTE=$(rclone lsf "$BUCKET/$VER/" 2>/dev/null | grep -- '-fileplugin\.tar\.xz$' | head -1 || true)
  if [ -z "$REMOTE" ]; then
    echo "ERROR: no fileplugin tarball for $VER in the bucket." >&2
    echo "       (archeology for this version may still be running on multi-1 — retry later.)" >&2
    exit 1
  fi
  echo "downloading $REMOTE …" >&2
  rclone copyto "$BUCKET/$VER/$REMOTE" "/tmp/$REMOTE"
  rclone copyto "$BUCKET/$VER/$REMOTE.sha256" "/tmp/$REMOTE.sha256"
  EXPECTED=$(awk '{print $1}' "/tmp/$REMOTE.sha256")
  ACTUAL=$(sha256sum "/tmp/$REMOTE" | awk '{print $1}')
  [ "$EXPECTED" = "$ACTUAL" ] || { echo "ERROR: sha256 mismatch for $REMOTE" >&2; exit 1; }
  TARBALL="/tmp/$REMOTE"
fi

DEST="/tmp/fixtures-${VER}"
if [ ! -d "$DEST" ] || [ -z "$(ls -A "$DEST" 2>/dev/null)" ]; then
  mkdir -p "$DEST"
  echo "extracting $TARBALL → $DEST …" >&2
  tar -xJf "$TARBALL" -C "$DEST"
  # Tarball layouts vary; flatten any nested block files.
  find "$DEST" -mindepth 2 \( -name 'block-*-meta' -o -name 'block-*-data' \) \
    -exec mv -n {} "$DEST/" \;
fi

go run ./tools/fixtureextract scan "$DEST"
```

- [ ] **Step 2: Smoke-test with a tiny version**

```bash
chmod +x scripts/curate_fixtures.sh
scripts/curate_fixtures.sh v0.1.2
```

Expected: extraction + one JSON line per block in 78621–78631 (≤11 lines). If the tarball layout differs (e.g. only a subset of heights), that is fine — scan reports what exists.

- [ ] **Step 3: Commit**

```bash
git add scripts/curate_fixtures.sh
git commit -m "feat(scripts): curate_fixtures.sh — stage archived version data for fixture curation"
```

---

### Tasks 13a–13f: Fixture curation batches

Identical procedure per batch; only the version list changes. **Procedure for EACH version in a batch:**

1. `scripts/curate_fixtures.sh <ver>` → activity index.
2. Pick heights per the §8.1 criteria above (boundary `applied_height` per the reference table, max-activity, quiet — dedupe if one block satisfies several; tiny eras may only yield 2).
3. For each picked H (era dir from the reference table; `mkdir -p` the dir if new):
   ```bash
   cp /tmp/fixtures-<ver>/block-<H>-meta test/fixtures/<era>/
   cp /tmp/fixtures-<ver>/block-<H>-data test/fixtures/<era>/
   go run ./tools/fixtureextract <H> test/fixtures/<era> > test/fixtures/<era>/block-<H>-expected.json
   ```
   Check stderr: the reported `decoder: vX_Y_Z` MUST match the era column of the reference table — a mismatch means the upgrade map or the era assignment is wrong: STOP and investigate, do not commit.
4. `go test ./internal/fixturereport/ -run TestGoldenWalk` — the new triplets enroll automatically and must pass.
5. After the whole batch: `rm -rf /tmp/fixtures-<ver>` for each version (keep the tarballs), update `test/fixtures/README.md` (Task 16 creates it in batch 13a), commit.

**Notes that apply across batches:**
- `discovery/candidates.json` (`/tmp/pocketscribe-discovery/`) lists migration-era heights with `EventSupplierUnbondingEnd` (96281, 96600–96606…) — they fall in v0.1.15: use one as that version's activity fixture.
- Eras v0.1.2–v0.1.11 lasted 5–13 blocks each; expect zero supplier activity (negative fixtures are valid coverage — v0_1_0 precedent).
- Large extracts (v0.1.21/22/24/26/27: 0.7–2.9 GB compressed) can be tens of GB uncompressed: extract → curate → `rm -rf` the extract dir before the next one. Disk has ~870 GB free; never hold more than 2 large extracts simultaneously.
- Commit message per batch: `test(fixtures): curate <ver-list> fixtures (spec §8.1, phase F batch N)` with a body line per version: heights picked + why.

- [ ] **Task 13a:** v0.1.2, v0.1.3, v0.1.4, v0.1.5, v0.1.6, v0.1.7 → era `v0_1_0/`. Also create `test/fixtures/README.md` in this commit (see Task 16 for the required table; start it with the rows known so far).
- [ ] **Task 13b:** v0.1.8, v0.1.9 → new era dir `v0_1_8/`; v0.1.11, v0.1.12 → `v0_1_10/`.
- [ ] **Task 13c:** v0.1.13, v0.1.14, v0.1.15 → `v0_1_10/` (v0.1.15: use a candidates.json height with the MIGRATION unbonding event as the activity fixture).
- [ ] **Task 13d:** v0.1.16, v0.1.17, v0.1.18, v0.1.19 → `v0_1_10/`. Note: block 102542 (already curated in Phase D, `test/fixtures/v0_1_10/`) is v0.1.17-era (102142–116099), so v0.1.17 only needs its boundary block 102142 + a quiet block.
- [ ] **Task 13e:** v0.1.21, v0.1.22, v0.1.23 → `v0_1_20/`.
- [ ] **Task 13f:** v0.1.24, v0.1.25, v0.1.26 → `v0_1_20/`; v0.1.27 → new era dir `v0_1_27/`.

Each batch task ends with:

```bash
go test ./internal/fixturereport/ -run TestGoldenWalk -v 2>&1 | tail -5
git add test/fixtures/ && git commit -m "test(fixtures): curate <versions> fixtures (spec §8.1, phase F batch <N>)"
```

---

### Task 14: Router boundary matrix — all 31 mainnet boundaries

**Files:**
- Create: `internal/router/mainnet_boundaries_test.go` (external test package — avoids the router↔fixturereport import cycle)

- [ ] **Step 1: Write the test**

```go
package router_test

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/fixturereport"
	"github.com/pokt-network/pocketscribe/internal/router"
)

// TestDecoderForAllMainnetBoundaries pins DecoderFor at EVERY mainnet upgrade
// boundary (±1) against the era expectations machine-derived from the break
// map (docs/research/supplier-shape-breaks.md): breaks at v0_1_8 and v0_1_27;
// v0_1_0/10/20/28/29/30 registered as range anchors. Spec test 15 extended to
// the full table (spec §9 Phase F).
func TestDecoderForAllMainnetBoundaries(t *testing.T) {
	r, err := router.NewStaticRouter(fixturereport.MainnetUpgrades(), router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	// expected decoder version AT the boundary (and until the next boundary).
	eras := []struct {
		name   string
		height int64
		want   string
	}{
		{"genesis", 1, "v0_1_0"},
		{"v0.1.2", 78621, "v0_1_0"}, {"v0.1.3", 78632, "v0_1_0"}, {"v0.1.4", 78641, "v0_1_0"},
		{"v0.1.5", 78654, "v0_1_0"}, {"v0.1.6", 78659, "v0_1_0"}, {"v0.1.7", 78665, "v0_1_0"},
		{"v0.1.8", 78671, "v0_1_8"}, {"v0.1.9", 78678, "v0_1_8"},
		{"v0.1.10", 78683, "v0_1_10"}, {"v0.1.11", 78689, "v0_1_10"}, {"v0.1.12", 78697, "v0_1_10"},
		{"v0.1.13", 80510, "v0_1_10"}, {"v0.1.14", 93825, "v0_1_10"}, {"v0.1.15", 94370, "v0_1_10"},
		{"v0.1.16", 99293, "v0_1_10"}, {"v0.1.17", 102142, "v0_1_10"}, {"v0.1.18", 116100, "v0_1_10"},
		{"v0.1.19", 117454, "v0_1_10"},
		{"v0.1.20", 135297, "v0_1_20"}, {"v0.1.21", 138931, "v0_1_20"}, {"v0.1.22", 155173, "v0_1_20"},
		{"v0.1.23", 161109, "v0_1_20"}, {"v0.1.24", 161169, "v0_1_20"}, {"v0.1.25", 190974, "v0_1_20"},
		{"v0.1.26", 190979, "v0_1_20"},
		{"v0.1.27", 247893, "v0_1_27"},
		{"v0.1.28", 287932, "v0_1_28"},
		{"v0.1.29", 382250, "v0_1_29"},
		{"v0.1.30", 484473, "v0_1_30"}, {"v0.1.31", 635506, "v0_1_30"}, {"v0.1.33", 703870, "v0_1_30"},
	}
	for i, e := range eras {
		dec, err := r.DecoderFor(e.height)
		if err != nil {
			t.Fatalf("%s @%d: %v", e.name, e.height, err)
		}
		if dec.Version() != e.want {
			t.Errorf("%s @%d: decoder %s, want %s", e.name, e.height, dec.Version(), e.want)
		}
		// One height BEFORE each boundary must resolve to the PREVIOUS era.
		if i > 0 {
			prev := eras[i-1]
			dec, err := r.DecoderFor(e.height - 1)
			if err != nil {
				t.Fatalf("%s @%d-1: %v", e.name, e.height, err)
			}
			if dec.Version() != prev.want {
				t.Errorf("@%d (just below %s): decoder %s, want %s", e.height-1, e.name, dec.Version(), prev.want)
			}
		}
	}
}
```

- [ ] **Step 2: Run**

```bash
go test ./internal/router/ -run TestDecoderForAllMainnetBoundaries -v
```

Expected: PASS. Any failure here means DefaultRegistry/fallback semantics changed — investigate against the break map before touching the expectations.

- [ ] **Step 3: Commit**

```bash
git add internal/router/mainnet_boundaries_test.go
git commit -m "test(router): pin DecoderFor across all 31 mainnet upgrade boundaries"
```

---

### Task 15: Integration matrix additions (representative new eras, full stack)

**Files:**
- Modify: `test/integration/supplier_consumer_test.go` (the `cases` table, see `:300-320`)
- Modify: `test/integration/block_consumer_test.go` (the analogous table at `:182-186`)

- [ ] **Step 1: Pick 3 representative new fixtures** (after batches land): one early-era block (v0.1.2-era, decoder v0_1_0 — likely quiet/negative), the v0.1.15 MIGRATION-unbonding block (decoder v0_1_10), one v0.1.24-era block (decoder v0_1_20). Add a `fixtureCase` row each to the supplier table and the corresponding row to the block table, with `expectedVersion` matching how that test resolves `decoder_version` (follow the existing seeding pattern in the file — see the comment at `supplier_consumer_test.go:306` for the lenient-fallback spelling convention). The upgrades-table seeding inside these tests must gain rows for the new eras involved (mirror how 102542/v0.1.17 is seeded today).

- [ ] **Step 2: Run the two integration tests**

```bash
go test -tags integration ./test/integration/ -run 'TestSupplierConsumer|TestBlockConsumer' -v -count=1 2>&1 | tail -30
```

Expected: PASS including the new rows (full pipeline: Bootstrap fan-out → NATS → consumer → Postgres rows → expected.json values).

- [ ] **Step 3: Commit**

```bash
git add test/integration/
git commit -m "test(integration): exercise early/migration/late eras through the full pipeline"
```

---

### Task 16: Fixture coverage matrix README

**Files:**
- Create/finish: `test/fixtures/README.md` (started in batch 13a)

- [ ] **Step 1: Write the matrix**

The README must contain: (a) the triplet format (`block-{H}-meta` length-delimited Req/ResponseFinalizeBlock, `block-{H}-data` uvarint-delimited StoreKVPairs, `block-{H}-expected.json` generated by `go run ./tools/fixtureextract <H> <dir>`); (b) the era-dir convention (dir = decoder version that must decode it); (c) the FULL 32-version matrix — one row per mainnet-applied version: version, applied range (from the reference table in this plan), fixture heights present, era dir, status (`covered` / `negative-only` / `PENDING — no archived data`); v0.1.30, v0.1.31, v0.1.33 rows marked PENDING with: "archeology running on multi-1; when the tarball lands in the bucket, run the curate-version-fixtures skill"; (d) footnote: v0.1.1 and v0.1.32 were never applied on mainnet (bucket `versions.yaml`, verified vs Sauron LCD).

- [ ] **Step 2: Commit**

```bash
git add test/fixtures/README.md
git commit -m "docs(fixtures): 32-version coverage matrix and curation conventions"
```

---

### Task 17: `curate-version-fixtures` skill (covers v0.1.30/31/33 when data lands)

**Files:**
- Create: `.claude/skills/curate-version-fixtures/SKILL.md`

- [ ] **Step 1: Write the skill** (mirror the frontmatter style of `.claude/skills/` neighbors, e.g. the `verify-migrations` skill):

```markdown
---
name: curate-version-fixtures
description: Curate FilePlugin fixtures for a poktroll version from the Hetzner archeology bucket — download, scan, select per spec §8.1, generate expected.json, enroll in the golden walker. Use when a new version's tarball lands in the bucket (v0.1.30 / v0.1.31 / v0.1.33 pending as of 2026-06-10) or when refreshing an era's coverage.
---

# Curate version fixtures

## When
- A new poktroll version's `<ver>-h<H>-fileplugin.tar.xz` appears in
  `pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/<ver>/`
  (check: `rclone lsf pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/`).
  As of 2026-06-10 the archeology run on multi-1 has not yet uploaded
  v0.1.30, v0.1.31, v0.1.33.

## Steps
1. `scripts/curate_fixtures.sh <ver>` — downloads (sha256-verified), extracts
   to /tmp/fixtures-<ver>/, prints the per-height activity index.
2. Select ~3 heights per spec §8.1 (docs/superpowers/specs/2026-06-08-slice-1-design.md:356):
   boundary block (applied_height — confirm against the upgrades table or
   internal/fixturereport/mainnet.go), max supplier activity, quiet block.
3. Era dir: the decoder version DecoderFor(H) resolves to — confirm with the
   stderr `decoder:` line in step 4. New break version → new era dir AND
   follow .claude/rules/decoders.md rules 9–10 FIRST (machine-derived closure
   diff; never assume stability).
4. Per height H:
   cp /tmp/fixtures-<ver>/block-<H>-{meta,data} test/fixtures/<era>/
   go run ./tools/fixtureextract <H> test/fixtures/<era> \
     > test/fixtures/<era>/block-<H>-expected.json
5. If the version is NOT yet in internal/fixturereport/mainnet.go: add it
   (Name dotted, AppliedAtHeight from `ps sync-upgrades` / the chain,
   DecoderVersion underscored) and extend
   internal/router/mainnet_boundaries_test.go with the new boundary row.
   Remember: consumer wakeup is restart-based — after `ps sync-upgrades`
   lands a version that wakes a dormant consumer, restart that consumer.
6. `go test ./internal/fixturereport/ -run TestGoldenWalk` — must pass.
7. Update test/fixtures/README.md matrix row (PENDING → covered).
8. Optionally add a full-stack row in test/integration/ (see
   supplier_consumer_test.go cases table) for new eras.
9. `rm -rf /tmp/fixtures-<ver>`; commit
   `test(fixtures): curate <ver> fixtures (spec §8.1)`.
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/curate-version-fixtures/
git commit -m "feat(skills): curate-version-fixtures — repeatable curation for future versions (v0.1.30/31/33 pending)"
```

---

### Task 18: Final gate + spec progress note + merge

- [ ] **Step 1: Full gauntlet**

```bash
make ci
golangci-lint run --build-tags=integration ./...
go test -tags integration ./test/integration/ -count=1
go test ./internal/... -coverprofile=/tmp/cover-f.out && go tool cover -func=/tmp/cover-f.out | tail -3
```

Expected: all green; decoders 100%, `internal/` ≥90% combined.

- [ ] **Step 2: Spec progress note**

Append to `docs/superpowers/specs/2026-06-08-slice-1-design.md` next to the existing phase-complete notes (around `:661`):

```
**Phase F complete**: branch slice-1/phase-f — version-aware required_set/IsSealed (semver-gated via internal/protover), dormancy gate + wakeup, multi-network and backfill seal semantics, sidecar 256KiB/1MiB payload caps (spec tests 22–27 green), fixtures curated for all 24 archivable remaining versions (~3 each per §8.1), golden walker enrolls the whole corpus, router pinned across all 31 mainnet boundaries. v0.1.30/v0.1.31/v0.1.33 PENDING archived data — covered by curate-version-fixtures skill + break-map closure evidence. 2026-06-10.
```

Commit: `docs(spec): record Phase F completion`.

- [ ] **Step 3: Merge (NO push)**

```bash
git checkout main && git merge --no-ff slice-1/phase-f -m "Merge Phase F: version-aware orchestration + multi-version fixture expansion"
```

User pushes (`--force-with-lease`) when ready — do NOT push.

---

## Self-review checklist (run after writing, before execution)

- Spec tests 22–27 each map to a named test: 22 `TestDormantConsumer`, 23 `TestDynamicRequiredSetPerHeight`, 24 `TestConsumerWakeup`, 25 `TestMultiNetworkRequiredSet`, 26 `TestBackfillSemantics`, 27 `TestCapPublishPolicy`/`TestFanOutHeightRefusesOversizeTx`.
- Spec §9 Phase F items: fixtures for remaining versions (Tasks 12–13), codegen/adapters — NOT needed beyond existing (break map: no new break versions among the 24; documented), sync-upgrades known-names — already complete in `configs/networks/mainnet.yaml:17-49` (verified during planning; no task needed), exit matrix → golden walker + boundary matrix + README.
- Invariants: no UPDATE outside allowed metadata; `upgrades`/`consumer_registry` writes unchanged in nature; no `now()` on queryable axes; append-only untouched.
- Out of scope (unchanged from handoff loose ends): ADR-024 valves (Phase G), `Migrate("down")` (Phase G), EventSupplierSlashed/tokenomics (future module).
