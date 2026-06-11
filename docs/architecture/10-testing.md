# 10 — Testing Strategy

## TDD + 5 layers

> Always write the test first. The test names the contract; the implementation satisfies it. Bugs without tests reappear; tests without bugs find them.

```
Layer 1: Unit                  ← fast (<10s), every commit
Layer 2: Component             ← testcontainers, ~seconds
Layer 3: Golden / Contract     ← fixture-based, every PR
Layer 4: Integration           ← full stack (no real chain), ~minutes
Layer 5: E2E                   ← real poktroll, ~10-30 min, pre-release
```

## Layer 1: Unit tests

**Goal**: prove pure logic correctness without IO.

**Where**: `_test.go` next to the code (`internal/.../<name>_test.go`).

**Style**: table-driven, sub-tests via `t.Run(name, fn)`, `require.X` (testify).

**Examples**:
- Decoder field mapping (proto bytes → canonical type).
- Sealing condition logic (given cursors X, Y, should this bucket seal?).
- Consolidation gap detection (given heights {1,2,4,5}, where's the gap?).
- Subject naming functions (`subjects.KVStore("supplier", 123)` → `"pokt.kv.supplier.123"`).

**Run**: `make test` (every commit; <10s total).

**Coverage target**: 90% on `internal/`.

## Layer 2: Component tests

**Goal**: prove one subsystem works correctly with its real dependencies, in isolation.

**Where**: `_test.go` in the same package, optionally gated with `//go:build component` if slower than unit.

**Tools**: [testcontainers-go](https://golang.testcontainers.org/) for NATS + Postgres. No mocks.

**Examples**:
- Consumer reads a NATS message, writes to Postgres, asserts row appears.
- Sealing loop iterates registry, refreshes a CAGG, inserts bucket_seal row.
- Reconciler bulk-fetches from a mock chain, compares with DB, inserts correction.

**Setup helpers** in `internal/testutil/`:

```go
func SetupPostgres(t *testing.T) *pgxpool.Pool {
    pgC, err := postgres.RunContainer(ctx,
        postgres.WithImage("timescale/timescaledb:latest-pg18"),
        postgres.WithDatabase("test"),
        postgres.WithUsername("test"),
        postgres.WithPassword("test"),
        postgres.WithInitScripts(filepath.Join("..", "..", "schema", "migrations")),
    )
    require.NoError(t, err)
    t.Cleanup(func() { pgC.Terminate(ctx) })
    return pgxpool.New(ctx, pgC.MustConnectionString(ctx))
}

func SetupNATS(t *testing.T) *nats.Conn {
    natsC, err := natscontainer.RunContainer(ctx,
        natscontainer.WithImage("nats:2.10"),
        natscontainer.WithArgument("-js"),
    )
    require.NoError(t, err)
    t.Cleanup(func() { natsC.Terminate(ctx) })
    return mustConnect(t, natsC.MustConnectionString(ctx))
}
```

**Run**: `make test-component` (manual; CI runs on PR).

## Layer 3: Golden / Contract tests

**Goal**: prove decoders produce stable canonical output for known chain data.

**Where**: `test/golden/v{X}_{Y}_{Z}/` (fixtures) + `internal/decoders/v{X}_{Y}_{Z}/decoder_test.go`.

**Tool**: [sebdah/goldie/v2](https://github.com/sebdah/goldie) — diffs against `.json` files; `-update` flag regenerates.

**Layout**:
```
test/golden/
├── v0_0_10/
│   ├── supplier_stake_001.bin                 ← raw KV value bytes (captured)
│   └── supplier_stake_001.json                ← expected canonical JSON (golden)
├── v0_1_0/
│   └── ...
└── v0_1_5/
    └── ...
```

**Pattern**:
```go
import "github.com/sebdah/goldie/v2"

func TestSupplierDecoder_v0_1_5_StakeSnapshot(t *testing.T) {
    g := goldie.New(t,
        goldie.WithFixtureDir("../../../test/golden/v0_1_5"),
        goldie.WithNameSuffix(".json"),
    )
    raw := loadFixture(t, "supplier_stake_001.bin")
    got, err := v0_1_5.Decoder{}.DecodeSupplierKV(nil, raw)
    require.NoError(t, err)
    g.AssertJson(t, "supplier_stake_001", got)
}
```

**Cross-version test** (in `test/integration/`):
```go
func TestSupplierDecoder_CrossVersion(t *testing.T) {
    cases := []struct {
        version string
        fixture string
    }{
        {"v0_0_10", "v0_0_10/supplier_stake_001.bin"},
        {"v0_1_0",  "v0_1_0/supplier_stake_001.bin"},
        {"v0_1_5",  "v0_1_5/supplier_stake_001.bin"},
    }
    for _, c := range cases {
        t.Run(c.version, func(t *testing.T) {
            raw := loadFixture(t, c.fixture)
            decoder := decoders.For(c.version)
            got, err := decoder.DecodeSupplierKV(nil, raw)
            require.NoError(t, err)
            require.NotNil(t, got)
            // Assert canonical equivalence (matching addresses, sensible stake)
        })
    }
}
```

**Capturing fixtures** (one-time per version):
```bash
# Connect to a testnet/mainnet node at a known height
poktrolld query supplier show-supplier pokt1abc --height=H --output json > /tmp/sup.json

# Extract raw KV value with a debug tool (TODO: scripts/tools/capture-kv.go)
poktrolld debug kv-extract supplier pokt1abc H > test/golden/vX_Y_Z/supplier_stake_001.bin

# Regenerate golden expected
make regenerate-goldens
git diff test/golden/  # review
```

**Run**: `make test-golden` or as part of `make test`.

**Coverage target**: 100% of decoder methods × every version.

## Layer 4: Integration tests

**Goal**: prove the full pipeline works end-to-end without a real chain.

**Where**: `test/integration/*_test.go` with `//go:build integration` tag.

**Pattern**: spin sidecar + NATS + consumers + Postgres in testcontainers. Inject synthetic block files. Assert SQL state.

**Examples**:
- `pipeline_supplier_stake_test.go` — write a synthetic `block-N-meta`/`block-N-data` to a temp dir, run sidecar, assert NATS receives publish, assert consumer writes supplier_history row.
- `pipeline_late_arrival_test.go` — seal a bucket, inject a late event, assert sealing loop re-seals.
- `pipeline_ha_dedup_test.go` — publish same block twice from two sidecars, assert one consumer row.
- `reconciler_drift_test.go` — set up mismatched state in DB and mock chain, run reconciler, assert auto-heal correction.

**Run**: `make test-integration` (~minutes; CI on PR).

## Layer 5: E2E tests

**Goal**: prove everything works against a real chain.

**Where**: `test/e2e/*_test.go` with `//go:build e2e` tag.

**Setup**: docker-compose (or kind) with a local poktroll devnet that produces blocks. Apply known transactions. Wait. Assert.

**Examples**:
- `e2e_happy_path_test.go` — boot devnet, send `MsgStakeSupplier`, wait 60s, assert supplier row in DB.
- `e2e_upgrade_boundary_test.go` — boot devnet, apply upgrade, send msgs before+after, assert correct decoder used.
- `e2e_node_restart_test.go` — kill poktroll mid-block, restart, assert no loss.

**Run**: `make test-e2e` (~10-30 min; nightly + pre-release).

## Coverage targets

| Layer | Target |
|---|---|
| Unit | 90% on `internal/` |
| Component | every consumer module + sealing + reconciler |
| Golden | 100% of decoder methods × every version |
| Integration | every "pipeline" critical path |
| E2E | happy path + 1-2 catastrophic failures per release |

## Anti-patterns

- ❌ Writing implementation before the test.
- ❌ Mocking the database. Use testcontainers.
- ❌ Mocking the chain in golden tests. Use real captured fixtures.
- ❌ Tests with `time.Sleep(N)` for synchronization. Use channels, signals, or `require.Eventually`.
- ❌ Tests that depend on execution order. Each test independent; `t.Parallel()` safe by default.
- ❌ One mega-test covering 5 behaviors. Split.
- ❌ "Manual verification" instead of an automated test.

## TDD discipline checklist

For every change:
1. Identify the smallest behavior under test.
2. Write the failing test.
3. Run it. Confirm it fails for the **right reason** (assertion mismatch, not compilation error).
4. Write minimal code to pass.
5. Run again. Confirm green.
6. Refactor if needed.
7. Add edge cases (malformed input, version boundary, out-of-order arrival, late insert).
8. Commit test + code together.

## CI matrix

```yaml
# .github/workflows/ci.yml
jobs:
  lint:        # golangci-lint
  test-unit:   # make test
  test-component: # make test-component
  test-golden: # included in make test
  test-integration: # make test-integration
  gen-check:   # make gen-check (fail if generated code is stale)
  
  # Cross-version proto matrix
  proto-matrix:
    strategy:
      matrix:
        proto_version: [v0_0_10, v0_0_12, v0_1_0, v0_1_5]
    # ...

  # Nightly
  e2e:
    if: github.event_name == 'schedule'
    # ...
```

## Tools recap

| Concern | Choice |
|---|---|
| Test runner | `go test` |
| Assertions | `testify/require` |
| Mocks | `mockery v3` (YAML config) |
| Containers | `testcontainers-go` (Postgres, NATS modules) |
| Golden files | `sebdah/goldie/v2` |
| Coverage | `go tool cover` |
| Mutation | future: `gremlins` (deferred) |

## See also

- ADR-012 (testing strategy) — full rationale.
- `.claude/agents/pocketscribe-test-author.md` — agent guide for writing tests at each layer.
- `docs/operations/development-workflow.md` — TDD loop in practice.
