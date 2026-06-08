# ADR-012: 5-layer testing strategy with TDD

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

A distributed indexer with multiple binaries, multi-version decoders, and gap-aware aggregates needs disciplined testing across multiple layers — unit alone misses pipeline issues, E2E alone is too slow for every commit.

Prior indexer implementations suffered **silent errors** (list-and-diff with no validation; math drift surfacing months later). Testing must specifically guard against these.

## Decision

Adopt a **5-layer testing pyramid**, with **TDD as the default mode of work**.

| Layer | Location | Build tag | When |
|---|---|---|---|
| **1. Unit** | `_test.go` next to code | none | Every commit; <10s |
| **2. Component** | same package | `component` | Pre-merge; testcontainers |
| **3. Golden / Contract** | `test/golden/v*/` + decoder tests | none | Every commit on decoder changes |
| **4. Integration** | `test/integration/*_test.go` | `integration` | Pre-merge |
| **5. E2E** | `test/e2e/*_test.go` | `e2e` | Nightly + pre-release |

**TDD rules**:
- Always write the test first.
- Bugs are reproduced as failing tests **before** fixing.
- Coverage target: 90% on `internal/`, 100% on decoders.

## Consequences

### Positive

- **Bugs reproduce as tests** → never silently reappear.
- **Refactors are safe** — tests guarantee correctness.
- **Cross-version decoder fidelity is verifiable** via golden + cross-version tests.
- **Pipeline integration coverage** via testcontainers (no manual setup).
- **Real chain coverage** via E2E with poktroll devnet.
- **Coverage targets are objective** — CI enforces.

### Negative

- **Slower initial development** for TDD newcomers. Mitigated by the agents/skills automating test scaffolding (`pocketscribe-test-author`, `add-consumer`).
- **CI runtime higher** due to integration + e2e suites. Mitigated by gating heavy suites behind tags.
- **Golden file maintenance** — fixtures must be captured from a real node. Mitigated by `make regenerate-goldens` + manual review of diff.

### Neutral

- testcontainers requires Docker on the dev machine. Already a requirement for Tilt.

## Alternatives considered

### Option A: Unit + E2E only (skip middle layers)
- Pro: simpler.
- Con: misses subsystem interactions (consumer↔NATS↔DB) that don't show up in unit tests.
- Con: relies on slow E2E for everything → painful feedback loop.
- **Rejected because**: missing layers miss bugs.

### Option B: Mock everything (Postgres, NATS, chain)
- Pro: very fast.
- Con: mocks drift from reality → false confidence.
- Con: this is the **Pocketdex pattern** that hid bugs.
- **Rejected because**: real subsystems with real wire protocols.

### Option C: Property-based testing only
- Pro: powerful for invariant verification (e.g., "for any sequence of inserts, final state matches sorted insert").
- Con: doesn't replace example-based tests for known bugs.
- **Partially adopted**: property-based tests for commutativity, idempotency invariants.

## Implementation notes

### Tooling

- `go test` + `testify/require` (assertions).
- `mockery/v3` (mocks for interfaces; rare — most things test against real subsystems).
- `testcontainers-go` (Postgres + Timescale + NATS modules).
- `sebdah/goldie/v2` (golden files).
- `gremlins` (mutation testing — future).

### Patterns enforced

- Test names describe the failure: `TestSupplierConsumer_LateArrival_ReSealsBucket`.
- Table-driven by default; sub-tests via `t.Run(name, fn)`.
- `t.Parallel()` on independent tests.
- No `time.Sleep(N)` — use channels, signals, or `require.Eventually`.
- No execution-order dependencies.

### Per-layer coverage targets

| Layer | Target |
|---|---|
| Unit | 90% on `internal/` |
| Component | Every consumer module + sealing + reconciler |
| Golden | 100% of decoder methods × every version |
| Integration | Every "pipeline" critical path |
| E2E | Happy path + 1-2 catastrophic failures per release |

### CI

```yaml
# Pre-merge
jobs:
  lint        # golangci-lint
  test-unit   # make test
  test-component
  test-golden
  test-integration
  proto-matrix # cross-version decoder tests

# Nightly
jobs:
  test-e2e
```

## References

- Full session transcript: Topic 17 (testing strategy consolidated).
- `docs/architecture/10-testing.md` — detailed strategy doc.
- `.claude/agents/pocketscribe-test-author.md` — agent guide.
- CLAUDE.md Process Invariants section.
