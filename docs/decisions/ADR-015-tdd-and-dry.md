# ADR-015: TDD by default + DRY across the codebase

**Status**: Accepted
**Date**: 2026-05-22
**Authors**: Jorge Cuesta, Claude

## Context

The user explicitly named these as base process principles: "makefile, best practices DRY, TDD, etc deberian ser la clave y base del proceso."

These principles are operational and cultural — not just a tooling choice — and deserve an ADR so reviewers can cite them.

## Decision

1. **TDD is the default mode of work.** Write the test first; see it fail; write the code; see it pass.
2. **DRY across the codebase**, but **no premature abstraction.** Extract only when the same logic appears in 3+ places AND varies along the same axis.

## Consequences

### Positive (TDD)

- Specs become executable. The test names the expected behavior; the implementation satisfies it.
- Bugs reproduce as tests; never silently reappear.
- Refactors are safe (tests guarantee correctness).
- Codebase is testable by design (poor designs are unmockable; TDD surfaces them early).
- Tests serve as living documentation.

### Positive (DRY)

- Single source of truth per concept (e.g., NATS subject naming lives ONLY in `internal/nats/subjects.go`).
- Easier to evolve patterns — change one place, propagate.
- Less code → less surface for bugs.

### Negative (TDD)

- Slower initial development for newcomers to TDD.
- Tempting to skip TDD for "obvious" code (which then breaks in production).

### Negative (DRY)

- Premature abstraction is a real cost (look-alike code != same logic). Wrong abstraction is worse than duplication.
- Aggressive DRY makes code harder to read for newcomers (must follow indirection).

### Mitigations

- Skill `pre-commit-check` enforces tests exist for changed Go files (warning if test missing).
- Agent `pocketscribe-test-author` automates TDD scaffolding.
- Agent `pocketscribe-reviewer` calls out duplicate logic and missing tests.
- Rule: "3 strikes before abstraction" — first occurrence: copy. Second: still copy (note duplication). Third: abstract.

## Alternatives considered

### Option A: Test-after development
- Pro: faster initial iteration.
- Con: tests written after the fact tend to confirm existing behavior, not specify expected behavior.
- Con: design decisions are baked in by the time tests are written.
- **Rejected**: TDD's primary value is design pressure.

### Option B: No DRY enforcement
- Pro: less cognitive overhead.
- Con: code rot — same logic in many places drifts.
- **Rejected**: DRY at the right granularity is essential.

### Option C: Strict DRY (every duplicate extracted immediately)
- Pro: zero duplication.
- Con: premature abstractions are hard to undo; wrong abstraction worse than duplication.
- **Rejected**: 3-strikes rule balances.

## Implementation notes

### TDD enforcement

- CLAUDE.md "Process invariants" section codifies the rule.
- Agents (`pocketscribe-test-author`) generate test scaffolds first.
- `pre-commit-check` skill warns if new logic files have no companion test file.
- Reviewer agent calls out "no test for this fix" in PR review.
- Coverage targets (per ADR-012) enforced by CI.

### DRY enforcement (single-source rules)

These are the **canonical single sources** — anything mentioning them outside these files is a bug:

| Concept | Single source |
|---|---|
| NATS subject names | `internal/nats/subjects.go` |
| Prometheus metric names | `internal/metrics/metrics.go` |
| Configuration schema | `internal/config/*.go` |
| Canonical entity types | `internal/types/*.go` |
| SQL queries (named) | `internal/store/queries/*.sql` (via sqlc) |
| Decoder interface | `internal/decoders/decoder.go` |
| Aggregate definitions | `aggregate_registry` (DB), not code |
| Hard invariants | `CLAUDE.md` |
| Architecture decisions | `docs/decisions/` (ADRs) |

### Anti-patterns to reject in code review

- `internal/util/`, `internal/common/`, `internal/helpers/` — packages without a clear domain.
- Same string literal in multiple Go files (subject name, metric name, table name).
- Same SQL query inline in multiple Go files.
- Custom assertion helpers when `testify/require` covers it.
- "We'll add a test later."

## References

- User request: "deberiamos optar por la version MAS MODERNA... [also] makefile, best practices DRY, TDD, etc deberian ser la clave y base del proceso."
- CLAUDE.md "Process Invariants" section.
- ADR-012 (testing strategy) — the test layers.
