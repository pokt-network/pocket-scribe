# ADR-001: Go over Rust for the indexer

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude
**Supersedes**: —

## Context

The new indexer (replacing SubQuery+Node.js Pocketdex) needs a fast, memory-efficient runtime with good concurrency and a strong ecosystem for Cosmos SDK chains.

Two viable options were considered:
- **Go** — same language as poktroll, native protobuf support, mature Cosmos ecosystem.
- **Rust** — faster, more memory-efficient, but Cosmos client ecosystem is thin.

## Decision

Use **Go** (latest stable) for all PocketScribe binaries.

## Consequences

### Positive

- **Reuse poktroll types directly**. No need to re-implement protobuf decoders from scratch — vendor the protos and codegen.
- **Cosmos SDK & CometBFT in Go**. Tooling (testnet, RPC clients, gRPC) is first-class.
- **ABCI `StreamingService` interface is Go-native** — implementing the official `ABCIListener` (if we ever needed to) is straightforward.
- **Team familiarity**. Most Pocket Network engineers already work in Go.
- **Hiring pool**. Mid-senior Go developers are plentiful.
- **Real concurrency** (goroutines + channels) handles the per-block fan-out cleanly.

### Negative

- **Higher memory overhead** than Rust (~30-50% larger working sets).
- **GC pauses** for long-running aggregates / large batches (mitigated by tuning and small allocations).
- **Less type safety** for complex parser invariants (mitigated by `internal/types/` canonical types).

### Neutral

- Build times comparable.
- Both languages have testcontainers libraries.

## Alternatives considered

### Option A: Rust
- Pro: ~2-3x performance, smaller memory footprint, fearless concurrency.
- Con: No mature Cosmos SDK protobuf decoders. Would need to write/maintain decoders for every Cosmos type ourselves.
- Con: Smaller hiring pool for Pocket-ecosystem contributors.
- **Rejected because**: the marginal performance gain doesn't justify the ecosystem cost. Pocket's volume is well within Go's comfort zone (estimated <50k events/sec peak).

### Option B: TypeScript (status quo)
- Pro: Existing team knowledge from Pocketdex.
- Con: The exact reason we're rewriting.
- **Rejected because**: single-thread + GC + per-block tx is the root cause of current pain.

### Option C: Polyglot (Rust for hot paths, Go elsewhere)
- Pro: best of both.
- Con: 2 build systems, 2 dependency management, FFI overhead, harder to debug.
- **Rejected because**: complexity not justified.

## Implementation notes

- Pin Go to latest stable major in `go.mod` and `Makefile` (`GO_VERSION ?= 1.26` at project start, Feb 2026).
- Use `gofumpt` for stricter formatting consistency.
- Use `golangci-lint` with the standard linter set + `gosec`, `errorlint`, `staticcheck`.

## References

- CLAUDE.md "Stack commitments" section.
