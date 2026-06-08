# ADR-008: Versioned proto decoders with height-based router

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

poktroll has had multiple upgrades since Shannon launch and will have many more. Each upgrade can change:
- Add fields to protobuf messages (additive).
- Rename or remove fields (breaking).
- Change semantic meaning (silently breaking).
- Add new event types.

An indexer that lives for years must decode every height since genesis correctly — including heights from chain versions that no longer exist as live binaries.

Two strategies were considered:
- **Latest decoder for everything** (try-and-fallback when fields are missing): fragile; silent breakage when proto fields are reused for different purposes.
- **Versioned decoders, dispatched by height** (chosen): explicit, deterministic, debuggable.

## Decision

Maintain **one decoder package per chain version**, vendored protos per version, generated types per version. A **router** picks the correct decoder for a given `block_height` based on an `upgrades` table.

## Consequences

### Positive

- **Deterministic**: same height always produces the same decoded output.
- **Survivable**: works for any height from genesis to current, regardless of how many versions have passed.
- **Debuggable**: when something looks off, `decoders.For(H).Version()` tells you exactly which decoder ran.
- **Testable cross-version**: golden fixtures per version; CI matrix runs all versions.
- **Breaking changes are loud**: `buf breaking` catches them; ADRs document them.

### Negative

- **Storage of vendored protos** — modest; each version is a few MB.
- **Cognitive overhead** — N decoders to maintain. Mitigated by the `Decoder` interface keeping them parallel.
- **Generated code is duplicated** per version (each has its own `gen/`). Acceptable.
- **Maintenance burden** when poktroll releases a new version. Mitigated by `/generate-decoder` skill that automates the bulk of onboarding.

### Neutral

- Build time slightly longer (more packages).

## Alternatives considered

### Option A: Single latest decoder
- Pro: less code.
- **Con (fatal)**: silent breakage when proto fields are renamed or repurposed.
- **Rejected because**: silent breakage is the worst kind.

### Option B: Best-effort decoder with try-each-version fallback
- Pro: handles unknown heights gracefully.
- Con: non-deterministic; which version's "successful" decode wins?
- Con: 5x decode work per message (try v0_0_10, v0_0_12, etc.).
- **Rejected because**: determinism > convenience.

### Option C: One decoder per breaking change (not per release)
- Pro: fewer decoders to maintain (most releases are additive).
- Con: harder to know which "breaking change generation" a release belongs to.
- Con: doesn't catch additive changes that need new fields populated.
- **Rejected because**: per-release versioning is the simpler heuristic; additive changes are cheap (decoder just sets new field).

## Implementation notes

### Folder layout

```
third_party/proto/poktroll/v{X}_{Y}_{Z}/    # vendored protos
internal/decoders/v{X}_{Y}_{Z}/             # decoder package
  ├── decoder.go                            # implements Decoder interface
  └── gen/                                  # buf-generated Go types
internal/router/
  ├── router.go                             # height → decoder
  └── upgrades.go                           # static fallback for upgrade heights
```

### Versioning protocol

- Each poktroll **release** = potentially a new decoder version.
- Each decoder version exists under `internal/decoders/v{X}_{Y}_{Z}/` matching the release tag.
- The `upgrades` table maps `applied_at_height → decoder_version`.
- Router does `for i := len(upgrades) - 1; i >= 0; i-- { if h >= upgrades[i].Height { return decoder[upgrades[i].DecoderVersion] } }`.

### Upgrade table sources

1. **On-chain authoritative**: `poktrolld query upgrade list-applied-plans` returns historical upgrades with heights.
2. **Hardcoded fallback** in `internal/router/upgrades.go` for bootstrap before DB is populated.
3. **`upgrades` table in Postgres** populated from on-chain queries on startup + reconciler verification.

### Cross-version testing

Every PR runs the **CI matrix** (`.github/workflows/proto-matrix.yml`):
```yaml
matrix:
  proto_version: [v0_0_10, v0_0_12, v0_1_0, v0_1_5]
```

Per-version tests + golden tests run for each cell.

### Breaking change protocol

When a new version introduces a breaking change:
1. `buf breaking third_party/proto/poktroll/v{NEW} --against third_party/proto/poktroll/v{OLD}` flags it.
2. Write `docs/decisions/ADR-NNN-poktroll-v{X.Y.Z}-breaks.md` documenting the break and our mapping strategy.
3. Update canonical types (`internal/types/`) if the break affects the canonical shape.
4. Add a cross-version test asserting old goldens still decode (with new fields nil) and new goldens decode with the new field populated.

## References

- Full session transcript: Topic 17 ("versioning strategy" in consolidated decisions).
- `docs/architecture/05-versioning.md` — full workflow.
- `.claude/agents/pocketscribe-proto-versioner.md` — onboarding skill.
- CLAUDE.md "Multi-version protos" rules.
