---
paths:
  - "internal/decoders/**/*.go"
  - "third_party/proto/poktroll/**"
---

# Decoder rules

When editing decoder packages or vendored protos:

1. **Versioned, never modified.** Each `internal/decoders/v{X}_{Y}_{Z}/` corresponds to a poktroll release tag. Once committed, only fix bugs; never repurpose for a different version.
2. **Generated code is read-only.** `internal/decoders/v*/gen/` is buf-generated. Never hand-edit. If you need to change, regenerate via `make gen-proto`.
3. **Implement the `Decoder` interface** (`internal/decoders/decoder.go`). All methods return `(*types.<X>Snapshot, error)`.
4. **Map to canonical types** in `internal/types/`. The canonical type is the union of all fields that have ever existed across versions; older decoders leave newer fields nil.
5. **Set `proto_version`** field on every snapshot — e.g. `"v0_1_5"`.
6. **Errors are returned, never logged** at this layer. Let consumers decide what to do with errors.
7. **Golden test required** for every `Decode*KV` method, every version. Use `sebdah/goldie/v2`.
8. **Cross-version test required** in `test/integration/proto_version_compat_test.go` to assert canonical equivalence.

9. **Never ASSUME shape stability between versions — for ANY entity.** Shape
   histories are non-monotonic: real poktroll entities had fields REMOVED, then
   RE-ADDED months later, then CHANGED again; and `x/<module>/types/keys.go`
   layouts drift too (real case: `ServiceConfigUpdate/operator_address/` index
   component ordering changed v0.1.8→v0.1.12). Every "no change between vX and
   vY" claim must be MACHINE-DERIVED: proto-source diff or `.shapes`
   recomputation over the FULL transitive closure, consecutive-pair across the
   whole range (consecutive pairs are mandatory — they catch remove→re-add→change
   cycles that endpoint diffs swallow). Never cite `spine-shape-evolution.md`
   (per-message, non-transitive — proven to hide breaks), changelogs, or version
   adjacency as evidence. `.shapes` blind spots (enums, `reserved` ranges):
   verify against the vendored `.proto` sources when those matter.
10. **Expand the shape-guard BEFORE the decoder.** When a new module/entity
    enters decode scope, add its transitive closure to the seed list in
    `internal/router/shapeguard_test.go` first — let CI tell you which versions
    need decoder packages.

Anti-patterns:
- ❌ Editing `gen/` files directly.
- ❌ Calling `time.Now()` in decoder code (Invariant 1).
- ❌ Throwing/panicking on parse error — return the error.
- ❌ One decoder version handling multiple chain versions via try-catch — split into separate decoder packages.
- ❌ "Versions vX..vY look the same, reuse the decoder" without a machine-derived shape proof (rule 9).

See `docs/architecture/05-versioning.md`, ADR-008, `.claude/agents/pocketscribe-proto-versioner.md`.
