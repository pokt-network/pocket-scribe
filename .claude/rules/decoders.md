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

Anti-patterns:
- ❌ Editing `gen/` files directly.
- ❌ Calling `time.Now()` in decoder code (Invariant 1).
- ❌ Throwing/panicking on parse error — return the error.
- ❌ One decoder version handling multiple chain versions via try-catch — split into separate decoder packages.

See `docs/architecture/05-versioning.md`, ADR-008, `.claude/agents/pocketscribe-proto-versioner.md`.
