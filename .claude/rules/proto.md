---
paths:
  - "**/*.proto"
---

# Protobuf rules

When editing `.proto` files:

1. **Our protos** (envelope, internal RPC) live in `internal/proto/`. Edit freely; run `make gen-proto` after.
2. **Vendored protos** (poktroll, cosmos-sdk) live in `third_party/proto/`. **Do not edit directly.** They are vendored snapshots of upstream repos. If a version needs changes, re-vendor it (or apply a patch via `make sync-protos`).
3. **Run `buf format`** before committing.
4. **Run `buf lint`** to catch issues.
5. **Run `buf breaking`** against the previous version when introducing a new poktroll version.
6. **Field numbers are immutable** once published. Adding fields = next available number. Removing fields = mark as `reserved <number>;`.

Anti-patterns:
- ❌ Editing files under `third_party/proto/` directly.
- ❌ Reusing field numbers.
- ❌ Renaming fields without keeping the original.

See ADR-008, `docs/architecture/05-versioning.md`.
