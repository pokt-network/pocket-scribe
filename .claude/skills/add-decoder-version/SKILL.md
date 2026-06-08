---
name: add-decoder-version
description: Onboard a new poktroll chain version — vendor protos, codegen, scaffold decoder, register in router, add CI matrix. Use when poktroll publishes a new release that introduces protobuf changes.
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# Add a new poktroll decoder version

Use this skill when poktroll releases a new version that introduces (additive or breaking) protobuf changes that affect PocketScribe decoders.

## Inputs

Ask the user for:
1. **Version tag** (e.g. `v0.1.6`). Must be a git tag on `pokt-network/poktroll`.
2. **Approximate mainnet upgrade height** (when this version will go live, or has gone live). Get from `poktrolld query upgrade list-applied-plans` if it's already applied.
3. **Upgrade name** (cosmos-sdk upgrade plan name, e.g. `"v0.1.6-revshare-v2"`).

## Steps

### 1. Read context

- `CLAUDE.md`
- `docs/architecture/05-versioning.md`
- `docs/research/file-plugin-spec.md`
- `internal/router/upgrades.go` (current versions)
- `.claude/agents/pocketscribe-proto-versioner.md` (detailed agent)

### 2. Vendor the protos

```bash
VERSION=v0.1.6
VERSION_DIR=v0_1_6

mkdir -p third_party/proto/poktroll/$VERSION_DIR
cd /tmp
git clone --depth=1 --branch $VERSION https://github.com/pokt-network/poktroll.git poktroll-$VERSION
cp -r /tmp/poktroll-$VERSION/proto/* /home/overlordyorch/development/pocketscribe/third_party/proto/poktroll/$VERSION_DIR/
```

### 3. Check for breaking changes

```bash
cd /home/overlordyorch/development/pocketscribe
buf breaking third_party/proto/poktroll/$VERSION_DIR \
    --against third_party/proto/poktroll/<PREVIOUS_VERSION>
```

If `buf breaking` reports breaks:
- Document each break in a new ADR: `docs/decisions/ADR-NNN-poktroll-vX.Y.Z-breaks.md`.
- Decide how to handle each (additive shadow column, dual-write, ADR with operator signoff if semantic shift).

### 4. Add buf config entry

Edit `buf.yaml`:
```yaml
modules:
  ...
  - path: third_party/proto/poktroll/<NEW_VERSION_DIR>
```

### 5. Generate Go types

```bash
buf generate --path third_party/proto/poktroll/$VERSION_DIR \
    --output internal/decoders/$VERSION_DIR/gen
```

(or use a per-version `buf.gen.<version>.yaml` template.)

### 6. Scaffold decoder

Create `internal/decoders/v0_1_6/decoder.go` implementing the `decoders.Decoder` interface, similar to existing versions. Map each `Decode<Entity>KV` from the new proto types to the canonical types in `internal/types/`.

If the version adds a new field:
- Add the field to the canonical type (nullable pointer).
- This decoder populates the field; older decoders leave it nil.
- Add a migration: `ALTER TABLE <entity>_history ADD COLUMN <field> <type> NULL`.

### 7. Add unit test

`internal/decoders/v0_1_6/decoder_test.go`:
- Golden test for each `Decode*KV` method.
- Use `sebdah/goldie/v2`.
- Fixtures captured from a testnet/mainnet block at this version (use a debug tool — TODO `scripts/tools/capture-kv.go`).

### 8. Register in router

Edit `internal/router/upgrades.go`:
```go
var DefaultUpgrades = []Upgrade{
    // existing
    {Height: 0,        Name: "genesis",      DecoderVersion: "v0_0_10"},
    {Height: 50_000,   Name: "alpha-2",      DecoderVersion: "v0_0_12"},
    {Height: 180_000,  Name: "beta-launch",  DecoderVersion: "v0_1_0"},
    {Height: 420_000,  Name: "rev-share",    DecoderVersion: "v0_1_5"},
    {Height: <NEW_H>,  Name: "<NEW_NAME>",   DecoderVersion: "v0_1_6"},  // ADD
}
```

And the migration for the `upgrades` table:
```sql
-- schema/migrations/NNNN_upgrade_v0_1_6.sql
INSERT INTO upgrades (name, applied_at_height, applied_at_time, decoder_version, notes)
VALUES (
    '<NEW_NAME>',
    <NEW_H>,
    (SELECT time FROM block WHERE height = <NEW_H>),
    'v0_1_6',
    'Auto-recorded during decoder onboarding'
) ON CONFLICT (name) DO NOTHING;
```

### 9. Add to CI matrix

Edit `.github/workflows/proto-matrix.yml`:
```yaml
matrix:
  proto_version: [v0_0_10, v0_0_12, v0_1_0, v0_1_5, v0_1_6]  # ADD
```

### 10. Update docs

- `docs/architecture/05-versioning.md` "Known versions" table → add row.
- If breaking change: `docs/decisions/ADR-NNN-poktroll-vX.Y.Z-breaks.md`.

### 11. Verify

```bash
make gen-check
make lint
make test
make test-integration
```

### Output report

```
✅ Decoder version v0.1.6 onboarded.

Vendored: third_party/proto/poktroll/v0_1_6/
Generated: internal/decoders/v0_1_6/gen/
Decoder: internal/decoders/v0_1_6/decoder.go
Router updated: internal/router/upgrades.go (height: <H>)
Migration: schema/migrations/NNNN_upgrade_v0_1_6.sql
Tests: internal/decoders/v0_1_6/decoder_test.go (TODO: capture fixtures)
CI matrix: .github/workflows/proto-matrix.yml
Docs: docs/architecture/05-versioning.md

Breaking changes: <none | see ADR-NNN>

Next steps:
1. Capture golden fixtures from a node at this version (scripts/tools/capture-kv.go).
2. Implement the golden test bodies.
3. Run `make test-integration -run TestDecoder.*CrossVersion`.
4. Deploy.
```
