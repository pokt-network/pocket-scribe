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

### 4. Vendor the well-known-type + cosmos-sdk protos (offline codegen)

If this version pins a cosmos-sdk not yet vendored, the `generate-decoder`
skill already vendored `third_party/proto/cosmos-sdk/<csdk>/`. Ensure the
well-known-type protos exist under `third_party/proto/wkt/` (they are shared
across all versions and were vendored in Slice 1 Phase C). Then add the new
version's module path to the root `buf.yaml` workspace:

```yaml
version: v2
modules:
  - path: third_party/proto/poktroll/<NEW_VERSION_DIR>
  - path: third_party/proto/poktroll/v0_1_30
  - path: third_party/proto/cosmos-sdk/<csdk_dir>
  - path: third_party/proto/wkt
```

### 5. Generate Go types (buf, fully offline)

Copy `buf.gen.poktroll-v0_1_30.yaml` to `buf.gen.poktroll-<NEW_VERSION_DIR>.yaml`,
then replace every `v0_1_30` occurrence with `<NEW_VERSION_DIR>` (the `out:`
path and all 9 `go_package` override values). Generate:

```bash
make tools-proto    # idempotent: installs pinned buf + protoc-gen-gocosmos
PATH="$(go env GOPATH)/bin:$PATH" buf generate \
  --template buf.gen.poktroll-<NEW_VERSION_DIR>.yaml \
  third_party/proto/poktroll/<NEW_VERSION_DIR>
go build ./internal/decoders/<NEW_VERSION_DIR>/gen/...   # must compile
```

Managed mode rewrites poktroll `go_package` under our module so versions
coexist; it is DISABLED for cosmos/tendermint/amino/gogoproto/cosmos_proto/google
so those imports resolve to the real Go modules. Generated `gen/` is committed
and read-only (ADR-008); never hand-edit — regenerate via `make gen-proto`.

### 6. Scaffold the decoder adapter

```bash
test -f internal/decoders/<NEW_VERSION_DIR>/decoder.go || \
  scripts/scaffold_decoder.sh <NEW_VERSION_DIR> > internal/decoders/<NEW_VERSION_DIR>/decoder.go
```

The scaffold implements the current `decoders.Decoder` interface. Hand-fill the
version-specific methods (entity/tx/event decoders) using the generated `gen/`
types, mapping to canonical `internal/types`. The block header needs nothing — it
delegates to the shared, version-invariant `decoders.DecodeBlockHeader`.

### 7. Add unit tests

- Interface satisfaction: `var _ decoders.Decoder = Decoder{}`.
- `Version()` returns the tag.
- Golden tests for each implemented `Decode*` method against real captured
  fixtures (`sebdah/goldie/v2` once fixtures exist; block-header is covered by
  the shared `internal/decoders/blockheader_test.go`). Coverage: 100% of
  hand-written decoder methods (CLAUDE.md mandate).

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
