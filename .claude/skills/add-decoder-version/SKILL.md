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
- `docs/research/supplier-shape-breaks.md` (break version map)
- `docs/research/phase-e-spike-findings.md` (proven recipes)
- `internal/router/registry.go` (current DefaultRegistry)

### 2. Vendor the protos

```bash
VERSION=v0.1.6
VERSION_DIR=v0_1_6

mkdir -p third_party/proto/poktroll/$VERSION_DIR
cd /tmp
git clone --depth=1 --branch $VERSION https://github.com/pokt-network/poktroll.git poktroll-$VERSION
# Copy ONLY the pocket/ subtree (not the top-level buf.yaml/buf.lock)
cp -r /tmp/poktroll-$VERSION/proto/pocket /home/overlordyorch/development/pocketscribe/third_party/proto/poktroll/$VERSION_DIR/
```

**Delete any `buf.yaml` / `buf.lock` that came from the vendored tree.** Old poktroll trees contain upstream buf v1 config + BSR lock files that must not leak into the ephemeral workspace and silently pull remote dependencies during offline codegen.

```bash
rm -f third_party/proto/poktroll/$VERSION_DIR/buf.yaml
rm -f third_party/proto/poktroll/$VERSION_DIR/buf.lock
```

### 3. Check for breaking changes

Use a **two-ephemeral-workspace recipe** — never compare bare version directories directly with `buf breaking <dir> --against <dir>`, as buf v2 rejects multiple workspace inputs with conflicting module paths.

```bash
# Build an ephemeral workspace for the previous version
PREV_DIR=v0_1_30  # replace with the actual previous version dir
WS_PREV=$(mktemp -d /tmp/bufbrk-prev-XXXXXX)
WS_NEW=$(mktemp -d /tmp/bufbrk-new-XXXXXX)
ROOT=$(pwd)

for WS in $WS_PREV $WS_NEW; do
  mkdir -p $WS/poktroll
  ln -s $ROOT/third_party/proto/cosmos-sdk/v0_53_0 $WS/cosmos-sdk
  ln -s $ROOT/third_party/proto/wkt                $WS/wkt
  cat > $WS/buf.yaml <<'EOF'
version: v2
modules:
  - path: poktroll
  - path: cosmos-sdk
  - path: wkt
breaking:
  use:
    - WIRE
EOF
done

ln -s $ROOT/third_party/proto/poktroll/$PREV_DIR/pocket $WS_PREV/poktroll/pocket
ln -s $ROOT/third_party/proto/poktroll/$VERSION_DIR/pocket $WS_NEW/poktroll/pocket

(cd $WS_NEW && buf breaking --against $WS_PREV poktroll)
rm -rf $WS_PREV $WS_NEW
```

If `buf breaking` reports breaks:
- Document each break in a new ADR: `docs/decisions/ADR-NNN-poktroll-vX.Y.Z-breaks.md`.
- Determine whether the **supplier closure BFS** is affected (run `TestSupplierShapeGuard` mentally or against the new `.shapes/` snapshot — see `docs/research/supplier-shape-breaks.md`).
- Decide how to handle each break (additive shadow column, dual-write, or ADR with operator signoff for semantic shifts).

### 4. Generate Go types (offline, ephemeral workspace)

**Do NOT add the new version to the root `buf.yaml` workspace.** The root workspace holds only `internal/proto` + shared cosmos-sdk + WKT trees. Each poktroll version is generated in its own ephemeral workspace via the dedicated script:

```bash
bash scripts/gen_decoder_protos.sh $VERSION_DIR
```

This script builds an isolated `/tmp/bufws-*` workspace that symlinks `pocket/` from the vendored tree plus the shared cosmos-sdk + WKT trees, runs `buf generate` with the `buf.gen.poktroll-v0_1_30.yaml` template (substituting version strings), and writes output to `internal/decoders/$VERSION_DIR/gen/`.

Global registrations are stripped automatically when you run `make gen-proto`. To strip a single version manually:

```bash
go run ./tools/stripregister internal/decoders/$VERSION_DIR/gen
go build ./internal/decoders/$VERSION_DIR/gen/...   # must compile
grep -rE '\bproto\.Register' internal/decoders/$VERSION_DIR/gen | wc -l  # expect 0
```

### 5. Scaffold the decoder adapter

Decide if this version **starts a new supplier shape range**:
- Run `go test ./internal/router/ -run TestSupplierShapeGuard -v` after adding a minimal adapter. If the test fails naming this version, it IS a shape-range start.
- If it IS a range start: implement the full supplier decode methods (`DecodeSupplierMsg`, `DecodeSupplierEvent`, `DecodeSupplierKV`) using the version's generated `gen/` types and append the version to `DECODER_GEN_VERSIONS` in the Makefile so `make gen-proto` regenerates it.
- If it is NOT a range start: write a thin delegating adapter pointing at the range-start package (e.g. `v0_1_8` for the `[v0_1_8..v0_1_26]` range).

```bash
# Thin delegating adapter skeleton (non-range-start versions):
cat > internal/decoders/$VERSION_DIR/decoder.go << 'EOF'
// Package vX_Y_Z delegates to the range-owner package.
package vX_Y_Z

import (
    "github.com/pokt-network/pocketscribe/internal/decoders"
    rangeowner "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8"
    "github.com/pokt-network/pocketscribe/internal/types"
)

type Decoder struct{}

func (Decoder) Version() string { return "vX_Y_Z" }

func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
    return decoders.DecodeBlockHeader(metaBytes)
}

func (d Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
    return rangeowner.Decoder{}.DecodeSupplierMsg(typeURL, value)
}

func (d Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
    return rangeowner.Decoder{}.DecodeSupplierEvent(eventType, attrs)
}

func (d Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
    return rangeowner.Decoder{}.DecodeSupplierKV(key, value, deleted)
}
EOF
```

If this version adds **new enum values** to any proto type used in `internal/decoders/enums.go`, update the imports in that file to reference the latest-range package that exposes the expanded enum maps.

### 6. Add unit tests

- Interface satisfaction: `var _ decoders.Decoder = Decoder{}`.
- `Version()` returns the tag.
- For delegating adapters: smoke-test each `Decode*` method returns no panic on a constructed minimal input (coverage via the range-owner's tests).
- For range-start packages: 100% coverage on all hand-written decode methods (CLAUDE.md mandate). Golden tests against real captured fixtures.

### 7. Register in router

Edit `internal/router/registry.go` (`DefaultRegistry()` map):
```go
import vX_Y_Z "github.com/pokt-network/pocketscribe/internal/decoders/vX_Y_Z"

// Inside DefaultRegistry():
"vX_Y_Z": vX_Y_Z.Decoder{},
```

Keep entries in numeric order (v0_1_0, v0_1_8, v0_1_10, ...). Run `TestSupplierShapeGuard` — if this version is a supplier-closure break version, the test will fail until it is registered. A failing shape-guard means supplier rows would be silently mis-decoded under lenient fallback; it must be fixed before merge.

### 8. Capture golden fixtures + update expected.json

- Extract real block-meta + block-data files at the relevant height into `test/fixtures/vX_Y_Z/`.
- Write `block-{H}-expected.json` with correct `pokt1...` operator addresses (cross-check via mainnet LCD, retry up to 15× for uneven-retention backends).
- Extract one stake-msg `Any.value`, one staked-event attrs JSON, one `Supplier/operator_address/` KV pair and one `ServiceConfigUpdate/service_id/` KV pair into `internal/decoders/testdata/supplier/vX_Y_Z/`.
- Add golden tests in `internal/decoders/vX_Y_Z/supplier_golden_test.go`.

### 9. Write the upgrade migration SQL

```sql
-- schema/migrations/NNNN_upgrade_vX_Y_Z.sql
-- +goose Up
INSERT INTO upgrades (name, applied_at_height, applied_at_time, decoder_version, notes)
VALUES (
    '<UPGRADE_NAME>',
    <APPLIED_HEIGHT>,
    (SELECT time FROM block WHERE height = <APPLIED_HEIGHT>),
    'vX_Y_Z',
    'Auto-recorded during decoder onboarding'
) ON CONFLICT (name) DO NOTHING;

-- +goose Down
DELETE FROM upgrades WHERE name = '<UPGRADE_NAME>';
```

Run `ps sync-upgrades` in your local dev environment to populate the `upgrades` table; the migration is the durable record.

### 10. Update docs

- `docs/architecture/05-versioning.md` "Known versions" table → add row.
- If breaking change: `docs/decisions/ADR-NNN-poktroll-vX.Y.Z-breaks.md`.

### 11. Verify

```bash
make gen-check
make ci
golangci-lint run --build-tags=integration ./...
go test -cover ./internal/decoders/...   # 100% on range-start packages
make test-integration
```

The **shape-guard test** (`TestSupplierShapeGuard`) runs as part of `make ci` (`go test ./internal/router/`). It will fail if this version introduces supplier-closure protobuf breaks and lacks a registry entry. Fix: implement + register a range-start decoder package before merging.

### Output report

```
Decoder version vX.Y.Z onboarded.

Vendored:    third_party/proto/poktroll/vX_Y_Z/
Generated:   internal/decoders/vX_Y_Z/gen/
Adapter:     internal/decoders/vX_Y_Z/decoder.go (<range-start | delegates to vA_B_C>)
Registry:    internal/router/registry.go (added "vX_Y_Z")
Migration:   schema/migrations/NNNN_upgrade_vX_Y_Z.sql
Fixtures:    test/fixtures/vX_Y_Z/  +  internal/decoders/testdata/supplier/vX_Y_Z/
Tests:       internal/decoders/vX_Y_Z/decoder_test.go + supplier_golden_test.go
Shape guard: TestSupplierShapeGuard PASS

Breaking changes: <none | see ADR-NNN>
Shape range:      <new range starting here | delegates to vA_B_C range>

Next steps:
1. Run make test-integration to confirm tests 18-21 stay green.
2. Deploy + run ps sync-upgrades on target network.
```
