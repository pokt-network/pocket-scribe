# Slice 1 Phase E — Supplier Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ps consumer supplier` indexes the supplier lifecycle (tx msgs, events, KV state) end-to-end across 5 versions, fed by the ADR-022 sidecar fan-out, with per-height seal requiring block AND supplier (spec §9 Phase E, tests 18–21).

**Architecture:** The sidecar gains the ADR-022 fan-out: it structurally splits `block-{H}-meta` + `block-{H}-data` into per-tx (`pokt.tx.{H}.{idx}`), per-event (`pokt.events.{type}.{H}`) and per-KV (`pokt.kv.{store}.{H}`) messages using ONLY version-invariant containers (cometbft abci + cosmos tx/StoreKVPair — no poktroll decode, no router in the sidecar), then publishes a metadata-only `BlockEnvelope` on `pokt.block.{H}` **LAST** (the ordering contract). The supplier consumer runs a new `BatchRuntime` (ADR-024): buffer fan-out per height, flush in ONE Postgres tx when the envelope arrives (ack-after-commit preserved; quiet heights flush empty and advance — no cursor freeze). Version-specific decode uses 3 shape-range gen trees (`v0_1_0` [0..7], `v0_1_8` [8..26], `v0_1_27` [27..33]) with init-registration stripped (gogoproto duplicate-enum panic), a shape-complete LENIENT registry (`+v0_1_8 +v0_1_27`), and a CI shape-guard test that fails when a future break version is unregistered.

**Tech Stack:** Go 1.26, buf v1.70.0 + protoc-gen-gocosmos v1.7.0 (offline, ephemeral workspaces), gogoproto v1.7.0, cosmos-sdk v0.53.0 types, NATS JetStream (nats.go v1.46.1, multi-FilterSubjects), pgx v5, goose, testcontainers.

**Ground-truth inputs (read before implementing):**
- `docs/research/supplier-shape-breaks.md` — verified break map (two independent methods agree: supplier closure breaks ONLY at v0_1_8 and v0_1_27; migration-module types also break at v0_1_2/v0_1_12 but are OUT of Phase E scope).
- `docs/research/phase-e-spike-findings.md` — verbatim PROVEN recipes (codegen, strip, coexistence, real-bytes decode of mainnet blocks 135836/135837, supplier KV key table, event attribute encoding).
- Spec `docs/superpowers/specs/2026-06-08-slice-1-design.md` §4.8, §8.1, §9 Phase E, §11.1 tests 18–21; ADR-022/024/025/027.

**Phase E decisions (user-ratified 2026-06-09):**
1. Full ADR-022 fan-out; sidecar split is STRUCTURAL (version-invariant); `pokt.block.{H}` becomes the metadata-only envelope; block consumer becomes envelope→row mapper.
2. Shape-complete lenient registry (add `v0_1_8`, `v0_1_27`) + mechanical CI shape-guard. No strict router variant.
3. 3 gen trees at range starts + `tools/stripregister` post-gen transform (incl. `v0_1_30/gen`).
4. v0_1_0 era supplier fixture is a NEGATIVE fixture (zero supplier activity on-chain; exercises the quiet-height path).
5. NEW table `supplier_service_config_update_history`: from v0.1.8 the chain stores `Supplier` DEHYDRATED; ServiceConfigUpdate primaries are first-class chain state (invariant 3 — hydrate at query time, never at write time).
6. OUT of Phase E scope (documented, not forgotten): supplier `MsgUpdateParam` (shared table lacks module discriminator), `pocket.migration.MsgClaimMorseSupplier`/`EventMorseSupplierClaimed` (own consumer later; supplier rows still arrive via KV), tokenomics `EventSupplierSlashed`, ADR-024 size/time partial-flush valves (Phase G), KV `delete=true` Supplier records (skip + WARN + metric; unbond-end captured via events).
7. Msgs are indexed only from txs with `ExecTxResult.Code == 0` (failed txs change no state; events/KV only exist for successful ones).
8. `decoded_by_version` records the REGISTERED decoder package the router returned (`Decoder.Version()` → `decoder_version.id` via tag lookup), not the delegation target.

**Hard rules (carry-over):** no AI attribution in commits; TDD per task; `make ci` green per task; integration tests behind `//go:build integration`; lint clean incl. `--build-tags=integration`; 100% coverage on decoders, 80% on internal/; archeology module stays isolated; never modify an existing migration — new ones only.

---

## File map (what this phase touches)

| Area | Files |
|---|---|
| Codegen | `scripts/gen_decoder_protos.sh` (new), `tools/stripregister/main.go` (new), `Makefile` (gen-proto/gen-check rewrite), `internal/decoders/{v0_1_0,v0_1_8,v0_1_27}/gen/` (new, generated+stripped), `internal/decoders/v0_1_30/gen/` (re-gen + stripped) |
| Envelope | `internal/proto/pocketscribe/v1/envelope.proto` (new), `buf.gen.envelope.yaml` (new), `buf.yaml` (+1 module), `internal/proto/gen/` (generated) |
| Subjects | `internal/nats/subjects.go` (+tx/events/kv grammars), `internal/nats/subjects_test.go` |
| Decoders | `internal/decoders/decoder.go` (+3 methods), `internal/decoders/enums.go` (new), `internal/decoders/supplierevents.go` (new), `internal/decoders/supplierkv.go` (new), `internal/decoders/meta.go` (new, SplitMeta), `internal/decoders/{v0_1_0,v0_1_8,v0_1_27}/supplier.go` (range impls), `internal/decoders/{v0_1_10,v0_1_20,v0_1_28,v0_1_29,v0_1_30}/supplier.go` (delegates), `internal/decoders/v0_1_8/decoder.go` + `v0_1_27/decoder.go` (new packages) |
| Types | `internal/types/supplier.go` (new), `internal/types/event.go` (new) |
| Router | `internal/router/registry.go` (+2 entries), `internal/router/shapeguard_test.go` (new) |
| Schema | `schema/migrations/0040_supplier_service_config_update.sql` (new) |
| Store | `internal/store/supplier.go` (new), `internal/store/decoder_version.go` (new) |
| Sidecar | `internal/fileplugin/bootstrap.go` (fan-out rewrite), `internal/app/fileplugin/cmd.go` (+--config) |
| Consumers | `internal/consumer/batch.go` (new BatchRuntime), `internal/consumer/supplier/handler.go` (new), `internal/consumer/block/handler.go` (envelope migration), `internal/app/consumer/block.go` (drop router), `internal/app/consumer/supplier.go` (new), `internal/app/consumer/cmd.go` (+supplier), `internal/metrics/metrics.go` (+Buffered gauge) |
| Fixtures | `test/fixtures/v0_1_*/block-*-{meta,data}` (+ -data for existing, + 4 new supplier heights), per-height `*-expected.json` (multi-table), `internal/decoders/testdata/supplier/` (golden blobs) |
| Tests | `test/integration/supplier_consumer_test.go` (tests 18–21, new), `test/integration/fileplugin_test.go` (fan-out rewrite), `test/integration/block_consumer_test.go` (envelope migration), `test/testcontainers/postgres.go` (Reset += supplier tables) |
| Docs | ADR-022/024/025 Accepted + ordering-contract amendment, `docs/architecture/05-versioning.md` correction, `.claude/skills/add-decoder-version/SKILL.md` rewrite, spec Phase E marker |

Execution order is Task 1 → 13. Tasks 2–7 are independent of 8–10 except where noted; run sequentially anyway (each commits atomically).

---

### Task 1: Accept + amend the ADRs the implementation relies on

**Files:**
- Modify: `docs/decisions/ADR-022-nats-payload-discipline.md`
- Modify: `docs/decisions/ADR-024-consumer-batching.md`
- Modify: `docs/decisions/ADR-025-indexer-coordination.md`

- [ ] **Step 1: ADR-022 — status + ordering contract + envelope fields.** Change `**Status**: Proposed` → `**Status**: Accepted (Slice 1 Phase E, 2026-06-09)`. Fix the false claim at lines ~30-31: replace `(already defined in internal/nats/subjects.go)` on both the events and kv lines with `(constructors in internal/nats/subjects.go as of Phase E)`. Append this section before `## References` (or at the end if no such header):

```markdown
## Amendment (Phase E, 2026-06-09): ordering contract + envelope encoding

1. **Ordering contract (load-bearing):** for every height H the sidecar publishes
   ALL fan-out messages (`pokt.tx.*`, `pokt.events.*`, `pokt.kv.*`) BEFORE the
   `pokt.block.{H}` envelope. JetStream delivers a durable's messages in stream
   sequence order, so a consumer that receives the envelope for H has already
   received every fan-out message for H matching its filters. The envelope is
   the per-height completeness fence ADR-024 batches on. Enforced by test in
   `test/integration/fileplugin_test.go`.
2. **Envelope encoding:** `pocketscribe.v1.BlockEnvelope` (gogo proto,
   `internal/proto/pocketscribe/v1/envelope.proto`): height, time_unix_nano,
   hash, proposer_address, chain_id (from network config — NOT in the ABCI
   header), tx_count, event_count, kv_count, published_msg_count (ADR-025).
   The event-type subject token replaces `.` with `_`
   (`pokt.events.pocket_supplier_EventSupplierStaked.{H}`) because `.` is the
   NATS token separator.
3. **Per-tx payload** is `pocketscribe.v1.TxWithResult` (raw cosmos tx bytes +
   raw `abci.ExecTxResult` bytes). **Per-event payload** is
   `pocketscribe.v1.EventInBlock` (raw `abci.Event` bytes + tx_index +
   event_index; tx_index = -1 for block-level events). **Per-KV payload** is the
   `cosmos.store.v1beta1.StoreKVPair` wire bytes (the uvarint length prefix is
   stripped by the framing reader — payload only, never the framing).
4. The 256 KiB / 1 MiB caps now hold by construction: the largest observed
   single fan-out message on supplier-heavy mainnet blocks is ~19.9 KiB
   (`docs/research/phase-e-spike-findings.md` §5). Cap *enforcement* (WARN/refuse)
   remains Phase G (test 27).
```

- [ ] **Step 2: ADR-024 — status + Phase E scoping note.** Status → `Accepted (Slice 1 Phase E, 2026-06-09)`. Append:

```markdown
## Amendment (Phase E, 2026-06-09): implementation scoping

Phase E implements the block-boundary fence (trigger 1) in
`internal/consumer/batch.go`. The size cap (trigger 2) and time cap (trigger 3)
partial-flush valves are deferred to Phase G hardening — bootstrap replays are
bounded and the envelope follows the fan-out immediately. The buffer dedups
redeliveries by Nats-Msg-Id so an AckWait redelivery cannot double-buffer.
Quiet heights (zero fan-out messages for a consumer's filters) flush an EMPTY
batch when the envelope arrives — this is what advances the supplier cursor
over heights with no supplier activity.
```

- [ ] **Step 3: ADR-025 — status only.** Status → `Accepted (Slice 1 Phase E, 2026-06-09)`. Add one line under the Decision heading: `Phase E implements the envelope counts as metadata; the per-consumer count cross-check and `ps indexed-height-publisher` remain future work — per-height completeness in Phase E comes from the ADR-022 ordering contract.`

- [ ] **Step 4: Commit**

```bash
git add docs/decisions/ADR-022-nats-payload-discipline.md docs/decisions/ADR-024-consumer-batching.md docs/decisions/ADR-025-indexer-coordination.md
git commit -m "docs(adr): accept ADR-022/024/025 with Phase E ordering contract + scoping amendments"
```

---

### Task 2: Codegen infrastructure — ephemeral-workspace script, stripregister, 3 gen trees

**Files:**
- Create: `scripts/gen_decoder_protos.sh`
- Create: `tools/stripregister/main.go`
- Create: `tools/stripregister/main_test.go`
- Modify: `Makefile` (gen-proto / gen-check)
- Generated: `internal/decoders/{v0_1_0,v0_1_8,v0_1_27}/gen/` (new), `internal/decoders/v0_1_30/gen/` (stripped)

Background: buf v2 cannot hold two poktroll trees in one workspace (`pocket/application/event.proto is contained in multiple modules`); two unstripped gen trees in one binary PANIC at init (`proto: duplicate enum registered: pocket.shared.RPCType`, gogoproto v1.7.0 `properties.go:526`). Both PROVEN in the spike; recipes below are verbatim-from-spike.

- [ ] **Step 1: Write the stripregister test first** — `tools/stripregister/main_test.go`:

```go
package main

import (
	"bytes"
	"testing"
)

const in = `package shared

func init() {
	proto.RegisterType((*Supplier)(nil), "pocket.shared.Supplier")
	proto.RegisterEnum("pocket.shared.RPCType", RPCType_name, RPCType_value)
	proto.RegisterMapType((map[string]string)(nil), "pocket.shared.M.Entry")
}

func init() { proto.RegisterFile("pocket/shared/supplier.proto", fileDescriptor_aabb) }

func init() {
	proto.RegisterType((*X)(nil), "pocket.shared.X")
	somethingElse()
}

func keep() {}
`

const want = `package shared

func init() {
	somethingElse()
}

func keep() {}
`

func TestTransformStripsRegistrationsOnly(t *testing.T) {
	got := transform([]byte(in))
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("transform mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestTransformIdempotent(t *testing.T) {
	once := transform([]byte(in))
	twice := transform(once)
	if !bytes.Equal(once, twice) {
		t.Fatal("transform is not idempotent")
	}
}
```

- [ ] **Step 2: Run it — must FAIL** (`transform` undefined): `go test ./tools/stripregister/` → compile error.

- [ ] **Step 3: Create `tools/stripregister/main.go`** — verbatim from the spike (PROVEN: 189 files rewritten, idempotent, `gofmt -l` clean, `grep -rE 'proto\.Register'` → 0):

```go
// stripregister removes gogoproto GLOBAL registry registration from generated
// .pb.go files so that multiple poktroll decoder versions can coexist in one
// binary (gogoproto's proto.RegisterEnum PANICS on duplicate full names; the
// fully-qualified proto names "pocket.shared.RPCType" etc. are identical
// across every vendored poktroll version).
//
// It strips, inside init() functions only:
//   - proto.RegisterType / proto.RegisterEnum / proto.RegisterMapType lines
//     (multi-line init blocks; the block is dropped entirely if it becomes empty)
//   - single-line `func init() { proto.RegisterFile(...) }` declarations
//   - golang_proto.* variants of all of the above (defensive; gocosmos does
//     not emit them without the gogoproto_registration option)
//
// Everything else is left byte-identical. The transform is deterministic and
// idempotent: running it twice produces the same bytes, so it is safe inside
// gen-check.
//
// Usage: go run ./tools/stripregister <gen-dir> [<gen-dir>...]
package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// `	proto.RegisterType((*EventSupplierStaked)(nil), "pocket.supplier.EventSupplierStaked")`
	reRegisterLine = regexp.MustCompile(`^\t(proto|golang_proto)\.Register(Type|Enum|MapType|File)\(.*\)$`)
	// `func init() { proto.RegisterFile("pocket/supplier/event.proto", fileDescriptor_x) }`
	reInitOneLine = regexp.MustCompile(`^func init\(\) \{ (proto|golang_proto)\.RegisterFile\(.*\) \}$`)
)

func transform(src []byte) []byte {
	lines := strings.Split(string(src), "\n")
	var out []string
	// skipBlank swallows the blank line following a dropped declaration when
	// the previously emitted line is already blank, so the output stays
	// gofmt-clean (gofmt collapses consecutive blank lines).
	skipBlank := func(i int) int {
		if i+1 < len(lines) && lines[i+1] == "" && len(out) > 0 && out[len(out)-1] == "" {
			return i + 1
		}
		return i
	}
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if reInitOneLine.MatchString(line) {
			i = skipBlank(i)
			continue // drop the whole single-line init
		}
		if line == "func init() {" {
			// Buffer the block; filter register lines; drop block if emptied.
			var body []string
			j := i + 1
			for ; j < len(lines) && lines[j] != "}"; j++ {
				if !reRegisterLine.MatchString(lines[j]) {
					body = append(body, lines[j])
				}
			}
			if len(body) > 0 {
				out = append(out, line)
				out = append(out, body...)
				out = append(out, "}")
				i = j // skip past closing brace
			} else {
				i = skipBlank(j) // block dropped entirely
			}
			continue
		}
		out = append(out, line)
	}
	return []byte(strings.Join(out, "\n"))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: stripregister <gen-dir> [<gen-dir>...]")
		os.Exit(2)
	}
	changed := 0
	for _, root := range os.Args[1:] {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".pb.go") {
				return err
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			dst := transform(src)
			if !bytes.Equal(src, dst) {
				if err := os.WriteFile(path, dst, 0o644); err != nil {
					return err
				}
				changed++
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "stripregister: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("stripregister: rewrote %d files\n", changed)
}
```

- [ ] **Step 4: Run the test — must PASS**: `go test ./tools/stripregister/` → `ok`.

- [ ] **Step 5: Create `scripts/gen_decoder_protos.sh`** — verbatim from the spike (PROVEN deterministic; symlink ONLY `pocket/` because v0_1_0/v0_1_10 vendored trees still contain upstream v1 `buf.yaml`+`buf.lock` that silently pull BSR deps):

```bash
#!/usr/bin/env bash
set -euo pipefail
V="${1:?usage: gen_decoder_protos.sh <version_dir e.g. v0_1_8>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$(go env GOPATH)/bin:$PATH"
SRC="$ROOT/third_party/proto/poktroll/$V"
TEMPLATE="$ROOT/buf.gen.poktroll-v0_1_30.yaml"
OUT="$ROOT/internal/decoders/$V/gen"
[ -d "$SRC/pocket" ] || { echo "FATAL: $SRC/pocket not found"; exit 1; }
WS="$(mktemp -d /tmp/bufws-$V-XXXXXX)"; trap 'rm -rf "$WS"' EXIT
# Symlink ONLY pocket/: old vendored trees (v0_1_0/v0_1_10) still contain
# upstream buf.yaml v1 + buf.lock (BSR deps) which must not leak into the ws.
mkdir -p "$WS/poktroll"
ln -s "$SRC/pocket"                                "$WS/poktroll/pocket"
ln -s "$ROOT/third_party/proto/cosmos-sdk/v0_53_0" "$WS/cosmos-sdk"
ln -s "$ROOT/third_party/proto/wkt"                "$WS/wkt"
cat > "$WS/buf.yaml" <<'EOF'
version: v2
modules:
  - path: poktroll
  - path: cosmos-sdk
  - path: wkt
EOF
sed -e "s/v0_1_30/$V/g" \
    -e "s#^    out: internal/decoders/$V/gen\$#    out: $OUT#" \
    "$TEMPLATE" > "$WS/buf.gen.yaml"
grep -q "out: $OUT" "$WS/buf.gen.yaml" || { echo "FATAL: out: rewrite failed"; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT"
(cd "$WS" && buf generate --template buf.gen.yaml poktroll)
N="$(find "$OUT" -name '*.pb.go' | wc -l)"
[ "$N" -gt 0 ] || { echo "FATAL: buf exited 0 but produced no .pb.go"; exit 1; }
echo "OK: generated $N .pb.go files under internal/decoders/$V/gen/"
```

`chmod +x scripts/gen_decoder_protos.sh`.

- [ ] **Step 6: Rewrite the Makefile codegen targets.** Replace the current `gen-proto` and `gen-check` recipes (Makefile, "Proto codegen (buf)" section) with:

```make
# Versions that own a generated tree (shape-range representatives + v0_1_30,
# which Phase C committed; range map: docs/research/supplier-shape-breaks.md).
DECODER_GEN_VERSIONS := v0_1_0 v0_1_8 v0_1_27 v0_1_30

gen-proto: tools-proto ## Generate decoder bindings (offline, ephemeral buf workspaces) + envelope, then strip global registrations
	@for v in $(DECODER_GEN_VERSIONS); do \
	  bash scripts/gen_decoder_protos.sh $$v || exit 1; \
	done
	@PATH="$(PROTO_BIN):$$PATH" buf generate --template buf.gen.envelope.yaml internal/proto
	@go run ./tools/stripregister $(foreach v,$(DECODER_GEN_VERSIONS),internal/decoders/$(v)/gen)
	@echo "Generated + stripped: $(DECODER_GEN_VERSIONS) and internal/proto/gen/"

gen-check: ## Verify committed generated code matches the protos (regenerate + strip + diff)
	@$(MAKE) gen-proto >/dev/null
	@if ! git diff --quiet -- $(foreach v,$(DECODER_GEN_VERSIONS),internal/decoders/$(v)/gen) internal/proto/gen; then \
	  echo "generated code is stale; run 'make gen-proto' and commit the result:"; \
	  git --no-pager diff --stat -- internal/decoders/*/gen internal/proto/gen; \
	  exit 1; \
	fi
	@echo "generated code up to date."
```

NOTE: `buf.gen.envelope.yaml` + `internal/proto` arrive in Task 3. Until then run the loop manually (next step) — do NOT run `make gen-proto` in this task.

- [ ] **Step 7: Generate + strip the trees** (v0_1_30 regenerates byte-identical then gets stripped — a large one-time diff):

```bash
for v in v0_1_0 v0_1_8 v0_1_27 v0_1_30; do bash scripts/gen_decoder_protos.sh $v; done
go run ./tools/stripregister internal/decoders/v0_1_0/gen internal/decoders/v0_1_8/gen internal/decoders/v0_1_27/gen internal/decoders/v0_1_30/gen
go build ./internal/decoders/... && gofmt -l internal/decoders | tee /dev/stderr | wc -l   # expect 0
grep -rE '\bproto\.Register' internal/decoders/*/gen | wc -l                              # expect 0
```

Expected: `OK: generated 63 .pb.go files` ×4, `stripregister: rewrote <N> files`, build clean.

- [ ] **Step 8: Run unit tests + lint**: `go test ./tools/... && make ci` (ci does not yet call the new gen-check path with envelope — fine; gen-check becomes fully green after Task 3).

- [ ] **Step 9: Commit**

```bash
git add scripts/gen_decoder_protos.sh tools/stripregister Makefile internal/decoders/v0_1_0/gen internal/decoders/v0_1_8/gen internal/decoders/v0_1_27/gen internal/decoders/v0_1_30/gen
git commit -m "feat(codegen): ephemeral-workspace gen script + stripregister; gen trees for v0_1_0/v0_1_8/v0_1_27 (ADR-008 shape ranges)"
```

NOTE: `internal/decoders/v0_1_8/` and `v0_1_27/` contain ONLY `gen/` after this task — the adapter packages arrive in Task 5. That is fine: nothing imports them yet.

---

### Task 3: `internal/proto` — BlockEnvelope / TxWithResult / EventInBlock

**Files:**
- Create: `internal/proto/pocketscribe/v1/envelope.proto`
- Create: `buf.gen.envelope.yaml`
- Modify: `buf.yaml` (add module)
- Create: `internal/proto/envelope_test.go`
- Generated: `internal/proto/gen/pocketscribe/v1/envelope.pb.go`

- [ ] **Step 1: Create `internal/proto/pocketscribe/v1/envelope.proto`:**

```proto
syntax = "proto3";

package pocketscribe.v1;

option go_package = "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1";

// BlockEnvelope is the metadata-only per-height message on pokt.block.{H}
// (ADR-022 rule 3: never the transactional body). The sidecar publishes it
// AFTER every fan-out message for H — consumers use it as the per-height
// completeness fence (ADR-024 ordering contract).
message BlockEnvelope {
  int64  height              = 1;
  // Consensus header time as unix nanos (invariant #1 source; decoded by the
  // sidecar from RequestFinalizeBlock via the shared version-invariant
  // decoders.DecodeBlockHeader — no router involved).
  int64  time_unix_nano      = 2;
  string hash                = 3; // hex lowercase
  string proposer_address    = 4; // hex lowercase
  // chain_id comes from the sidecar's network config YAML — it is NOT in the
  // per-block ABCI header (Phase C finding).
  string chain_id            = 5;
  int32  tx_count            = 6;
  int32  event_count         = 7; // block-level + per-tx events
  int32  kv_count            = 8;
  // Total fan-out messages published for this height EXCLUDING this envelope
  // (ADR-025 defense-in-depth metadata; not chain data).
  int32  published_msg_count = 9;
}

// TxWithResult is one transaction on pokt.tx.{H}.{idx} (ADR-022): the raw tx
// bytes and its ABCI execution result, both exactly as captured by FilePlugin.
message TxWithResult {
  bytes tx     = 1; // cosmos.tx.v1beta1.Tx wire bytes (RequestFinalizeBlock.txs[idx])
  bytes result = 2; // cometbft abci.ExecTxResult wire bytes (ResponseFinalizeBlock.tx_results[idx])
}

// EventInBlock is one ABCI event on pokt.events.{type}.{H}, with the position
// metadata the deterministic row PKs need (tables key on tx_index/event_index).
message EventInBlock {
  bytes event       = 1; // cometbft abci.Event wire bytes
  int32 tx_index    = 2; // -1 for block-level (ResponseFinalizeBlock.events)
  int32 event_index = 3; // ordinal within its scope (block-level set or its tx)
}
```

- [ ] **Step 2: Create `buf.gen.envelope.yaml`:**

```yaml
# buf generate template for OUR envelope protos -> internal/proto/gen/.
# go_package is set in the .proto file itself; no managed mode needed. Offline.
version: v2
plugins:
  - local: protoc-gen-gocosmos
    out: internal/proto/gen
    opt:
      - plugins=grpc
      - paths=source_relative
    include_imports: false
    include_wkt: false
```

- [ ] **Step 3: Add the module to `buf.yaml`** — in the `modules:` list append:

```yaml
  - path: internal/proto
```

- [ ] **Step 4: Write the failing test** — `internal/proto/envelope_test.go` (package `proto_test`, imports the gen package which does not exist yet):

```go
package proto_test

import (
	"testing"

	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

func TestBlockEnvelopeRoundtrip(t *testing.T) {
	in := &psv1.BlockEnvelope{
		Height: 135836, TimeUnixNano: 1748469041000000000,
		Hash: "dd01f0", ProposerAddress: "aa11bb", ChainId: "pocket",
		TxCount: 4, EventCount: 87, KvCount: 556, PublishedMsgCount: 647,
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out psv1.BlockEnvelope
	if err := out.Unmarshal(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Height != in.Height || out.ChainId != in.ChainId || out.PublishedMsgCount != in.PublishedMsgCount {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", &out, in)
	}
}

func TestEventInBlockBlockLevelSentinel(t *testing.T) {
	e := &psv1.EventInBlock{Event: []byte{0x0a}, TxIndex: -1, EventIndex: 3}
	raw, _ := e.Marshal()
	var out psv1.EventInBlock
	if err := out.Unmarshal(raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.TxIndex != -1 {
		t.Fatalf("tx_index sentinel lost: %d", out.TxIndex)
	}
}
```

Run: `go test ./internal/proto/` → FAIL (gen package missing).

- [ ] **Step 5: Generate**: `make gen-proto` (now fully wired). Then `go test ./internal/proto/` → PASS. `make gen-check` → `generated code up to date.`

- [ ] **Step 6: Commit**

```bash
git add internal/proto buf.gen.envelope.yaml buf.yaml
git commit -m "feat(proto): pocketscribe.v1 BlockEnvelope/TxWithResult/EventInBlock + offline buf template (ADR-022/025)"
```

---

### Task 4: NATS subject grammars for tx / events / kv

**Files:**
- Modify: `internal/nats/subjects.go`
- Create: `internal/nats/subjects_test.go` (extend if it exists — check first with `ls internal/nats/`)

- [ ] **Step 1: Write the failing tests** (append to or create `internal/nats/subjects_test.go`, package `nats`):

```go
func TestTxSubjectRoundtrip(t *testing.T) {
	s := TxSubject(135836, 3)
	if s != "pokt.tx.135836.3" {
		t.Fatalf("TxSubject = %q", s)
	}
	h, idx, err := HeightFromTxSubject(s)
	if err != nil || h != 135836 || idx != 3 {
		t.Fatalf("HeightFromTxSubject = %d,%d,%v", h, idx, err)
	}
}

func TestEventSubjectSanitizesType(t *testing.T) {
	s := EventSubject("pocket.supplier.EventSupplierStaked", 135836)
	if s != "pokt.events.pocket_supplier_EventSupplierStaked.135836" {
		t.Fatalf("EventSubject = %q", s)
	}
	h, err := HeightFromEventSubject(s)
	if err != nil || h != 135836 {
		t.Fatalf("HeightFromEventSubject = %d,%v", h, err)
	}
	if f := EventSubjectFilter("pocket.supplier.EventSupplierStaked"); f != "pokt.events.pocket_supplier_EventSupplierStaked.*" {
		t.Fatalf("EventSubjectFilter = %q", f)
	}
}

func TestKVSubjectRoundtrip(t *testing.T) {
	s := KVSubject("supplier", 135836)
	if s != "pokt.kv.supplier.135836" {
		t.Fatalf("KVSubject = %q", s)
	}
	h, err := HeightFromKVSubject(s)
	if err != nil || h != 135836 {
		t.Fatalf("HeightFromKVSubject = %d,%v", h, err)
	}
	if f := KVSubjectFilter("supplier"); f != "pokt.kv.supplier.*" {
		t.Fatalf("KVSubjectFilter = %q", f)
	}
}

func TestHeightFromSubjectDispatch(t *testing.T) {
	cases := []struct {
		subject string
		want    int64
		wantErr bool
	}{
		{"pokt.block.42", 42, false},
		{"pokt.tx.42.7", 42, false},
		{"pokt.events.pocket_supplier_EventSupplierStaked.42", 42, false},
		{"pokt.kv.supplier.42", 42, false},
		{"pokt.kv.supplier.notanumber", 0, true},
		{"pokt.unknown.42", 0, true},
		{"pokt.tx.42", 0, true}, // missing idx token
	}
	for _, c := range cases {
		h, err := HeightFromSubject(c.subject)
		if c.wantErr != (err != nil) || (!c.wantErr && h != c.want) {
			t.Errorf("HeightFromSubject(%q) = %d,%v want %d,err=%v", c.subject, h, err, c.want, c.wantErr)
		}
	}
}
```

- [ ] **Step 2: Run — FAIL** (`go test ./internal/nats/`): undefined functions.

- [ ] **Step 3: Append to `internal/nats/subjects.go`:**

```go
// ── tx fan-out (ADR-022: pokt.tx.{H}.{idx}, one tx per message) ─────────────

// TxSubjectFilter matches every per-tx message regardless of height/index.
const TxSubjectFilter = "pokt.tx.>"

const txPrefix = "pokt.tx."

// TxSubject returns the subject for tx index idx of height h.
func TxSubject(h int64, idx int) string {
	return txPrefix + strconv.FormatInt(h, 10) + "." + strconv.Itoa(idx)
}

// HeightFromTxSubject parses pokt.tx.{H}.{idx}.
func HeightFromTxSubject(subject string) (int64, int, error) {
	if !strings.HasPrefix(subject, txPrefix) {
		return 0, 0, fmt.Errorf("not a tx subject: %q", subject)
	}
	rest := strings.Split(subject[len(txPrefix):], ".")
	if len(rest) != 2 {
		return 0, 0, fmt.Errorf("malformed tx subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	idx, err := strconv.Atoi(rest[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse tx index from %q: %w", subject, err)
	}
	return h, idx, nil
}

// ── event fan-out (ADR-022: pokt.events.{eventType}.{H}) ────────────────────

const eventPrefix = "pokt.events."

// EventToken converts an ABCI event type to a single NATS token: "." is the
// NATS separator, so "pocket.supplier.EventSupplierStaked" becomes
// "pocket_supplier_EventSupplierStaked" (ADR-022 amendment).
func EventToken(eventType string) string { return strings.ReplaceAll(eventType, ".", "_") }

// EventSubject returns the subject for one event of eventType at height h.
func EventSubject(eventType string, h int64) string {
	return eventPrefix + EventToken(eventType) + "." + strconv.FormatInt(h, 10)
}

// EventSubjectFilter matches all heights of one event type.
func EventSubjectFilter(eventType string) string { return eventPrefix + EventToken(eventType) + ".*" }

// HeightFromEventSubject parses pokt.events.{token}.{H}.
func HeightFromEventSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, eventPrefix) {
		return 0, fmt.Errorf("not an event subject: %q", subject)
	}
	rest := strings.Split(subject[len(eventPrefix):], ".")
	if len(rest) != 2 {
		return 0, fmt.Errorf("malformed event subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// ── kv fan-out (ADR-022: pokt.kv.{store}.{H}) ───────────────────────────────

const kvPrefix = "pokt.kv."

// KVSubject returns the subject for one StoreKVPair of store at height h.
func KVSubject(store string, h int64) string {
	return kvPrefix + store + "." + strconv.FormatInt(h, 10)
}

// KVSubjectFilter matches all heights of one store.
func KVSubjectFilter(store string) string { return kvPrefix + store + ".*" }

// HeightFromKVSubject parses pokt.kv.{store}.{H}.
func HeightFromKVSubject(subject string) (int64, error) {
	if !strings.HasPrefix(subject, kvPrefix) {
		return 0, fmt.Errorf("not a kv subject: %q", subject)
	}
	rest := strings.Split(subject[len(kvPrefix):], ".")
	if len(rest) != 2 {
		return 0, fmt.Errorf("malformed kv subject: %q", subject)
	}
	h, err := strconv.ParseInt(rest[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height from %q: %w", subject, err)
	}
	return h, nil
}

// HeightFromSubject extracts the height from any PocketScribe subject grammar
// (block / tx / events / kv). Single dispatch point for the consumer runtimes.
func HeightFromSubject(subject string) (int64, error) {
	switch {
	case strings.HasPrefix(subject, blockPrefix):
		return HeightFromBlockSubject(subject)
	case strings.HasPrefix(subject, txPrefix):
		h, _, err := HeightFromTxSubject(subject)
		return h, err
	case strings.HasPrefix(subject, eventPrefix):
		return HeightFromEventSubject(subject)
	case strings.HasPrefix(subject, kvPrefix):
		return HeightFromKVSubject(subject)
	default:
		return 0, fmt.Errorf("unknown subject grammar: %q", subject)
	}
}
```

- [ ] **Step 4: Run — PASS**: `go test ./internal/nats/` → ok. `make ci` clean.

- [ ] **Step 5: Commit**

```bash
git add internal/nats/
git commit -m "feat(nats): tx/events/kv subject grammars + HeightFromSubject dispatch (ADR-022)"
```

---

### Task 5: Decoder packages `v0_1_8` + `v0_1_27`, registry, CI shape-guard

**Files:**
- Create: `internal/decoders/v0_1_8/decoder.go`, `internal/decoders/v0_1_27/decoder.go`
- Modify: `internal/router/registry.go`
- Create: `internal/router/shapeguard_test.go`
- Modify: `internal/router/router_test.go` (boundary cases for the new entries)

- [ ] **Step 1: Write the shape-guard test FIRST** — `internal/router/shapeguard_test.go`. It recomputes the supplier-closure break versions mechanically from `docs/research/.shapes/*.json` and asserts every break version is registered. With the current registry it MUST FAIL (v0_1_8/v0_1_27 missing) — that failure is the forcing function. Algorithm verified in `docs/research/supplier-shape-breaks.md` §5 (a prototype run produced exactly `{v0_1_8, v0_1_27}`):

```go
package router

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Supplier-closure seed types. Append the three pocket.migration claim types
// when that decode scope lands (docs/research/supplier-shape-breaks.md §6).
var shapeGuardSeeds = []string{
	"pocket.supplier.MsgStakeSupplier",
	"pocket.supplier.MsgUnstakeSupplier",
	"pocket.supplier.MsgStakeSupplierResponse",
	"pocket.supplier.MsgUnstakeSupplierResponse",
	"pocket.supplier.EventSupplierStaked",
	"pocket.supplier.EventSupplierUnbondingBegin",
	"pocket.supplier.EventSupplierUnbondingEnd",
	"pocket.supplier.EventSupplierUnbondingCanceled",
	"pocket.supplier.EventSupplierServiceConfigActivated",
	"pocket.tokenomics.EventSupplierSlashed",
	"pocket.shared.Supplier",
}

type shapeField struct {
	Tag      int    `json:"tag"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Repeated bool   `json:"repeated"`
}

type shapeMessage struct {
	Fields []shapeField `json:"fields"`
}

type shapeSnapshot struct {
	Version  string                  `json:"version"`
	Messages map[string]shapeMessage `json:"messages"`
}

// loadSnapshots globs the .shapes dir and returns snapshots sorted numerically
// by patch (v0_1_2 must sort before v0_1_10 — NOT lexicographic).
func loadSnapshots(t *testing.T) []shapeSnapshot {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "research", ".shapes", "v0_1_*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no shape snapshots found: %v", err)
	}
	patch := func(p string) int {
		base := strings.TrimSuffix(filepath.Base(p), ".json") // v0_1_N
		n, err := strconv.Atoi(strings.TrimPrefix(base, "v0_1_"))
		if err != nil {
			t.Fatalf("bad snapshot filename %q: %v", p, err)
		}
		return n
	}
	sort.Slice(paths, func(i, j int) bool { return patch(paths[i]) < patch(paths[j]) })
	snaps := make([]shapeSnapshot, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		var s shapeSnapshot
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		// The FILENAME governs the version string; the snapshot's own version
		// field may use either "v0.1.8" or "v0_1_8" spelling and is ignored.
		s.Version = strings.TrimSuffix(filepath.Base(p), ".json")
		snaps = append(snaps, s)
	}
	return snaps
}

// closure BFS-expands the seed set: a field's type resolves to a message key by
// trying (i) exact, (ii) <package-of-container>.<type>, (iii) "pocket."+type.
// Unresolvable types (scalars, enums, cosmos imports) are skipped.
func closure(s shapeSnapshot) map[string]shapeMessage {
	out := map[string]shapeMessage{}
	queue := append([]string(nil), shapeGuardSeeds...)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if _, done := out[name]; done {
			continue
		}
		msg, ok := s.Messages[name]
		if !ok {
			continue
		}
		out[name] = msg
		pkg := name[:strings.LastIndex(name, ".")]
		for _, f := range msg.Fields {
			for _, cand := range []string{f.Type, pkg + "." + f.Type, "pocket." + f.Type} {
				if _, ok := s.Messages[cand]; ok {
					queue = append(queue, cand)
					break
				}
			}
		}
	}
	return out
}

// canon normalizes a message to its tag-sorted (tag,name,type,repeated) tuples.
func canon(m shapeMessage) string {
	fs := append([]shapeField(nil), m.Fields...)
	sort.Slice(fs, func(i, j int) bool { return fs[i].Tag < fs[j].Tag })
	var b strings.Builder
	for _, f := range fs {
		fmt.Fprintf(&b, "%d|%s|%s|%v;", f.Tag, f.Name, f.Type, f.Repeated)
	}
	return b.String()
}

// TestSupplierShapeGuard: any difference in the canonical form of any
// transitively-reachable closure message between consecutive snapshots marks
// the LATER version as a break version; every break version (plus the oldest
// snapshot) must be in DefaultRegistry(). Deliberately stricter than
// wire-breaking: an additive field is silently DROPPED under earlier-decoder
// fallback — data loss for an indexer. Known blind spots (documented, not
// asserted): enum values and `reserved` ranges are absent from .shapes.
func TestSupplierShapeGuard(t *testing.T) {
	snaps := loadSnapshots(t)
	reg := DefaultRegistry()
	required := map[string][]string{snaps[0].Version: {"(oldest snapshot baseline)"}}
	prev := closure(snaps[0])
	prevV := snaps[0].Version
	for _, s := range snaps[1:] {
		cur := closure(s)
		var changed []string
		seen := map[string]bool{}
		for name := range prev {
			seen[name] = true
			a, inA := prev[name]
			b, inB := cur[name]
			if inA != inB || canon(a) != canon(b) {
				changed = append(changed, name)
			}
		}
		for name := range cur {
			if !seen[name] {
				changed = append(changed, name) // type entered the closure
			}
		}
		if len(changed) > 0 {
			sort.Strings(changed)
			required[s.Version] = changed
		}
		prev, prevV = cur, s.Version
	}
	_ = prevV
	var missing []string
	for v, types := range required {
		if _, ok := reg[v]; !ok {
			missing = append(missing, fmt.Sprintf("%s (changed: %s)", v, strings.Join(types, ", ")))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("supplier shape-break versions missing from router.DefaultRegistry() — "+
			"register a decoder package for each (ADR-008) or supplier rows will be "+
			"silently mis-decoded under lenient fallback:\n  %s", strings.Join(missing, "\n  "))
	}
}
```

- [ ] **Step 2: Run — must FAIL** naming v0_1_8 and v0_1_27: `go test ./internal/router/ -run TestSupplierShapeGuard -v`.

- [ ] **Step 3: Create the two adapter packages.** `internal/decoders/v0_1_8/decoder.go`:

```go
// Package v0_1_8 is the decoder for poktroll v0.1.8 — the start of the
// [v0_1_8..v0_1_26] supplier shape range (pocket.shared.ServiceConfigUpdate
// tag reuse; the chain stores Supplier DEHYDRATED from this version on — see
// docs/research/supplier-shape-breaks.md). The buf-generated bindings live in
// gen/ (read-only; regenerate via `make gen-proto`). Registered so the lenient
// router never falls back across the v0.1.8 shape boundary.
package v0_1_8

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.8.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_8" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
```

`internal/decoders/v0_1_27/decoder.go` — same file with `v0_1_8`→`v0_1_27`, `v0.1.8`→`v0.1.27`, and the package comment's range note replaced by: `the start of the [v0_1_27..v0_1_33] supplier shape range (EventSupplierStaked / EventSupplierServiceConfigActivated / EventSupplierSlashed restructured: supplier embed removed, operator_address added).`

- [ ] **Step 4: Register both** in `internal/router/registry.go` — add imports `v0_1_8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8"` and `v0_1_27 ".../v0_1_27"` and map entries `"v0_1_8": v0_1_8.Decoder{},` + `"v0_1_27": v0_1_27.Decoder{},` (keep numeric order in the map literal: v0_1_0, v0_1_8, v0_1_10, v0_1_20, v0_1_27, v0_1_28, v0_1_29, v0_1_30).

- [ ] **Step 5: Add router boundary cases** — append to `internal/router/router_test.go`:

```go
// TestDecoderForShapeBreakEras pins the Phase E shape-complete registry: the
// v0.1.8/v0.1.9 and v0.1.27 eras must resolve to their OWN range decoders, not
// fall back across a supplier shape boundary (mainnet applied heights from
// docs/research/poktroll-versions.md).
func TestDecoderForShapeBreakEras(t *testing.T) {
	ups := []Upgrade{
		{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
		{Name: "v0.1.9", AppliedAtHeight: 78677, DecoderVersion: "v0_1_9"}, // unregistered → falls back to v0_1_8 (same range)
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
	}
	r, err := NewStaticRouter(ups, DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}
	cases := []struct {
		height int64
		want   string
	}{
		{78670, "v0_1_0"},   // pre-v0.1.8 era
		{78671, "v0_1_8"},   // v0.1.8 boundary
		{78680, "v0_1_8"},   // v0.1.9 era → nearest registered earlier = v0_1_8 (shape-correct)
		{247893, "v0_1_27"}, // v0.1.27 boundary — previously fell back to v0_1_20 (WRONG events)
		{287931, "v0_1_27"},
		{287932, "v0_1_28"},
	}
	for _, c := range cases {
		d, err := r.DecoderFor(c.height)
		if err != nil {
			t.Fatalf("DecoderFor(%d): %v", c.height, err)
		}
		if d.Version() != c.want {
			t.Errorf("DecoderFor(%d) = %s, want %s", c.height, d.Version(), c.want)
		}
	}
}
```

- [ ] **Step 6: Run — all PASS**: `go test ./internal/router/ ./internal/decoders/... -v` (shape-guard now green). `make ci` clean.

- [ ] **Step 7: Commit**

```bash
git add internal/decoders/v0_1_8/decoder.go internal/decoders/v0_1_27/decoder.go internal/router/
git commit -m "feat(router): shape-complete registry (+v0_1_8 +v0_1_27) + mechanical CI shape-guard"
```

---

### Task 6: Canonical supplier types, Decoder interface growth, range implementations

**Files:**
- Create: `internal/types/event.go`, `internal/types/supplier.go`
- Modify: `internal/decoders/decoder.go` (+3 methods)
- Create: `internal/decoders/meta.go` (SplitMeta), `internal/decoders/supplierevents.go`, `internal/decoders/supplierkv.go`, `internal/decoders/jsonpb.go`, `internal/decoders/enums.go`, `internal/decoders/enums_test.go`, `internal/decoders/supplierkv_test.go`, `internal/decoders/supplierevents_test.go`
- Create: `internal/decoders/v0_1_8/supplier.go`, `internal/decoders/v0_1_8/supplier_test.go`, `internal/decoders/v0_1_27/supplier.go`, `internal/decoders/v0_1_0/supplier.go`
- Create (delegates): `internal/decoders/{v0_1_10,v0_1_20,v0_1_28,v0_1_29,v0_1_30}/supplier.go`

**PREREQUISITE: Task 2 complete** — the gen trees under `internal/decoders/{v0_1_0,v0_1_8,v0_1_27}/gen/` must exist or nothing in this task compiles.

**Design (locked):** Decoder methods return `(nil, nil)` for "not mine / skip" and an error ONLY for a real decode failure (consumer Naks the height — spec §10). Decoders fill CHAIN CONTENT; the consumer handler stamps position (`Height/Time/TxIndex/EventIndex`) before insert. Event JSONB columns store the RAW attribute JSON exactly as the chain emitted it (fidelity); jsonpb-decode into the gen type serves as schema VALIDATION + scalar extraction. Range owners: `v0_1_0` [0..7], `v0_1_8` [8..26], `v0_1_27` [27..33]; in-range versions delegate (user-ratified: "si de la .8 a la .27 no hubo cambios se use el decoder de la .8"). BANNED in all decoder paths (registry-dependent, import-order-sensitive after stripping): `proto.MessageType`, `proto.MessageName`, codec `InterfaceRegistry`/`UnpackAny`, jsonpb on enum fields without the central enum registration.

- [ ] **Step 1: `internal/types/event.go`:**

```go
package types

// EventAttr is one ABCI event attribute exactly as captured on chain. For
// typed (proto-named) events the SDK emits key = proto field name and value =
// RAW JSON (int64s quoted, enums by NAME, messages as JSON objects); legacy
// events (transfer, coin_spent, …) use plain strings.
type EventAttr struct {
	Key   string
	Value string
}
```

- [ ] **Step 2: `internal/types/supplier.go`:**

```go
package types

import "time"

// Position is stamped by the consumer handler (from the BlockEnvelope and the
// fan-out message metadata), NOT by decoders — decoders see only chain bytes.
// Height/Time are the invariant-#1 axis; TxIndex/EventIndex complete the
// deterministic PK (block_time, block_height, tx_index, event_index).
type Position struct {
	Height     int64
	Time       time.Time
	TxIndex    int32 // -1 → stored as 0 with EventIndex disambiguating? NO: block-level events keep -1 sentinel only on the bus; rows store the table default 0 for block-level. See handler.
	EventIndex int32 // msg tables: the msg index within its tx
}

// MsgStakeSupplier → msg_stake_supplier (hypertable). Field-stable across
// v0_1_0..v0_1_33 (docs/research/supplier-shape-breaks.md §3a).
type MsgStakeSupplier struct {
	Position
	Signer          string
	OwnerAddress    string
	OperatorAddress string
	StakeAmount     int64
	StakeDenom      string
	ServicesJSON    []byte // JSON array of SupplierServiceConfig (jsonpb, OrigName)
}

// MsgUnstakeSupplier → msg_unstake_supplier (hypertable).
type MsgUnstakeSupplier struct {
	Position
	Signer          string
	OperatorAddress string
}

// SupplierMsg is a tagged union: exactly one field is non-nil.
type SupplierMsg struct {
	Stake   *MsgStakeSupplier
	Unstake *MsgUnstakeSupplier
}

// EventSupplierStaked → event_supplier_staked. SupplierJSON is the raw
// "supplier" attribute (pre-v0.1.27 eras only); OperatorAddress is set from
// v0.1.27 on (column added by migration 0032). Exactly one of the two is set.
type EventSupplierStaked struct {
	Position
	SupplierJSON     []byte
	SessionEndHeight int64
	OperatorAddress  string
}

// EventSupplierUnbondingBegin → event_supplier_unbonding_begin. The unbonding
// events retain the supplier embed across ALL versions (no v0.1.27 break).
type EventSupplierUnbondingBegin struct {
	Position
	SupplierJSON       []byte
	ReasonJSON         []byte // raw "reason" attribute (enum name as JSON string)
	SessionEndHeight   int64
	UnbondingEndHeight int64
}

// EventSupplierUnbondingEnd → event_supplier_unbonding_end.
type EventSupplierUnbondingEnd struct {
	Position
	SupplierJSON       []byte
	ReasonJSON         []byte
	SessionEndHeight   int64
	UnbondingEndHeight int64
}

// EventSupplierUnbondingCanceled → event_supplier_unbonding_canceled.
type EventSupplierUnbondingCanceled struct {
	Position
	SupplierJSON     []byte
	AtHeight         int64 // event field "height"
	SessionEndHeight int64
}

// EventSupplierServiceConfigActivated → event_supplier_service_config_activated.
// Pre-v0.1.27: SupplierJSON + ActivationHeight. v0.1.27+: OperatorAddress +
// ServiceID + ActivationHeight (0032 columns).
type EventSupplierServiceConfigActivated struct {
	Position
	SupplierJSON     []byte
	ActivationHeight int64
	OperatorAddress  string
	ServiceID        string
}

// SupplierEvent is a tagged union: exactly one field is non-nil.
type SupplierEvent struct {
	Staked                 *EventSupplierStaked
	UnbondingBegin         *EventSupplierUnbondingBegin
	UnbondingEnd           *EventSupplierUnbondingEnd
	UnbondingCanceled      *EventSupplierUnbondingCanceled
	ServiceConfigActivated *EventSupplierServiceConfigActivated
}

// SupplierSnapshot → supplier_history (append-only, PK (operator_address,
// block_height)). From v0.1.8 the chain stores Supplier DEHYDRATED (no
// services / service_config_history) — those fields stay nil and the hydrated
// truth lives in ServiceConfigUpdateSnapshot rows (decision 5).
type SupplierSnapshot struct {
	Position
	OperatorAddress          string
	OwnerAddress             string
	StakeAmount              int64
	StakeDenom               string
	ServicesJSON             []byte
	UnstakeSessionEndHeight  int64
	ServiceConfigHistoryJSON []byte
}

// ServiceConfigUpdateSnapshot → supplier_service_config_update_history
// (append-only). One row per chain KV write of a ServiceConfigUpdate primary
// (`ServiceConfigUpdate/service_id/...` keys; index layouts are skipped).
type ServiceConfigUpdateSnapshot struct {
	Position
	OperatorAddress    string
	ServiceID          string
	ActivationHeight   int64
	DeactivationHeight int64
	ServiceConfigJSON  []byte
	Deleted            bool
}

// SupplierKVRecord is a tagged union: exactly one field is non-nil.
type SupplierKVRecord struct {
	Supplier            *SupplierSnapshot
	ServiceConfigUpdate *ServiceConfigUpdateSnapshot
}
```

NOTE the `Position.TxIndex` comment above is resolved as: block-level events are stamped `TxIndex=0` in rows (table default; the bus keeps -1 only to distinguish scopes for EventIndex assignment). State this in the handler (Task 10).

- [ ] **Step 3: Grow the Decoder interface** — `internal/decoders/decoder.go`, replace the interface body with:

```go
type Decoder interface {
	// Version returns the canonical decoder version tag, e.g. "v0_1_30".
	Version() string
	// DecodeBlockHeader parses a FilePlugin `block-{H}-meta` payload into the
	// canonical BlockHeader. The header is version-invariant, so every version
	// delegates to the shared DecodeBlockHeader function in this package.
	DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)
	// DecodeSupplierMsg decodes one tx-body message given its Any type_url and
	// value bytes. Returns (nil, nil) when typeURL is not a supplier-module
	// message this indexer persists. An error means a real decode failure —
	// the consumer must NOT ack (spec §10).
	DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error)
	// DecodeSupplierEvent decodes one typed supplier event from its ABCI
	// attributes. Returns (nil, nil) for event types not persisted in Phase E.
	DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error)
	// DecodeSupplierKV decodes one StoreKVPair captured from the "supplier"
	// store. Returns (nil, nil) for index/params/non-persisted records
	// (key discrimination table: docs/research/phase-e-spike-findings.md §4d).
	DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error)
}
```

(Compile now breaks all 8 adapter packages — the rest of this task fixes them; that is the intended additive-growth flow documented in the interface comment.)

- [ ] **Step 4: Shared helpers.** `internal/decoders/jsonpb.go`:

```go
package decoders

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cosmos/gogoproto/jsonpb"
	"github.com/cosmos/gogoproto/proto"
)

// marshalJSONPB renders a gogo message as JSON with proto field names
// (OrigName) — the same convention the SDK uses when emitting typed-event
// attributes, so stored JSONB is consistent across sources.
func marshalJSONPB(m proto.Message) ([]byte, error) {
	var b bytes.Buffer
	mr := jsonpb.Marshaler{OrigName: true, EmitDefaults: false}
	if err := mr.Marshal(&b, m); err != nil {
		return nil, fmt.Errorf("jsonpb marshal %T: %w", m, err)
	}
	return b.Bytes(), nil
}

// MarshalJSONPBSlice renders a slice of gogo messages as a JSON array.
// Returns nil for an empty slice (dehydrated-era Supplier has no services).
func MarshalJSONPBSlice[T proto.Message](items []T) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	parts := make([]string, 0, len(items))
	for _, it := range items {
		j, err := marshalJSONPB(it)
		if err != nil {
			return nil, err
		}
		parts = append(parts, string(j))
	}
	return []byte("[" + strings.Join(parts, ",") + "]"), nil
}

// UnmarshalEventJSON validates+decodes a rebuilt typed-event JSON document into
// the version's generated event type. AllowUnknownFields tolerates attributes
// added by later chain versions (they are preserved in the raw JSONB anyway).
// REQUIRES the central enum registration (enums.go): jsonpb resolves enum
// NAMES via proto.EnumValueMap, which stripregister removed from gen init().
func UnmarshalEventJSON(doc []byte, m proto.Message) error {
	um := jsonpb.Unmarshaler{AllowUnknownFields: true}
	if err := um.Unmarshal(bytes.NewReader(doc), m); err != nil {
		return fmt.Errorf("jsonpb unmarshal %T: %w", m, err)
	}
	return nil
}
```

`internal/decoders/supplierevents.go`:

```go
package decoders

import (
	"strings"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// sdkBookkeepingAttrs are SDK-injected attributes that are NOT proto fields of
// typed events (spike finding §4c): "mode" on block-level events
// (BeginBlock/EndBlock) and "msg_index" on tx events.
func isBookkeepingAttr(key string) bool { return key == "mode" || key == "msg_index" }

// EventAttrsJSON rebuilds the typed-event JSON document from its ABCI
// attributes: {"<field>":<raw json>,...}. Attribute values of typed events ARE
// raw JSON (quoted int64s, enum names, embedded objects) — they are spliced in
// verbatim, never re-encoded (fidelity).
func EventAttrsJSON(attrs []types.EventAttr) []byte {
	parts := make([]string, 0, len(attrs))
	for _, a := range attrs {
		if isBookkeepingAttr(a.Key) {
			continue
		}
		parts = append(parts, `"`+a.Key+`":`+a.Value)
	}
	return []byte("{" + strings.Join(parts, ",") + "}")
}

// EventAttrRaw returns the raw JSON value of one attribute ("" if absent).
func EventAttrRaw(attrs []types.EventAttr, key string) []byte {
	for _, a := range attrs {
		if a.Key == key {
			return []byte(a.Value)
		}
	}
	return nil
}
```

`internal/decoders/supplierkv.go` — version-invariant key discrimination (keys.go identical at poktroll v0.1.20 and v0.1.29; spike §4d):

```go
package decoders

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SupplierKeyKind classifies a supplier-store KV key (spike §4d table).
type SupplierKeyKind int

const (
	// SupplierKeyIgnore: index layouts whose values are primary-key pointer
	// bytes, params ("p_supplier"), unbonding-height markers — NOT protos.
	SupplierKeyIgnore SupplierKeyKind = iota
	// SupplierKeyRecord: Supplier/operator_address/<addr>/ → pocket.shared.Supplier value.
	SupplierKeyRecord
	// SupplierKeySCURecord: ServiceConfigUpdate/service_id/<svc>/<actH:BE8>/<addr>/ →
	// full pocket.shared.ServiceConfigUpdate value (the PRIMARY layout).
	SupplierKeySCURecord
)

const (
	supplierRecordPrefix = "Supplier/operator_address/"
	scuPrimaryPrefix     = "ServiceConfigUpdate/service_id/"
)

// ClassifySupplierKey discriminates supplier-store keys. Only two layouts
// carry proto values; everything else is skipped (invariant 3: decoding an
// index pointer as a Supplier would write garbage snapshots).
func ClassifySupplierKey(key []byte) SupplierKeyKind {
	switch {
	case bytes.HasPrefix(key, []byte(supplierRecordPrefix)):
		return SupplierKeyRecord
	case bytes.HasPrefix(key, []byte(scuPrimaryPrefix)):
		return SupplierKeySCURecord
	default:
		return SupplierKeyIgnore
	}
}

// ParseSCUPrimaryKey extracts (serviceID, activationHeight, operatorAddress)
// from ServiceConfigUpdate/service_id/<svc>/<actH:8-byte big-endian>/<addr>/
// (all segments end with '/'; heights are 8-byte big-endian — spike §4d).
// Needed when a deleted record has an empty value.
func ParseSCUPrimaryKey(key []byte) (serviceID string, activationHeight int64, operator string, err error) {
	rest := bytes.TrimPrefix(key, []byte(scuPrimaryPrefix))
	i := bytes.IndexByte(rest, '/')
	if i < 0 {
		return "", 0, "", fmt.Errorf("malformed SCU key (no service segment): %q", key)
	}
	serviceID = string(rest[:i])
	rest = rest[i+1:]
	if len(rest) < 9 || rest[8] != '/' {
		return "", 0, "", fmt.Errorf("malformed SCU key (no height segment): %q", key)
	}
	activationHeight = int64(binary.BigEndian.Uint64(rest[:8]))
	rest = rest[9:]
	if j := bytes.IndexByte(rest, '/'); j > 0 {
		operator = string(rest[:j])
	} else {
		operator = string(bytes.TrimSuffix(rest, []byte("/")))
	}
	if operator == "" {
		return "", 0, "", fmt.Errorf("malformed SCU key (no operator segment): %q", key)
	}
	return serviceID, activationHeight, operator, nil
}
```

`internal/decoders/meta.go`:

```go
package decoders

import "fmt"

// metaRecordCount is the ADR-027 contract: block-{H}-meta is EXACTLY three
// uvarint-delimited gogo records — RequestFinalizeBlock, ResponseFinalizeBlock,
// ResponseCommit (empirically 0 bytes).
const metaRecordCount = 3

// SplitMeta splits a block-{H}-meta payload into its three records using the
// same uvarint framing as DecodeBlockHeader. The sidecar fan-out uses records
// [0] and [1]; record [2] is validated but unused.
func SplitMeta(metaBytes []byte) ([][]byte, error) {
	records := make([][]byte, 0, metaRecordCount)
	rest := metaBytes
	for len(rest) > 0 {
		payload, consumed, err := readDelimited(rest)
		if err != nil {
			return nil, fmt.Errorf("meta record %d: %w", len(records), err)
		}
		records = append(records, payload)
		rest = rest[consumed:]
	}
	if len(records) != metaRecordCount {
		return nil, fmt.Errorf("meta has %d records, want %d (ADR-027)", len(records), metaRecordCount)
	}
	return records, nil
}
```

CHECK FIRST: `internal/decoders/blockheader.go`'s `readDelimited` signature — if it returns `(payload []byte, consumed int)` without error (panics/errors differently), ADAPT SplitMeta to call it exactly as DecodeBlockHeader does; do NOT duplicate framing logic (DRY).

`internal/decoders/enums.go` — central, superset (newest range) registration; guarded so it is idempotent and panic-free regardless of import order:

```go
package decoders

import (
	"github.com/cosmos/gogoproto/proto"

	sh27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	sup27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// init restores ONLY the enum name↔value maps that jsonpb needs
// (proto.EnumValueMap consults the global registry; stripregister removed the
// gen init() registrations). Registered from the NEWEST range tree (v0_1_27)
// because enum value sets only ever GROW (verified across v0_1_0..v0_1_33:
// RPCType +COMET_BFT@v0_1_27, SupplierUnbondingReason +MIGRATION@v0_1_13) — an
// older tree's map would reject newer names. enums_test.go enforces the
// superset property; add-decoder-version step: re-point these imports when a
// future version adds enum values. Init order is safe: Go guarantees the
// imported sh27/sup27 packages' var initialization runs before this init(),
// so the _name/_value maps are always populated; the nil guard only protects
// against a test binary that also loads an UNstripped tree.
func init() {
	reg := func(name string, nm map[int32]string, vm map[string]int32) {
		if proto.EnumValueMap(name) == nil {
			proto.RegisterEnum(name, nm, vm)
		}
	}
	reg("pocket.shared.RPCType", sh27.RPCType_name, sh27.RPCType_value)
	reg("pocket.shared.ConfigOptions", sh27.ConfigOptions_name, sh27.ConfigOptions_value)
	reg("pocket.supplier.SupplierUnbondingReason", sup27.SupplierUnbondingReason_name, sup27.SupplierUnbondingReason_value)
}
```

`internal/decoders/enums_test.go`:

```go
package decoders

import (
	"testing"

	"github.com/cosmos/gogoproto/proto"

	sh0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	sh8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	sup0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/supplier"
	sup8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// TestRegisteredEnumsAreSupersets: the centrally-registered (newest-range) enum
// maps must contain every name of every older range's maps, or jsonpb decode of
// older-era events would fail on a valid name.
func TestRegisteredEnumsAreSupersets(t *testing.T) {
	cases := []struct {
		name string
		olds []map[string]int32
	}{
		{"pocket.shared.RPCType", []map[string]int32{sh0.RPCType_value, sh8.RPCType_value}},
		{"pocket.shared.ConfigOptions", []map[string]int32{sh0.ConfigOptions_value, sh8.ConfigOptions_value}},
		{"pocket.supplier.SupplierUnbondingReason", []map[string]int32{sup0.SupplierUnbondingReason_value, sup8.SupplierUnbondingReason_value}},
	}
	for _, c := range cases {
		reg := proto.EnumValueMap(c.name)
		if reg == nil {
			t.Fatalf("enum %s not registered (enums.go init missing?)", c.name)
		}
		for i, old := range c.olds {
			for name, val := range old {
				got, ok := reg[name]
				if !ok || got != val {
					t.Errorf("enum %s: older-range[%d] name %q=%d not in registered map (got %d, ok=%v)", c.name, i, name, val, got, ok)
				}
			}
		}
	}
}
```

NOTE: `sup0`/`sup8` SupplierUnbondingReason — VERIFY the enum exists in v0_1_0's supplier package (`grep -r "SupplierUnbondingReason_value" internal/decoders/v0_1_0/gen/`); if a map is absent in an older tree, drop that `olds` entry with a comment (absence means no older names to preserve).

- [ ] **Step 5: Write the unit tests for the helpers FIRST** (they fail until Step 4's files compile — write test files, run, see compile failures, then fill). `internal/decoders/supplierevents_test.go`:

```go
package decoders

import (
	"bytes"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"
)

func TestEventAttrsJSONSkipsBookkeepingAndSplicesRaw(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		{Key: "mode", Value: "EndBlock"},
		{Key: "supplier", Value: `{"operator_address":"pokt16ar"}`},
		{Key: "msg_index", Value: "0"},
	}
	got := EventAttrsJSON(attrs)
	want := `{"session_end_height":"135840","supplier":{"operator_address":"pokt16ar"}}`
	if string(got) != want {
		t.Fatalf("EventAttrsJSON = %s, want %s", got, want)
	}
	if raw := EventAttrRaw(attrs, "supplier"); !bytes.Equal(raw, []byte(`{"operator_address":"pokt16ar"}`)) {
		t.Fatalf("EventAttrRaw = %s", raw)
	}
	if raw := EventAttrRaw(attrs, "absent"); raw != nil {
		t.Fatalf("EventAttrRaw(absent) = %s, want nil", raw)
	}
}
```

`internal/decoders/supplierkv_test.go`:

```go
package decoders

import (
	"encoding/binary"
	"testing"
)

func TestClassifySupplierKey(t *testing.T) {
	cases := []struct {
		key  string
		want SupplierKeyKind
	}{
		{"Supplier/operator_address/pokt16ar6g3w/", SupplierKeyRecord},
		{"Supplier/unbonding_height/pokt16ar6g3w/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/service_id/eth/XXXXXXXX/pokt16ar/", SupplierKeySCURecord},
		{"ServiceConfigUpdate/operator_address/pokt16ar/eth/XXXXXXXX/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/activation_height/XXXXXXXX/eth/pokt16ar/", SupplierKeyIgnore},
		{"ServiceConfigUpdate/deactivation_height/XXXXXXXX/eth/pokt16ar/XXXXXXXX/", SupplierKeyIgnore},
		{"p_supplier", SupplierKeyIgnore},
		{"garbage", SupplierKeyIgnore},
	}
	for _, c := range cases {
		if got := ClassifySupplierKey([]byte(c.key)); got != c.want {
			t.Errorf("ClassifySupplierKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestParseSCUPrimaryKey(t *testing.T) {
	var hbuf [8]byte
	binary.BigEndian.PutUint64(hbuf[:], 96801)
	key := append([]byte("ServiceConfigUpdate/service_id/arb_one/"), hbuf[:]...)
	key = append(key, []byte("/pokt12qse7etheight/")...)
	svc, act, op, err := ParseSCUPrimaryKey(key)
	if err != nil || svc != "arb_one" || act != 96801 || op != "pokt12qse7etheight" {
		t.Fatalf("ParseSCUPrimaryKey = %q,%d,%q,%v", svc, act, op, err)
	}
	if _, _, _, err := ParseSCUPrimaryKey([]byte("ServiceConfigUpdate/service_id/x")); err == nil {
		t.Fatal("want error on malformed key")
	}
}
```

Run `go test ./internal/decoders/` → compile failures → fill Step 4 files → tests PASS (the interface growth still breaks the version packages; next steps fix them before `make ci`).

- [ ] **Step 6: Range implementation `internal/decoders/v0_1_8/supplier.go`** (the [8..26] owner; v0_1_27 and v0_1_0 mirror it with their own gen imports and era differences called out below):

```go
package v0_1_8

import (
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// Supplier-module decode for the [v0_1_8..v0_1_26] shape range
// (docs/research/supplier-shape-breaks.md §3). In-range versions
// (v0_1_10, v0_1_20) delegate here.

// DecodeSupplierMsg implements decoders.Decoder. Only Code==0 txs reach this
// point (handler filters); (nil, nil) = not a supplier msg we persist.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	switch typeURL {
	case "/pocket.supplier.MsgStakeSupplier":
		var m supplier.MsgStakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 MsgStakeSupplier: %w", err)
		}
		servicesJSON, err := decoders.MarshalJSONPBSlice(m.Services)
		if err != nil {
			return nil, err
		}
		out := &types.MsgStakeSupplier{
			Signer:          m.Signer,
			OwnerAddress:    m.OwnerAddress,
			OperatorAddress: m.OperatorAddress,
			ServicesJSON:    servicesJSON,
		}
		if m.Stake != nil {
			if !m.Stake.Amount.IsInt64() {
				return nil, fmt.Errorf("v0_1_8 MsgStakeSupplier stake overflows int64: %s", m.Stake.Amount)
			}
			out.StakeAmount = m.Stake.Amount.Int64()
			out.StakeDenom = m.Stake.Denom
		}
		return &types.SupplierMsg{Stake: out}, nil
	case "/pocket.supplier.MsgUnstakeSupplier":
		var m supplier.MsgUnstakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 MsgUnstakeSupplier: %w", err)
		}
		return &types.SupplierMsg{Unstake: &types.MsgUnstakeSupplier{
			Signer:          m.Signer,
			OperatorAddress: m.OperatorAddress,
		}}, nil
	default:
		return nil, nil
	}
}

// DecodeSupplierEvent implements decoders.Decoder. The jsonpb decode VALIDATES
// the payload against this range's schema and extracts scalars; JSONB columns
// store the raw attribute JSON verbatim (fidelity).
func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	doc := decoders.EventAttrsJSON(attrs)
	switch eventType {
	case "pocket.supplier.EventSupplierStaked":
		var ev supplier.EventSupplierStaked
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{Staked: &types.EventSupplierStaked{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingBegin":
		var ev supplier.EventSupplierUnbondingBegin
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingBegin: &types.EventSupplierUnbondingBegin{
			SupplierJSON:       decoders.EventAttrRaw(attrs, "supplier"),
			ReasonJSON:         decoders.EventAttrRaw(attrs, "reason"),
			SessionEndHeight:   ev.SessionEndHeight,
			UnbondingEndHeight: ev.UnbondingEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingEnd":
		var ev supplier.EventSupplierUnbondingEnd
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingEnd: &types.EventSupplierUnbondingEnd{
			SupplierJSON:       decoders.EventAttrRaw(attrs, "supplier"),
			ReasonJSON:         decoders.EventAttrRaw(attrs, "reason"),
			SessionEndHeight:   ev.SessionEndHeight,
			UnbondingEndHeight: ev.UnbondingEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingCanceled":
		var ev supplier.EventSupplierUnbondingCanceled
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingCanceled: &types.EventSupplierUnbondingCanceled{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			AtHeight:         ev.Height,
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierServiceConfigActivated":
		var ev supplier.EventSupplierServiceConfigActivated
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{ServiceConfigActivated: &types.EventSupplierServiceConfigActivated{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			ActivationHeight: ev.ActivationHeight,
		}}, nil
	default:
		return nil, nil
	}
}

// DecodeSupplierKV implements decoders.Decoder. Only the two proto-carrying
// key layouts are decoded; index pointers and params are skipped (nil, nil).
func (Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	switch decoders.ClassifySupplierKey(key) {
	case decoders.SupplierKeyRecord:
		if deleted {
			// Supplier record deletion (unbond completion). Phase E decision 6:
			// skip — captured via EventSupplierUnbondingEnd; revisit in Phase F.
			return nil, nil
		}
		var s shared.Supplier
		if err := s.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 Supplier KV: %w", err)
		}
		servicesJSON, err := decoders.MarshalJSONPBSlice(s.Services)
		if err != nil {
			return nil, err
		}
		schJSON, err := decoders.MarshalJSONPBSlice(s.ServiceConfigHistory)
		if err != nil {
			return nil, err
		}
		out := &types.SupplierSnapshot{
			OperatorAddress:          s.OperatorAddress,
			OwnerAddress:             s.OwnerAddress,
			ServicesJSON:             servicesJSON,
			UnstakeSessionEndHeight:  int64(s.UnstakeSessionEndHeight),
			ServiceConfigHistoryJSON: schJSON,
		}
		if s.Stake != nil {
			if !s.Stake.Amount.IsInt64() {
				return nil, fmt.Errorf("v0_1_8 Supplier stake overflows int64: %s", s.Stake.Amount)
			}
			out.StakeAmount = s.Stake.Amount.Int64()
			out.StakeDenom = s.Stake.Denom
		}
		return &types.SupplierKVRecord{Supplier: out}, nil
	case decoders.SupplierKeySCURecord:
		if deleted {
			svc, act, op, err := decoders.ParseSCUPrimaryKey(key)
			if err != nil {
				return nil, fmt.Errorf("v0_1_8 deleted SCU key: %w", err)
			}
			return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
				OperatorAddress: op, ServiceID: svc, ActivationHeight: act, Deleted: true,
			}}, nil
		}
		var scu shared.ServiceConfigUpdate
		if err := scu.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 ServiceConfigUpdate KV: %w", err)
		}
		var svcJSON []byte
		if scu.Service != nil {
			j, err := decoders.MarshalJSONPBSlice([]*shared.SupplierServiceConfig{scu.Service})
			if err != nil {
				return nil, err
			}
			// single-element array → unwrap to the object
			svcJSON = j[1 : len(j)-1]
		}
		return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
			OperatorAddress:    scu.OperatorAddress,
			ServiceID:          scu.Service.GetServiceId(),
			ActivationHeight:   scu.ActivationHeight,
			DeactivationHeight: scu.DeactivationHeight,
			ServiceConfigJSON:  svcJSON,
		}}, nil
	default:
		return nil, nil
	}
}
```

FIELD-NAME CHECK (required, the gen field spellings must be verified, not assumed): `grep -n "UnstakeSessionEndHeight\|ServiceConfigHistory\|ActivationHeight\|DeactivationHeight" internal/decoders/v0_1_8/gen/pocket/shared/supplier.pb.go internal/decoders/v0_1_8/gen/pocket/shared/service.pb.go | head` and the event field names in `gen/pocket/supplier/event.pb.go`. Adjust spellings to what gen actually emits (e.g. `UnstakeSessionEndHeight` may be uint64 vs int64 — cast as shown).

- [ ] **Step 7: `internal/decoders/v0_1_27/supplier.go`** — same skeleton with imports from `v0_1_27/gen`, plus these era differences:
  - `EventSupplierStaked`: gen type has `OperatorAddress` (no supplier embed) → fill `OperatorAddress: ev.OperatorAddress`, leave `SupplierJSON: nil`.
  - `EventSupplierServiceConfigActivated`: fill `OperatorAddress: ev.OperatorAddress, ServiceID: ev.ServiceId`, leave `SupplierJSON: nil`.
  - Unbonding events: identical to v0_1_8 (supplier embed retained — break map §1).
  - Msg + KV decode: identical code against v0_1_27 gen (shapes unchanged since v0_1_8 for Supplier/SCU).

- [ ] **Step 8: `internal/decoders/v0_1_0/supplier.go`** — same skeleton with imports from `v0_1_0/gen`, with these era notes in the package comment: mainnet has ZERO supplier activity in the v0.1.0..v0.1.7 eras (verified on-chain; docs/research/supplier-shape-breaks.md) — this implementation exists for registry completeness and decodes the v0_1_0-era shapes (hydrated Supplier; pre-refactor ServiceConfigUpdate). Era difference: v0_1_0's `ServiceConfigUpdate` gen type has NO `OperatorAddress`/`Service` fields (tags 1/2 are `repeated SupplierServiceConfig` + `uint64`) → for `SupplierKeySCURecord` parse op/svc/act from the KEY via `ParseSCUPrimaryKey` and store `ServiceConfigJSON` = `marshalJSONPB` of the whole decoded message; `DeactivationHeight: 0`. Add this comment to the SCU case: `// ParseSCUPrimaryKey assumes the key layout introduced at v0_1_8; the v0_1_0 keeper may have used another layout, but this era has zero supplier KV activity on mainnet (decision 4) — if this path ever fires, the parse error surfaces as a Nak (loud), never as garbage rows.`

- [ ] **Step 9: Delegates.** For each of `v0_1_10`, `v0_1_20` create `supplier.go`:

```go
package v0_1_10

import (
	v0_1_8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Supplier decode delegates to the v0_1_8 range owner: the supplier closure is
// shape-identical across [v0_1_8..v0_1_26] (docs/research/supplier-shape-breaks.md).

func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	return v0_1_8.Decoder{}.DecodeSupplierMsg(typeURL, value)
}

func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	return v0_1_8.Decoder{}.DecodeSupplierEvent(eventType, attrs)
}

func (Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	return v0_1_8.Decoder{}.DecodeSupplierKV(key, value, deleted)
}
```

For `v0_1_28`, `v0_1_29`, `v0_1_30`: identical files delegating to `v0_1_27.Decoder{}` (range [v0_1_27..v0_1_33]).

- [ ] **Step 10: Range-owner unit tests** — `internal/decoders/v0_1_8/supplier_test.go` (constructed-bytes roundtrips; REAL-bytes goldens land in Task 11):

```go
package v0_1_8

import (
	"bytes"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	"cosmossdk.io/math"
)

func TestDecodeSupplierMsgStakeRoundtrip(t *testing.T) {
	in := &supplier.MsgStakeSupplier{
		Signer:          "pokt1signer",
		OwnerAddress:    "pokt1owner",
		OperatorAddress: "pokt1operator",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(60000000000)},
		Services: []*shared.SupplierServiceConfig{{
			ServiceId: "eth",
			Endpoints: []*shared.SupplierEndpoint{{Url: "https://example.net", RpcType: shared.RPCType_JSON_RPC}},
		}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", raw)
	if err != nil {
		t.Fatalf("DecodeSupplierMsg: %v", err)
	}
	s := got.Stake
	if s == nil || s.OperatorAddress != "pokt1operator" || s.StakeAmount != 60000000000 || s.StakeDenom != "upokt" {
		t.Fatalf("decoded = %+v", got)
	}
	if !bytes.Contains(s.ServicesJSON, []byte(`"service_id":"eth"`)) {
		t.Fatalf("ServicesJSON = %s", s.ServicesJSON)
	}
}

func TestDecodeSupplierMsgSkipsForeignTypeURL(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

func TestDecodeSupplierEventStakedExtractsAndValidates(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		{Key: "supplier", Value: `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"60000000000"},"services":[{"service_id":"eth","endpoints":[{"url":"https://x","rpc_type":"JSON_RPC","configs":[]}]}]}`},
		{Key: "msg_index", Value: "0"},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	ev := got.Staked
	if ev == nil || ev.SessionEndHeight != 135840 {
		t.Fatalf("decoded = %+v", got)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
}

func TestDecodeSupplierEventSkipsUnknownType(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", nil)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

func TestDecodeSupplierKVRecordRoundtrip(t *testing.T) {
	in := &shared.Supplier{
		OwnerAddress:    "pokt1owner",
		OperatorAddress: "pokt1op",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(42)},
	}
	raw, _ := in.Marshal()
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op/"), raw, false)
	if err != nil || got.Supplier == nil {
		t.Fatalf("DecodeSupplierKV: %+v, %v", got, err)
	}
	if got.Supplier.StakeAmount != 42 || got.Supplier.ServicesJSON != nil {
		t.Fatalf("snapshot = %+v (dehydrated supplier must have nil ServicesJSON)", got.Supplier)
	}
}

func TestDecodeSupplierKVIgnoresIndexLayouts(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
```

CHECK the gen Coin field type: gen `MsgStakeSupplier.Stake` may be `*cosmostypes.Coin` (cosmos-sdk types) — confirm import path with `grep -n "Stake " internal/decoders/v0_1_8/gen/pocket/supplier/tx.pb.go`. Mirror equivalent tests for `v0_1_27` (operator_address variant) and a `v0_1_0` smoke test (constructed Supplier roundtrip).

- [ ] **Step 11: Run everything**: `go test ./internal/decoders/... ./internal/types/...` → PASS; `go build ./... && make ci` → clean (all 8 version packages now satisfy the grown interface; registry compiles).

- [ ] **Step 12: Commit**

```bash
git add internal/types/ internal/decoders/
git commit -m "feat(decoders): supplier tx/event/KV decode across 3 shape ranges + canonical types (tests 18-20 decode layer)"
```

---

### Task 7: Migration 0040 + store inserters + decoder-version map

**Files:** Create `schema/migrations/0040_supplier_service_config_update.sql`, `internal/store/supplier.go`, `internal/store/decoder_version.go`. Pattern to copy: `internal/store/block.go` (package-level `Insert*(ctx, tx pgx.Tx, …)`, `ON CONFLICT … DO NOTHING`, wrapped errors).

- [ ] **Step 1: Migration `0040_supplier_service_config_update.sql`:**

```sql
-- +goose Up
-- Phase E (decision 5): from v0.1.8 the chain stores Supplier DEHYDRATED; the
-- hydrated service-config truth lives in ServiceConfigUpdate primary KV records
-- (ServiceConfigUpdate/service_id/...). They are first-class chain state —
-- append-only snapshots here; hydration happens at QUERY time (invariant 3).
-- See docs/research/phase-e-spike-findings.md §4d.
CREATE TABLE IF NOT EXISTS supplier_service_config_update_history (
  operator_address    TEXT        NOT NULL,
  service_id          TEXT        NOT NULL,
  activation_height   BIGINT      NOT NULL,
  deactivation_height BIGINT      NOT NULL DEFAULT 0,
  service_config      JSONB       NULL,
  deleted             BOOLEAN     NOT NULL DEFAULT FALSE,
  block_height        BIGINT      NOT NULL,
  block_time          TIMESTAMPTZ NOT NULL,
  decoded_by_version  SMALLINT    NOT NULL,
  indexed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT supplier_scu_history_pk PRIMARY KEY (operator_address, service_id, activation_height, block_height),
  CONSTRAINT supplier_scu_history_decoder_fk FOREIGN KEY (decoded_by_version) REFERENCES decoder_version(id)
);
COMMENT ON TABLE supplier_service_config_update_history IS
  'Append-only snapshots of ServiceConfigUpdate primary KV records (chain stores Supplier dehydrated from v0.1.8; ADR-005). deactivation_height 0 = none. deleted=TRUE records a chain KV deletion.';
-- +goose Down
DROP TABLE supplier_service_config_update_history;
```

- [ ] **Step 2: Verify migrations** — run the `verify-migrations` skill (disposable TimescaleDB, `goose up`). Expected: all 40 apply clean.

- [ ] **Step 3: `internal/store/decoder_version.go`:**

```go
package store

import (
	"context"
	"fmt"
	"strings"
)

// DecoderVersionIDs returns tag → id from decoder_version (seeded by the
// per-version migrations, e.g. 'v0.1.8'→108). Loaded once at consumer startup.
func (s *Store) DecoderVersionIDs(ctx context.Context) (map[string]int16, error) {
	rows, err := s.pool.Query(ctx, `SELECT tag, id FROM decoder_version`)
	if err != nil {
		return nil, fmt.Errorf("load decoder versions: %w", err)
	}
	defer rows.Close()
	out := map[string]int16{}
	for rows.Next() {
		var tag string
		var id int16
		if err := rows.Scan(&tag, &id); err != nil {
			return nil, fmt.Errorf("scan decoder version: %w", err)
		}
		out[tag] = id
	}
	return out, rows.Err()
}

// DecoderTag converts a decoder package version ("v0_1_8") to the
// decoder_version.tag spelling ("v0.1.8").
func DecoderTag(version string) string { return strings.ReplaceAll(version, "_", ".") }
```

- [ ] **Step 4: `internal/store/supplier.go`** — nine inserters, all with the `InsertBlock` shape (`func InsertX(ctx context.Context, tx pgx.Tx, r *types.X, decodedBy int16) error`, exec + wrapped error). Empty-string TEXT fields and nil `[]byte` JSONB pass through as SQL NULL via a tiny helper `func nullStr(s string) any { if s == "" { return nil }; return s }`. The exact SQL per function (conflict target = the table PK):

```sql
-- InsertMsgStakeSupplier
INSERT INTO msg_stake_supplier (signer, owner_address, operator_address, stake_amount, stake_denom, services, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertMsgUnstakeSupplier
INSERT INTO msg_unstake_supplier (signer, operator_address, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertEventSupplierStaked
INSERT INTO event_supplier_staked (supplier, session_end_height, operator_address, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertEventSupplierUnbondingBegin / ...End (same columns both)
INSERT INTO event_supplier_unbonding_begin (supplier, reason, session_end_height, unbonding_end_height, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertEventSupplierUnbondingCanceled
INSERT INTO event_supplier_unbonding_canceled (supplier, height, session_end_height, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertEventSupplierServiceConfigActivated
INSERT INTO event_supplier_service_config_activated (supplier, activation_height, operator_address, service_id, block_height, block_time, tx_index, event_index, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (block_time, block_height, tx_index, event_index) DO NOTHING
-- InsertSupplierSnapshot
INSERT INTO supplier_history (operator_address, owner_address, stake_amount, stake_denom, services, unstake_session_end_height, service_config_history, block_height, block_time, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (operator_address, block_height) DO NOTHING
-- InsertServiceConfigUpdate. DO UPDATE fires ONLY when the SAME
-- (operator, service_id, activation_height) KV key is written more than once
-- at the SAME block_height (e.g. update + deletion in one block). It is NEVER
-- a cross-height update — block_height is in the PK (append-only preserved).
-- Deterministic KV enumeration order makes the same-height last-write-wins
-- idempotent across replays. Put this same justification in the store comment.
INSERT INTO supplier_service_config_update_history (operator_address, service_id, activation_height, deactivation_height, service_config, deleted, block_height, block_time, decoded_by_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (operator_address, service_id, activation_height, block_height) DO UPDATE
SET deactivation_height = EXCLUDED.deactivation_height, service_config = EXCLUDED.service_config,
    deleted = EXCLUDED.deleted, decoded_by_version = EXCLUDED.decoded_by_version
```

- [ ] **Step 5:** `go build ./internal/store/ && make ci` clean. Commit: `feat(store): supplier inserters + SCU history table (migration 0040) + decoder-version map`

---

### Task 8: Sidecar fan-out (ADR-022) — bootstrap rewrite

**Files:** Rewrite `internal/fileplugin/bootstrap.go`; modify `internal/app/fileplugin/cmd.go` (add `--config`, load `config.Load` like `internal/app/consumer/block.go:50` does, pass `cfg.Network.ChainID` — check the config struct field name with `grep -rn "ChainID\|chain_id" internal/config/`); rewrite `test/integration/fileplugin_test.go`.

- [ ] **Step 1: Write the failing integration test** (rewrite `test/integration/fileplugin_test.go`): Bootstrap over `test/fixtures/v0_1_0` (after Task 9 adds its `-data` files — if executing in order, copy `/tmp/block-1-data` etc. here, see Task 9 Step 1) must publish, per height: tx msgs (0 for these fixtures), event msgs, kv msgs, and the envelope **with the highest stream sequence of the height's messages** (ordering contract). Assert: (a) for each height the envelope's stream sequence > every fan-out sequence of that height (fetch via an ephemeral consumer reading all of `pokt.>`); (b) envelope unmarshals with correct `height/tx_count/kv_count/published_msg_count/chain_id`; (c) re-running Bootstrap publishes ZERO new messages (Nats-Msg-Id dedup); (d) Bootstrap errors if a `-data` file is missing.

- [ ] **Step 2: Rewrite `internal/fileplugin/bootstrap.go`:**

```go
package fileplugin

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

// Bootstrap republishes captured FilePlugin output as the ADR-022 fan-out:
// per-tx (pokt.tx.{H}.{i}), per-event (pokt.events.{type}.{H}), per-KV
// (pokt.kv.{store}.{H}) and FINALLY the metadata-only BlockEnvelope on
// pokt.block.{H} — the envelope is published LAST per height (ordering
// contract, ADR-022 amendment): consumers batch on it as the completeness
// fence. The split is STRUCTURAL: cometbft/cosmos containers only — no
// poktroll decode, no router (decision 1).
//
// Event/KV ordinals (used in Nats-Msg-Id and EventInBlock positions) follow
// the deterministic enumeration order of the captured files: block-level
// events first (ResponseFinalizeBlock.events), then per-tx events in tx
// order; KV pairs in data-file order. Returns (heights, messages) published.
func Bootstrap(ctx context.Context, client *natsx.Client, dir string, maxHeight int64, chainID string) (int, int, error) {
	pattern := filepath.Join(dir, "block-*-meta")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, fmt.Errorf("glob %s: %w", pattern, err)
	}
	type entry struct {
		height int64
		path   string
	}
	entries := make([]entry, 0, len(matches))
	for _, p := range matches {
		h, err := parseMetaHeight(filepath.Base(p))
		if err != nil {
			continue // skip non-conforming filenames
		}
		if maxHeight > 0 && h > maxHeight {
			continue
		}
		entries = append(entries, entry{height: h, path: p})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].height < entries[j].height })

	js := client.JetStream()
	publish := func(subj string, data []byte, msgID string) error {
		_, err := js.Publish(ctx, subj, data, jetstream.WithMsgID(msgID))
		return err
	}
	heights, total := 0, 0
	for _, e := range entries {
		n, err := fanOutHeight(ctx, publish, e.height, e.path, chainID)
		if err != nil {
			return heights, total, fmt.Errorf("height %d: %w", e.height, err)
		}
		heights++
		total += n
	}
	return heights, total, nil
}

// fanOutHeight publishes one height's fan-out + envelope. publish is injected
// for testability.
func fanOutHeight(_ context.Context, publish func(subj string, data []byte, msgID string) error, height int64, metaPath, chainID string) (int, error) {
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return 0, fmt.Errorf("read meta: %w", err)
	}
	dataPath := strings.TrimSuffix(metaPath, "-meta") + "-data"
	dataBytes, err := os.ReadFile(dataPath)
	if err != nil {
		return 0, fmt.Errorf("read data (FilePlugin always writes both, ADR-027): %w", err)
	}
	records, err := decoders.SplitMeta(metaBytes)
	if err != nil {
		return 0, err
	}
	var req abci.RequestFinalizeBlock
	if err := req.Unmarshal(records[0]); err != nil {
		return 0, fmt.Errorf("RequestFinalizeBlock: %w", err)
	}
	var resp abci.ResponseFinalizeBlock
	if err := resp.Unmarshal(records[1]); err != nil {
		return 0, fmt.Errorf("ResponseFinalizeBlock: %w", err)
	}
	header, err := decoders.DecodeBlockHeader(metaBytes)
	if err != nil {
		return 0, err
	}

	n := 0
	// ── txs ──
	for i, txBytes := range req.Txs {
		var resBytes []byte
		if i < len(resp.TxResults) {
			if resBytes, err = resp.TxResults[i].Marshal(); err != nil {
				return n, fmt.Errorf("tx_result %d: %w", i, err)
			}
		}
		raw, err := (&psv1.TxWithResult{Tx: txBytes, Result: resBytes}).Marshal()
		if err != nil {
			return n, err
		}
		subj := natsx.TxSubject(height, i)
		if err := publish(subj, raw, natsx.MsgID(subj, height, i)); err != nil {
			return n, err
		}
		n++
	}
	// ── events: block-level first, then per-tx (deterministic ordinal) ──
	ordinal := 0
	emit := func(ev abci.Event, txIndex int32, eventIndex int32) error {
		evBytes, err := ev.Marshal()
		if err != nil {
			return err
		}
		raw, err := (&psv1.EventInBlock{Event: evBytes, TxIndex: txIndex, EventIndex: eventIndex}).Marshal()
		if err != nil {
			return err
		}
		subj := natsx.EventSubject(ev.Type, height)
		if err := publish(subj, raw, natsx.MsgID(subj, height, ordinal)); err != nil {
			return err
		}
		ordinal++
		n++
		return nil
	}
	for k, ev := range resp.Events {
		if err := emit(ev, -1, int32(k)); err != nil {
			return n, fmt.Errorf("block event %d: %w", k, err)
		}
	}
	for ti, txr := range resp.TxResults {
		for k, ev := range txr.Events {
			if err := emit(ev, int32(ti), int32(k)); err != nil {
				return n, fmt.Errorf("tx %d event %d: %w", ti, k, err)
			}
		}
	}
	eventCount := ordinal
	// ── kv: raw StoreKVPair records in data-file order ──
	kvCount := 0
	rest := dataBytes
	for len(rest) > 0 {
		payload, consumed, err := decoders.ReadDelimited(rest)
		if err != nil {
			return n, fmt.Errorf("data record %d: %w", kvCount, err)
		}
		storeKey, err := decoders.StoreKeyOf(payload)
		if err != nil {
			return n, fmt.Errorf("data record %d: %w", kvCount, err)
		}
		subj := natsx.KVSubject(storeKey, height)
		if err := publish(subj, payload, natsx.MsgID(subj, height, kvCount)); err != nil {
			return n, err
		}
		kvCount++
		n++
		rest = rest[consumed:]
	}
	// ── envelope LAST (the fence) ──
	env := &psv1.BlockEnvelope{
		Height: height, TimeUnixNano: header.Time.UnixNano(),
		Hash: header.Hash, ProposerAddress: header.ProposerAddress, ChainId: chainID,
		TxCount: int32(len(req.Txs)), EventCount: int32(eventCount), KvCount: int32(kvCount),
		PublishedMsgCount: int32(n),
	}
	raw, err := env.Marshal()
	if err != nil {
		return n, err
	}
	subj := natsx.BlockSubject(height)
	if err := publish(subj, raw, natsx.MsgID(subj, height, 0)); err != nil {
		return n, err
	}
	return n + 1, nil
}
```

Supporting additions in `internal/decoders` (DRY: reuse the existing framing): export a THIN WRAPPER `func ReadDelimited(buf []byte) ([]byte, int, error)` delegating to the existing unexported `readDelimited` — do NOT rename it (`blockheader_test.go` calls it directly); adapt the wrapper to `readDelimited`'s REAL signature (CHECK it first — if it doesn't return an error, wrap its failure mode). Add to `meta.go`:

```go
// StoreKeyOf extracts only the store_key (field 1, string) of a
// cosmos.store.v1beta1.StoreKVPair record without a full unmarshal dependency
// here: unmarshal via cosmossdk.io/store/types (already in the module graph).
func StoreKeyOf(record []byte) (string, error) {
	var kv storetypes.StoreKVPair
	if err := kv.Unmarshal(record); err != nil {
		return "", fmt.Errorf("StoreKVPair: %w", err)
	}
	return kv.StoreKey, nil
}
```

(import `storetypes "cosmossdk.io/store/types"` — already an indirect dep; `go mod tidy` promotes it.)

- [ ] **Step 3:** Update `internal/app/fileplugin/cmd.go`: add `--config` (required), `config.Load`, pass chain id; print `published %d height(s), %d message(s)`. Update integration test → run → PASS. `make ci` + `golangci-lint run --build-tags=integration ./...` clean.

- [ ] **Step 4: do NOT commit yet.** Changing the `pokt.block.{H}` payload breaks `block_consumer_test.go` (publishFixture sends raw meta) until Task 9 migrates it — Tasks 8+9 land as ONE atomic commit at the end of Task 9 (the "make ci green per task" rule applies to the combined commit). Also update the two existing `fileplugin.Bootstrap(...)` call sites in `test/integration/fileplugin_test.go` (~lines 35, 90) to the new 5-arg/3-return signature in THIS task.

---

### Task 9: Block consumer migrates to the envelope; fixtures gain -data (commits together with Task 8)

**Files:** Modify `internal/consumer/block/handler.go` (+ its unit test), `internal/app/consumer/block.go`, `test/integration/block_consumer_test.go`; add `-data` files under `test/fixtures/`.

- [ ] **Step 1: Fixture `-data` files.** Copies exist in `/tmp` for heights 1, 78683, 135297, 287932, 382250 (`/tmp/block-{H}-data`). Blocks 2–3: list the v0.1.0 tarball with `rclone lsf pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/v0.1.0/` (≈9.9 MB), `rclone copyto` it to /tmp, `tar -tJf` to confirm member names, extract `./block-2-data ./block-3-data`. Place each next to its `-meta` in the matching `test/fixtures/v0_1_*/` dir.

- [ ] **Step 2: Handler rewrite (test first).** Unit test: constructed `BlockEnvelope` marshaled → `Handle` → fake inserter receives the mapped `types.BlockHeader`. New `Handle` (drop the `Router` interface + field entirely; `New(inserter Inserter)`):

```go
// Handle maps the sidecar's BlockEnvelope to the canonical BlockHeader and
// inserts the block row. No decoding and no router: the sidecar already
// decoded the version-invariant header (ADR-022 amendment).
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, msg consumer.Message) error {
	var env psv1.BlockEnvelope
	if err := env.Unmarshal(msg.Data); err != nil {
		return fmt.Errorf("block envelope at height %d: %w", msg.Height, err)
	}
	return h.inserter.InsertBlock(ctx, tx, &types.BlockHeader{
		Height: env.Height, Time: time.Unix(0, env.TimeUnixNano).UTC(),
		Hash: env.Hash, ProposerAddress: env.ProposerAddress, TxCount: int(env.TxCount),
	})
}
```

- [ ] **Step 3: App wiring.** In `internal/app/consumer/block.go` remove the router construction (`router.NewDBRouter…`) and, if nothing else uses it, the `--config` flag + `config.Load` (the supplier cmd in Task 10 takes over as the router's production user). `blockhandler.New(storeInserter{})`.

- [ ] **Step 4: Integration tests 16a/16b/17 migration.** In `test/integration/block_consumer_test.go`: replace `publishFixture` (raw meta publish) with a helper that drives the REAL pipeline — `bootstrapHeights(t, heights ...int64)` copies the chosen `block-{H}-{meta,data}` fixture pairs into a `t.TempDir()` and calls `fileplugin.Bootstrap(ctx, nats.Client, dir, 0, "pocket")`. Tests no longer build routers. Explicit checklist (`grep -n "startBlockRuntime\|NewStaticRouter" test/integration/block_consumer_test.go` finds them all): (a) drop the `rtr blockhandler.Router` parameter from `startBlockRuntime` (line ~38); (b) delete the three `router.NewStaticRouter(...)` constructions (lines ~130-135, ~200-203, ~232-240) and their `rtr` args at the three call sites; (c) remove the now-unused `router` import. Test 17 (gap): bootstrap heights {1,3} first, assert stall at 1, then bootstrap {2}, assert recovery to 3 — dedup makes re-publishing safe. Expected rows unchanged (`*-expected.json` untouched). Run `make test-integration` → tests 1–17 PASS.

- [ ] **Step 5: Commit Tasks 8+9 together:** `feat(fileplugin+consumer): ADR-022 fan-out + envelope-last contract; block consumer reads BlockEnvelope (tests 16-17 preserved)`

---

### Task 10: BatchRuntime (ADR-024 fence) + supplier handler + `ps consumer supplier`

**Files:** Create `internal/consumer/batch.go`, `internal/consumer/batch_test.go`, `internal/consumer/supplier/handler.go`, `internal/app/consumer/supplier.go`; modify `internal/consumer/types.go` (BatchHandler), `internal/app/consumer/cmd.go` (`cmd.AddCommand(newSupplierCmd())`), `internal/metrics/metrics.go` — exactly two additions: field `Buffered *prometheus.GaugeVec // fan-out messages buffered awaiting flush` in the `Consumer` struct, and `Buffered: gauge("buffered_messages", "Fan-out messages buffered per consumer awaiting the block-boundary flush."),` in the `NewConsumer` return literal.

- [ ] **Step 1: BatchHandler contract** (append to `internal/consumer/types.go`):

```go
// BatchHandler is the per-module logic for fan-out consumers (ADR-024): the
// runtime buffers a height's messages and calls FlushHeight ONCE inside the
// ack-after-commit transaction when the BlockEnvelope (the fence) arrives.
// msgs is every buffered fan-out message for the height in arrival order
// (deduplicated by Nats-Msg-Id); it is EMPTY for quiet heights — the handler
// must succeed writing nothing so the cursor still advances.
type BatchHandler interface {
	ID() string
	FirstValidVersion() string
	FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []Message) error
}
```

(import `psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"`.)

- [ ] **Step 2: `internal/consumer/batch.go`** — mirror `Runtime` (`runtime.go`) verbatim for `Run`/`consume`/reconnect/Nak; only `handle` differs:

```go
// BatchRuntime drives a fan-out consumer: buffer per height → flush on the
// pokt.block.{H} envelope in ONE Postgres tx → ack everything after commit
// (invariants 4+5; ADR-024 block-boundary fence; size/time valves are Phase G).
type BatchRuntime struct {
	handler  BatchHandler
	store    *store.Store
	consumer jetstream.Consumer
	logger   *slog.Logger
	metrics  *metrics.Consumer
	buf      map[int64]*heightBuf // accessed only from the consume goroutine
}

type heightBuf struct {
	msgs []Message
	acks []jetstream.Msg
	seen map[string]bool // Nats-Msg-Id dedup of AckWait redeliveries
}

func (r *BatchRuntime) handle(ctx context.Context, msg jetstream.Msg) error {
	id := r.handler.ID()
	subject := msg.Subject()
	height, err := natsx.HeightFromSubject(subject)
	if err != nil {
		_ = msg.Term()
		r.logger.Error("bad subject; terminating", "consumer", id, "subject", subject)
		return nil //nolint:nilerr // terminated, not propagatable
	}
	if !strings.HasPrefix(subject, "pokt.block.") {
		b := r.buf[height]
		if b == nil {
			b = &heightBuf{seen: map[string]bool{}}
			r.buf[height] = b
		}
		msgID := ""
		if md, err := msg.Metadata(); err == nil {
			msgID = fmt.Sprintf("%d", md.Sequence.Stream) // fallback ordering key
		}
		if hdr := msg.Headers().Get("Nats-Msg-Id"); hdr != "" {
			msgID = hdr
		}
		if b.seen[msgID] {
			_ = msg.Ack() // redelivery of an already-buffered message
			return nil
		}
		b.seen[msgID] = true
		b.msgs = append(b.msgs, Message{Height: height, Subject: subject, MsgID: msgID, Data: msg.Data()})
		b.acks = append(b.acks, msg)
		r.metrics.Buffered.WithLabelValues(id).Set(float64(len(b.msgs)))
		return nil
	}
	// ── the fence: envelope closes the height ──
	var env psv1.BlockEnvelope
	if err := env.Unmarshal(msg.Data()); err != nil {
		return fmt.Errorf("block envelope at height %d: %w", height, err)
	}
	b := r.buf[height]
	if b == nil {
		b = &heightBuf{} // quiet height: empty flush still advances the cursor
	}
	next, err := r.store.ProcessHeight(ctx, id, height, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.FlushHeight(ctx, tx, &env, b.msgs)
	})
	if err != nil {
		return err // envelope is Nak'd; buffered msgs stay buffered (dedup absorbs their redeliveries)
	}
	// Fan-out acks happen strictly AFTER commit (invariant 5). Do NOT "optimize"
	// by acking before/at buffering: an AckWait redelivery of a buffered msg hits
	// the seen-map (acked as duplicate); after a crash, redeliveries re-buffer
	// into an empty runtime, re-flush, and every insert is ON CONFLICT no-op.
	for _, a := range b.acks {
		_ = a.Ack()
	}
	delete(r.buf, height)
	r.metrics.Buffered.WithLabelValues(id).Set(0)
	r.metrics.Processed.WithLabelValues(id).Inc()
	r.metrics.Consolidated.WithLabelValues(id).Set(float64(next))
	if next < height {
		r.metrics.GapsTotal.WithLabelValues(id).Inc()
		r.logger.Warn("gap detected", "consumer", id, "from", next+1, "to", height-1, "processed", height)
	}
	return nil
}
```

`NewBatchRuntime(cfg BatchConfig)` mirrors `NewRuntime` (same fields, `Handler BatchHandler`, init `buf`). The envelope is acked by `consume`'s existing `msg.Ack()` after `handle` returns nil — buffered fan-out is acked inside `handle` (after commit), exactly once before the envelope's own ack. Copy `Run`/`consume` from `runtime.go` adjusting the handler type (do NOT extract a shared generic yet — two copies, documented; revisit in Phase F per "no premature abstraction").

- [ ] **Step 3: Unit test `batch_test.go`** (no containers — fake `jetstream.Msg` is heavy; instead test the pure parts): subject classification via `natsx.HeightFromSubject` is already covered; test `heightBuf` dedup and quiet-height flush by extracting `handle`'s buffer logic into small methods if needed. The full fence behavior is covered by integration tests 18/21 (Task 12). Keep this minimal — one test for "envelope with no buffer flushes empty and advances" using a real Postgres testcontainer is ALLOWED to live in `test/integration/supplier_consumer_test.go` instead (test 21 covers it via the v0_1_0 negative fixture).

- [ ] **Step 4: `internal/consumer/supplier/handler.go`:**

```go
// Package supplier implements the BatchHandler that indexes the supplier
// module: tx msgs → msg_*, typed events → event_supplier_*, KV writes →
// supplier_history + supplier_service_config_update_history. Decode is
// consumer-side via the router (ADR-008); rows record the REGISTERED decoder
// version the router returned (decision 8).
package supplier

import (
	"context"
	"fmt"
	"strings"
	"time"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// EventTypes are the supplier-module typed events this consumer subscribes to
// and persists (ADR-022 per-type subjects; tokenomics EventSupplierSlashed is
// the tokenomics consumer's job — decision 6).
var EventTypes = []string{
	"pocket.supplier.EventSupplierStaked",
	"pocket.supplier.EventSupplierUnbondingBegin",
	"pocket.supplier.EventSupplierUnbondingEnd",
	"pocket.supplier.EventSupplierUnbondingCanceled",
	"pocket.supplier.EventSupplierServiceConfigActivated",
}

// Router is the subset of router.Router this handler needs.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// Handler implements consumer.BatchHandler for the supplier module.
type Handler struct {
	router     Router
	versionIDs map[string]int16 // decoder_version tag → id (store.DecoderVersionIDs)
}

// New constructs the supplier handler.
func New(r Router, versionIDs map[string]int16) *Handler {
	return &Handler{router: r, versionIDs: versionIDs}
}

func (h *Handler) ID() string                { return "supplier" }
func (h *Handler) FirstValidVersion() string { return "v0.1.0" }

// FlushHeight decodes every buffered fan-out message for the height and writes
// the rows inside the runtime-managed transaction. Empty msgs (quiet height)
// writes nothing and succeeds.
func (h *Handler) FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []consumer.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	dec, err := h.router.DecoderFor(env.Height)
	if err != nil {
		return err
	}
	decodedBy, ok := h.versionIDs[store.DecoderTag(dec.Version())]
	if !ok {
		return fmt.Errorf("decoder version %s has no decoder_version row", dec.Version())
	}
	pos := types.Position{Height: env.Height, Time: time.Unix(0, env.TimeUnixNano).UTC()}
	for _, m := range msgs {
		switch {
		case strings.HasPrefix(m.Subject, "pokt.tx."):
			if err := h.flushTx(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		case strings.HasPrefix(m.Subject, "pokt.events."):
			if err := h.flushEvent(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		case strings.HasPrefix(m.Subject, "pokt.kv."):
			if err := h.flushKV(ctx, tx, dec, pos, decodedBy, m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected subject in supplier buffer: %s", m.Subject)
		}
	}
	return nil
}

func (h *Handler) flushTx(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	_, txIdx, err := natsx.HeightFromTxSubject(m.Subject)
	if err != nil {
		return err
	}
	var wrapped psv1.TxWithResult
	if err := wrapped.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("TxWithResult %s: %w", m.Subject, err)
	}
	var result abci.ExecTxResult
	if len(wrapped.Result) > 0 {
		if err := result.Unmarshal(wrapped.Result); err != nil {
			return fmt.Errorf("ExecTxResult %s: %w", m.Subject, err)
		}
	}
	if result.Code != 0 {
		return nil // failed tx: no state change, no events, no KV (decision 7)
	}
	var cosmosTx sdktx.Tx
	if err := cosmosTx.Unmarshal(wrapped.Tx); err != nil {
		return fmt.Errorf("cosmos tx %s: %w", m.Subject, err)
	}
	for j, anyMsg := range cosmosTx.Body.Messages {
		decoded, err := dec.DecodeSupplierMsg(anyMsg.TypeUrl, anyMsg.Value)
		if err != nil {
			return err
		}
		if decoded == nil {
			continue
		}
		p := pos
		p.TxIndex, p.EventIndex = int32(txIdx), int32(j) // event_index column = msg index for msg tables
		switch {
		case decoded.Stake != nil:
			decoded.Stake.Position = p
			if err := store.InsertMsgStakeSupplier(ctx, tx, decoded.Stake, decodedBy); err != nil {
				return err
			}
		case decoded.Unstake != nil:
			decoded.Unstake.Position = p
			if err := store.InsertMsgUnstakeSupplier(ctx, tx, decoded.Unstake, decodedBy); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) flushEvent(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	var wrapped psv1.EventInBlock
	if err := wrapped.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("EventInBlock %s: %w", m.Subject, err)
	}
	var ev abci.Event
	if err := ev.Unmarshal(wrapped.Event); err != nil {
		return fmt.Errorf("abci.Event %s: %w", m.Subject, err)
	}
	attrs := make([]types.EventAttr, 0, len(ev.Attributes))
	for _, a := range ev.Attributes {
		attrs = append(attrs, types.EventAttr{Key: a.Key, Value: a.Value})
	}
	decoded, err := dec.DecodeSupplierEvent(ev.Type, attrs)
	if err != nil {
		return err
	}
	if decoded == nil {
		return nil
	}
	p := pos
	p.TxIndex, p.EventIndex = max(wrapped.TxIndex, 0), wrapped.EventIndex // block-level (-1) stored as table-default 0
	switch {
	case decoded.Staked != nil:
		decoded.Staked.Position = p
		return store.InsertEventSupplierStaked(ctx, tx, decoded.Staked, decodedBy)
	case decoded.UnbondingBegin != nil:
		decoded.UnbondingBegin.Position = p
		return store.InsertEventSupplierUnbondingBegin(ctx, tx, decoded.UnbondingBegin, decodedBy)
	case decoded.UnbondingEnd != nil:
		decoded.UnbondingEnd.Position = p
		return store.InsertEventSupplierUnbondingEnd(ctx, tx, decoded.UnbondingEnd, decodedBy)
	case decoded.UnbondingCanceled != nil:
		decoded.UnbondingCanceled.Position = p
		return store.InsertEventSupplierUnbondingCanceled(ctx, tx, decoded.UnbondingCanceled, decodedBy)
	case decoded.ServiceConfigActivated != nil:
		decoded.ServiceConfigActivated.Position = p
		return store.InsertEventSupplierServiceConfigActivated(ctx, tx, decoded.ServiceConfigActivated, decodedBy)
	}
	return nil
}

func (h *Handler) flushKV(ctx context.Context, tx pgx.Tx, dec decoders.Decoder, pos types.Position, decodedBy int16, m consumer.Message) error {
	var kv storetypes.StoreKVPair
	if err := kv.Unmarshal(m.Data); err != nil {
		return fmt.Errorf("StoreKVPair %s: %w", m.Subject, err)
	}
	decoded, err := dec.DecodeSupplierKV(kv.Key, kv.Value, kv.Delete)
	if err != nil {
		return err
	}
	if decoded == nil {
		return nil
	}
	switch {
	case decoded.Supplier != nil:
		decoded.Supplier.Position = pos
		return store.InsertSupplierSnapshot(ctx, tx, decoded.Supplier, decodedBy)
	case decoded.ServiceConfigUpdate != nil:
		decoded.ServiceConfigUpdate.Position = pos
		return store.InsertServiceConfigUpdate(ctx, tx, decoded.ServiceConfigUpdate, decodedBy)
	}
	return nil
}
```

- [ ] **Step 5: `internal/app/consumer/supplier.go`** — clone `block.go`'s shape: flags `--config` (required) / `--dsn` / `--nats-url`; build store, nats, `EnsureStream`; durable `"supplier"` with **FilterSubjects** (plural — nats.go `jetstream.ConsumerConfig.FilterSubjects`):

```go
// TxSubjectFilter ("pokt.tx.>") intentionally delivers ALL modules' txs — the
// handler filters by type_url (spec §4.8 "filters internally"). Cost is
// O(tx_count) buffered msgs per height; fine-grained tx routing is a Phase G
// candidate (note added to the ADR-024 amendment).
filters := []string{natsx.TxSubjectFilter, natsx.KVSubjectFilter("supplier"), natsx.BlockSubjectFilter}
for _, et := range supplierhandler.EventTypes {
	filters = append(filters, natsx.EventSubjectFilter(et))
}
jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
	Durable: "supplier", FilterSubjects: filters,
	AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy,
	MaxDeliver: -1, AckWait: 60 * time.Second,
})
```

then `router.NewDBRouter(ctx, st, router.DefaultRegistry(), cfg.Network.GenesisDecoderVersion)`, `st.DecoderVersionIDs(ctx)`, `supplierhandler.New(rtr, ids)`, `consumer.NewBatchRuntime(...)`, `rt.Run(ctx)`. Register in `cmd.go`.

- [ ] **Step 6:** `make ci` clean (note `max()` builtin requires Go 1.21+ — fine on 1.26). Commit: `feat(consumer): BatchRuntime fence (ADR-024) + ps consumer supplier`

---

### Task 11: Fixture curation (4 supplier heights + golden blobs)

Network + disk heavy; no code logic. Heights chosen during grounding (LCD-verified, all OUTSIDE the ADR-021 non-deterministic window 94370–102141):

| Fixture dir | Height | Why | Covering tarball (`pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/...`) | Size |
|---|---|---|---|---|
| `test/fixtures/v0_1_10/` | 102542 | MsgStakeSupplier (v0.1.17 binary era → router resolves v0_1_10 → v0_1_8 range; validates lenient range-sharing on real bytes) | `v0.1.17/...` (find with `rclone lsf`) | ~12.5 MB |
| `test/fixtures/v0_1_20/` | 135836 | 4× MsgStakeSupplier + 4 staked events + 508 supplier KV (spike-verified) | `v0.1.20/v0.1.20-h138930-fileplugin.tar.xz` | ~36.6 MB |
| `test/fixtures/v0_1_28/` | 290584 | MsgStakeSupplier post-v0.1.27 event shape | `v0.1.28/...` | **4.33 GB** |
| `test/fixtures/v0_1_29/` | 385145 | 5+ MsgStakeSupplier in one block | `v0.1.29/...` | **4.90 GB** |

v0_1_0 stays NEGATIVE (decision 4): blocks 1–3 already in fixtures; zero supplier rows expected.

- [ ] **Step 1:** For each row: `rclone lsf` the version dir → `rclone copyto` the covering tarball to `/tmp` (WARN the user before the two >4 GB downloads; check `df -h /tmp` first) → `tar -xJf <tarball> ./block-{H}-meta ./block-{H}-data` → copy both into the fixture dir.
- [ ] **Step 2:** Write each height's `block-{H}-expected.json`. Extend the existing schema (see `test/fixtures/v0_1_20/block-135297-expected.json` for the current block-row keys) with a `supplier` object:

```json
{
  "height": 135836, "time": "<from decode>", "hash": "<from decode>", "proposer_address": "<from decode>", "tx_count": 4,
  "supplier": {
    "msg_stake": [{"tx_index": 0, "operator_address": "pokt16ar6g3wd9ppat0rtm390wdhnt06kf3z4u2mxm8", "stake_amount": 60000000000, "stake_denom": "upokt"}],
    "events_staked": [{"tx_index": 0, "session_end_height": 135840}],
    "history_operators": ["pokt12qse7...", "pokt15u74x...", "pokt15xacl...", "pokt16ar6g..."],
    "scu_rows_min": 100
  }
}
```

Derive values by decoding the fixture with the new decoder code (a `go run` one-off or the Task 12 test in verbose mode) and CROSS-CHECK operator addresses + amounts against mainnet LCD (`{lcd}/cosmos/tx/v1beta1/txs?query=tx.height=135836` — Sauron LCD load-balances over backends with uneven retention: retry up to 15× before trusting an empty result). Populate the real `pokt1...` values — never leave the `...` shorthand in committed files.
- [ ] **Step 3: Golden blobs** for the two positive ranges: from 135836 (v0_1_8 range) and 290584 (v0_1_27 range) extract one stake-msg `Any.value`, one staked-event attr set (JSON), one `Supplier/operator_address/` KV pair and one `ServiceConfigUpdate/service_id/` KV pair into `internal/decoders/testdata/supplier/<range>/` (adapt the spike's `spike/decodechain/main.go` from `docs/research/phase-e-spike-findings.md` as a /tmp one-off extractor). Add golden tests `internal/decoders/v0_1_8/supplier_golden_test.go` + `v0_1_27/supplier_golden_test.go` asserting decode of the real bytes against committed expected JSON (goldie or plain compare — follow `internal/decoders/blockheader_test.go`'s existing style).
- [ ] **Step 4:** `go test ./internal/decoders/...` green; commit fixtures + goldens: `test(fixtures): supplier-activity fixtures for 4 eras + range golden blobs`

---

### Task 12: Integration tests 18–21

**Files:** Create `test/integration/supplier_consumer_test.go`; modify `test/testcontainers/postgres.go` (`Reset` TRUNCATE list += `msg_stake_supplier, msg_unstake_supplier, event_supplier_staked, event_supplier_unbonding_begin, event_supplier_unbonding_canceled, event_supplier_unbonding_end, event_supplier_service_config_activated, supplier_history, supplier_service_config_update_history`); add a `startSupplierRuntime` helper mirroring `startBlockRuntime` (`block_consumer_test.go:38`) but building `NewBatchRuntime` + `supplierhandler.New(router, ids)` with a `NewStaticRouter(upgradesForFixtures, router.DefaultRegistry(), "v0_1_0")` where `upgradesForFixtures` declares the real mainnet boundaries used by the fixtures (`{v0.1.8@78671, v0.1.10@78683, v0.1.17@<applied>, v0.1.20@135297, v0.1.27@247893, v0.1.28@287932, v0.1.29@382250}` — applied heights from `docs/research/poktroll-versions.md`). NOTE the deliberate asymmetry: the BLOCK consumer tests do NOT add v0.1.8/v0.1.27 boundaries (header decode is version-invariant) — do not "fix" them.

Expected `decoded_by_version` per fixture (test 18) — trace of the lenient chain: 102542 sits in the v0.1.17 era (v0.1.17 unregistered) → nearest registered earlier = **v0_1_10 → id 110**; 135836 → v0_1_20 → 120; 290584 → v0_1_28 → 128; 385145 → v0_1_29 → 129.

Test 21 synchronization (no races): after starting the supplier runtime, POLL `consumer_registry` for the 'supplier' row (e.g. `SELECT EXISTS(SELECT 1 FROM consumer_registry WHERE name='supplier')` via `pg.Pool`, 5s timeout) BEFORE stopping its handle — `Run` registers asynchronously and stopping too early would leave it unregistered, making `IsSealed` trivially true and the test meaningless.

- [ ] **Step 1 — Test 18 (msg decode across versions):** `pg.Reset` + `freshStream`; start block + supplier runtimes; `bootstrapHeights` (Task 9 helper) for each positive fixture height {102542, 135836, 290584, 385145}; `waitCursor(supplier, H)` per height (non-contiguous heights: wait on `HasProcessed` instead of the contiguous cursor — mirror how test 16a polls rows, `block_consumer_test.go:101`); assert `msg_stake_supplier` rows match each `expected.json` `supplier.msg_stake` (query by `block_height`, compare tx_index/operator/amount/denom, and assert `decoded_by_version` equals the registered decoder's id: 110 for 102542, 120 for 135836, 128 for 290584, 129 for 385145 — this pins lenient version-recording, decision 8).
- [ ] **Step 2 — Test 19 (events):** same replay; assert `event_supplier_staked` rows per `expected.json.supplier.events_staked`; for 290584/385145 assert `operator_address IS NOT NULL AND supplier IS NULL` (post-27 shape) and for 102542/135836 the inverse (pre-27 embed).
- [ ] **Step 3 — Test 20 (KV history, append-only):** same replay; assert (a) one `supplier_history` row per operator in `expected.json.supplier.history_operators` with `services IS NULL` (dehydrated era!); (b) `supplier_service_config_update_history` count ≥ `scu_rows_min`; (c) append-only/out-of-order: bootstrap 135837 BEFORE 135836 in a fresh stream variant and assert identical final rows (commutativity).
- [ ] **Step 4 — Test 21 (AND-seal with supplier lag + quiet heights):** `pg.Reset` + fresh stream; bootstrap v0_1_0 heights {1,2,3} (negative fixtures); start ONLY the block runtime; `waitCursor(block, 3)`; assert `store.IsSealed(ctx, 3)` is FALSE (supplier registered? not yet — start it AFTER asserting block-only doesn't seal: with only `block` in `consumer_registry`, `IsSealed(3)` would be TRUE — so register the lag correctly: start the supplier runtime FIRST so it self-registers, then immediately stop it via its handle BEFORE bootstrapping (it has processed nothing), bootstrap, `waitCursor(block,3)`, assert NOT sealed (supplier row exists at 0); restart the supplier runtime; `waitCursor(supplier, 3)`; assert sealed AND `SELECT count(*) FROM supplier_history` = 0 + `msg_stake_supplier` = 0 (quiet heights advanced the cursor with zero rows — decisions 4 + ADR-024 amendment).
- [ ] **Step 5:** `make test-integration` → tests 1–21 PASS; `golangci-lint run --build-tags=integration ./...` clean. Commit: `test(integration): supplier consumer tests 18-21 (multi-version decode, events, append-only KV, AND-seal lag)`

---

### Task 13: Docs, skill updates, spec marker, final gauntlet

- [ ] **Step 1:** `docs/architecture/05-versioning.md`: replace the strict-variant note (~line 114) with the shape-complete-registry + CI shape-guard contract (lenient stays THE router; the guard makes registry completeness mechanical); delete the stale `internal/router/upgrades.go` reference (~line 48, removed per ADR-018).
- [ ] **Step 2:** `.claude/skills/add-decoder-version/SKILL.md`: rewrite step 3 (buf breaking → two-ephemeral-workspace recipe with `breaking: use: [WIRE]`; never bare version dirs) and step 4 (buf.yaml workspace entry is WRONG — one poktroll tree per workspace; new versions need NO buf.yaml change, gen runs via `scripts/gen_decoder_protos.sh <v>`); add steps: delete legacy `buf.yaml`/`buf.lock` from newly vendored trees; append version to `DECODER_GEN_VERSIONS` ONLY if it starts a new shape range; run stripregister (automatic in `make gen-proto`); update `internal/decoders/enums.go` imports if the version adds enum values; the shape-guard test will fail CI if a supplier-closure break version lacks a registry entry.
- [ ] **Step 3:** Spec `docs/superpowers/specs/2026-06-08-slice-1-design.md`: append the Phase E completion marker to the appendix (mirror the Phase D marker at ~line 665: deliverables, tests 18–21 green, dehydrated-Supplier/SCU finding, fan-out + envelope + BatchRuntime, shape-complete registry).
- [ ] **Step 4 — Final gauntlet:** `make ci` && `make gen-check` && `make test-integration` && `golangci-lint run --build-tags=integration ./...`; coverage: `go test -cover ./internal/decoders/...` (100% target on decode paths) and `./internal/...` ≥80%; `go list -deps ./... | grep -c archeology` → 0 (isolation). Commit docs: `docs: Phase E markers + versioning/skill updates`

---

## Self-review checklist (run before handing to execution)

1. **Spec coverage:** test 18 → Tasks 6+11+12; test 19 → 6+12; test 20 → 6+7+12; test 21 → 10+12; "replace NoOp #2" → Task 10 (`ps consumer supplier`; NoOp #2 only ever existed in tests); "extend decoder lib" → Tasks 2+5+6; fixtures → 9+11; AND-seal → free via self-registration (`internal/store/seal.go:12`).
2. **Known verification points for the executor** (marked CHECK above): gen field spellings (Step 6 Task 6), `readDelimited` signature (Task 6), config ChainID field (Task 8), enum maps present in older trees (Task 6), v0.1.17 applied height (Task 12).
3. **Invariants:** no UPDATE on chain data (SCU DO UPDATE is same-height idempotent re-write — reviewed as the one deliberate exception, documented in 0040 + store comment); (block_height, block_time) on every new table; no `valid_to_*`; chain-as-truth (KV snapshots; events validated but JSONB stored raw); deterministic PKs; ack-after-commit preserved by BatchRuntime (commit BEFORE any ack).

**Plan complete.** Execution: superpowers:subagent-driven-development (fresh implementer per task, spec-review on Tasks 6/8/10/12, final whole-branch review) after the adversarial plan review.
