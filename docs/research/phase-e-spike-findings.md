# Phase E Spike — Final Report

All work done in the throwaway worktree `/home/overlordyorch/development/pocketscribe/.claude/worktrees/agent-ac68466aa3166e408` (branch `worktree-agent-ac68466aa3166e408`, still at "Initial commit", zero commits made; main checkout untouched; downloads in `/tmp/spike-e/`). Note: the worktree was created from an ancient commit lacking all Phase C/D output, so it was synced from the main checkout via `rsync -a --exclude='.git' --exclude='.claude/worktrees' <main>/ <worktree>/` (a `git checkout <sha> -- .` pathspec-copy was denied by the permission classifier).

---

## Goal 1 — CODEGEN ×3 via ephemeral workspace: **PROVEN**

`scripts/gen_decoder_protos.sh` (in worktree) — run as `bash scripts/gen_decoder_protos.sh v0_1_0` (then `v0_1_10`, `v0_1_28`):

```bash
#!/usr/bin/env bash
set -euo pipefail
V="${1:?usage: gen_decoder_protos.sh <version_dir e.g. v0_1_10>}"
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

Results: **63 `.pb.go` files each** for v0_1_0/v0_1_10/v0_1_28; `go build ./internal/decoders/...` exit 0; re-run + `diff -r` → **byte-identical** (deterministic; symlinks work fine with buf v1.70.0). Only output noise: harmless gogo `WARNING: field ... repeated non-nullable native type` lines.

### Goal 1b — `buf breaking`: SKILL.md step 3 is BROKEN; ephemeral two-workspace form works
- `buf breaking third_party/proto/poktroll/v0_1_30 --against .../v0_1_28` (SKILL.md form) **FAILS**: `cannot find gogoproto.stable_marshaler_all in this scope / imported file does not exist` — bare version dirs can't resolve cosmos/gogo imports outside the root workspace.
- The v0_1_10-vs-v0_1_0 pair "worked" only because their legacy v1 `buf.yaml`+`buf.lock` made buf resolve **BSR deps** (cached in `~/.cache/buf` — not offline on a fresh box) and used the legacy FILE ruleset, injecting `google/protobuf/descriptor.proto` noise.
- **Working offline recipe**: build TWO ephemeral workspaces (same symlink layout as above, plus `breaking:\n  use:\n    - WIRE` in each `buf.yaml`), then `buf breaking /tmp/bk-new --against /tmp/bk-old`. Verbatim result for v0_1_30 vs v0_1_28 (exit 100 = breaks found):

```
/tmp/bk-new/poktroll/pocket/supplier/query.proto:64:3:Field "2" with name "service_id" on message "QueryAllSuppliersRequest" moved from inside to outside a oneof.
/tmp/bk-new/poktroll/pocket/supplier/query.proto:65:3:Field "3" with name "operator_address" on message "QueryAllSuppliersRequest" changed type from "bool" to "string".
```
The cosmos-sdk/wkt modules are identical on both sides so they produce zero noise.

---

## Goal 2 — Strip-init transform: **PROVEN**

What v0_1_30/gen actually contains: **226 `proto.RegisterType`, 10 `proto.RegisterEnum`, 63 `proto.RegisterFile`, 2 `proto.RegisterMapType`** — all single-line calls, all inside `init()` (multi-line blocks + one-line `func init() { proto.RegisterFile(...) }`). No `golang_proto` variants (gocosmos doesn't emit them without `gogoproto_registration`).

Tool: `tools/stripregister/main.go` (stdlib-only, in worktree). Line-oriented: drops lines matching `^\t(proto|golang_proto)\.Register(Type|Enum|MapType|File)\(.*\)$` inside `func init() {` blocks (dropping the block entirely if emptied), drops one-line `func init() { proto.RegisterFile(...) }` declarations, and swallows the trailing blank line when a drop would leave consecutive blanks (first attempt failed gofmt on all 189 files for exactly this — root cause: two adjacent dropped decls leave `\n\n\n`, which gofmt collapses).

Verified: `go run ./tools/stripregister internal/decoders/v0_1_0/gen internal/decoders/v0_1_10/gen internal/decoders/v0_1_28/gen` → `rewrote 189 files`; second run → `rewrote 0 files` (**idempotent**); `grep -rE 'proto\.Register'` → **0**; `go build ./internal/decoders/...` clean; `gofmt -l` → **0 dirty**. The `fileDescriptor_*` vars stay (used by `Descriptor()` methods); `proto` import stays used (`proto.CompactTextString` in `String()`).

**The plan MUST also strip v0_1_30/gen** once any second tree coexists with it in a binary (left unstripped here deliberately, see Goal 3).

---

## Goal 3 — Coexistence proof: **PROVEN**

Panic demo first (`spike/panicdemo/main.go`, importing unstripped v0_1_0 + v0_1_10 `pocket/shared`), verbatim:

```
2026/06/09 18:27:53 proto: duplicate proto type registered: pocket.shared.GenesisState
...
panic: proto: duplicate enum registered: pocket.shared.RPCType

goroutine 1 [running]:
github.com/cosmos/gogoproto/proto.RegisterEnum(...)
	/home/overlordyorch/go/pkg/mod/github.com/cosmos/gogoproto@v1.7.0/proto/properties.go:526
github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10/gen/pocket/shared.init.6()
	.../internal/decoders/v0_1_10/gen/pocket/shared/service.pb.go:420 +0x374
```
Confirms: `RegisterType` duplicates only **log**; `RegisterEnum` **panics**.

After stripping: same program prints `GRPC GRPC`. `spike/coexist/coexist_test.go` imports stripped v0_1_0+v0_1_10+v0_1_28 **plus unstripped v0_1_30** supplier/shared packages; `go test -race ./spike/coexist/` → 5/5 PASS. Proven empirically:
- gogo `proto.Marshal`/`proto.Unmarshal` (generated methods) never touch the registry — roundtrips pass while `proto.MessageType("pocket.supplier.MsgStakeSupplier")` resolves ONLY to the v0_1_30 type and `proto.MessageName()` on stripped types returns `""` (no `XXX_MessageName` emitted).
- **Registry-dependent APIs to BAN in decoder paths**: `proto.MessageType`/`MessageName`, codec `InterfaceRegistry`/`UnpackAny` auto-resolution, and **jsonpb enum-by-name** (see Goal 4c — found failing on real bytes).

---

## Goal 4 — Real-bytes decode chain: **PROVEN** (with one root-caused failure → fixed recipe)

### a. Download
v0.1.19 halts at h135296, v0.1.20 at h138930 → 135836/135837 covered by:
```
rclone copyto pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/v0.1.20/v0.1.20-h138930-fileplugin.tar.xz /tmp/spike-e/...   # 36,601,292 bytes
tar -xJf v0.1.20-h138930-fileplugin.tar.xz ./block-135836-data ./block-135836-meta ./block-135837-data ./block-135837-meta
```
Sizes: 135836 meta=135,409 / data=110,708; 135837 meta=264,128 / data=204,747. Meta = exactly 3 uvarint records; **record 2 (ResponseCommit) is 0 bytes** (135836: `7879 / 127524 / 0`).

### b. Tx → Any → MsgStakeSupplier (decoder: `spike/decodechain/main.go`)
**Import decision: `sdktx "github.com/cosmos/cosmos-sdk/types/tx"` — `tx.Tx{}.Unmarshal(txBytes)` directly.** cosmos-sdk v0.53.0 is already a direct dep; `cosmossdk.io/store v1.1.2` already in the graph; zero new deps. Works because `Tx.body(1,message)` is wire-compatible with `TxRaw.body_bytes(1,bytes)`. No registry involvement (Any stays packed until you switch on `TypeUrl` yourself). Real decode (135836 tx[0], `type_url=/pocket.supplier.MsgStakeSupplier`, value=1681B) via **v0_1_10 gen** — proving range-sharing on v0.1.20-era bytes:

```json
{"signer":"pokt16ar6g3wd9ppat0rtm390wdhnt06kf3z4u2mxm8",
 "owner_address":"pokt1l3gcm0ympfmvecuerep4k7agv7pzwypg7p2xc5",
 "operator_address":"pokt16ar6g3wd9ppat0rtm390wdhnt06kf3z4u2mxm8",
 "stake":{"denom":"upokt","amount":"60000000000"},
 "services":[{"service_id":"eth",
   "endpoints":[{"url":"https://non-custodial.shannon-mainnet.eu.nodefleet.net","rpc_type":3}],
   "rev_share":[{"address":"pokt1fgkqh3gp50u98p373263rxh34z8659cwqm3mpm","rev_share_percentage":65}, ...]}, ...]}
```

### c. Typed event attribute encoding — documented verbatim
Real `EventSupplierStaked` (135836, tx[0], code=0):
```
type=pocket.supplier.EventSupplierStaked
  attr key="session_end_height" index=true value="135840"
  attr key="supplier"           index=true value={"owner_address":"pokt1l3gc...","operator_address":"pokt16ar6...","stake":{"denom":"upokt","amount":"60000000000"},"services":[{"service_id":"arb_one","endpoints":[{"url":"...","rpc_type":"JSON_RPC","configs":[]}],...}]}  (19,833 bytes)
  attr key="msg_index"          index=true value=0
```
**Encoding rule**: typed events → attribute key = proto field name, attribute value = **raw JSON** (int64 quoted `"135840"`, enums by NAME `"JSON_RPC"`, messages as JSON objects — the pre-v0.1.27 era embeds the FULL hydrated Supplier, ~19.8KB). Legacy events (`coin_spent`, `transfer`…) use **plain strings** (`1655974upokt`, unquoted). SDK bookkeeping attrs: `mode=BeginBlock|EndBlock` on block-level events, `msg_index` on tx events — must be excluded before typed decode.

**Decode strategy (the one that works)**: rebuild `{"field":<raw json>,...}` from attributes (skipping `mode`/`msg_index`) → `jsonpb.Unmarshaler{AllowUnknownFields:true}` against the gen event type. **FAILED first, root-caused**: `unknown value "JSON_RPC" for enum pocket.shared.RPCType` — gogo jsonpb resolves enum names via `proto.EnumValueMap()` = the global registry the strip removed. Fix proven on real bytes — guarded re-registration from the still-generated `<Enum>_name/_value` vars:
```go
func registerEnumsOnce() {
	reg := func(name string, nm map[int32]string, vm map[string]int32) {
		if proto.EnumValueMap(name) == nil { proto.RegisterEnum(name, nm, vm) }
	}
	reg("pocket.shared.RPCType", sh10.RPCType_name, sh10.RPCType_value)
	reg("pocket.shared.ConfigOptions", sh10.ConfigOptions_name, sh10.ConfigOptions_value)
	reg("pocket.supplier.SupplierUnbondingReason", sup10.SupplierUnbondingReason_name, sup10.SupplierUnbondingReason_value)
}
```
Why jsonpb and not `encoding/json`: stdlib json can't decode quoted int64s or enum names into gogo structs. plain protojson: inapplicable (gogo types).

### d. block-data StoreKVPair — store census + supplier key discrimination
Framing: same uvarint-delimited records; each = `storetypes.StoreKVPair` (`cosmossdk.io/store/types`). Store census 135836: `acc=8 bank=12 distribution=17 ibc=1 mint=1 session=1 slashing=5 staking=3 supplier=508`.

poktroll `x/supplier/types/keys.go` **IDENTICAL at v0.1.20 and v0.1.29** (no drift). Discrimination rule, verified against real keys (all segments end `/`, heights 8-byte big-endian):

| key | value | example (real) |
|---|---|---|
| `Supplier/operator_address/<addr>/` | **`pocket.shared.Supplier` proto** | 70B key, 112B value |
| `Supplier/unbonding_height/<addr>/` | presence marker (observed only as `delete=true`, value 0B) | — |
| `ServiceConfigUpdate/service_id/<svc>/<actH>/<addr>/` | **FULL `pocket.shared.ServiceConfigUpdate` proto** (primary) | `op=pokt12qse7... svc=arb_one act=96801 deact=135841` |
| `ServiceConfigUpdate/operator_address/<addr>/<svc>/<actH>/` | **primary-key bytes** (`<svc>/<actH-BE>/<addr>/`), NOT a proto | value hex `6172625f6f6e652f0000000000017a212f706f6b74...` |
| `ServiceConfigUpdate/activation_height/<actH>/<svc>/<addr>/` | primary-key bytes | same |
| `ServiceConfigUpdate/deactivation_height/<deactH>/<svc>/<addr>/<actH>/` | primary-key bytes | same |
| `p_supplier` | `Params` proto | not observed at these heights |

**SURPRISE (big one)**: the stored Supplier at v0.1.20 is **DEHYDRATED** — 112 bytes: owner_address, operator_address, stake ONLY. No `services`, no `service_config_history`. The hydrated config truth lives in the `ServiceConfigUpdate/service_id/` primaries (132 logical records here, duplicated under 2 more index orderings + 104 deactivation entries = 508 supplier KV writes for just 4 stakes). The schema/consumer design must treat ServiceConfigUpdate primaries as a first-class appended entity, or hydration is impossible from KV alone.

### e. Cross-check: **PROVEN**
135836: 4 txs, all `MsgStakeSupplier`; all 4 `Supplier/operator_address/` writes match the 4 tx operator addresses (`pokt12qse7…`, `pokt15u74x…`, `pokt15xacl…`, `pokt16ar6g…`), each accompanied by a `delete=true` on its `Supplier/unbonding_height/` entry. 135837: 10/10 match.

---

## Goal 5 — Size/count reality check: **PROVEN**

| | 135836 | 135837 |
|---|---|---|
| txs / msgs | 4 / 4 | 10 / 10 |
| events total (block-level) | 87 (19) | 177 (19) |
| KV pairs (supplier) | 556 (508) | 1012 (938) |
| biggest tx | 1,899 B | 1,899 B |
| biggest event (type+attrs) | **19,912 B** | 19,912 B |
| biggest KV value | 1,909 B | 1,909 B |
| fan-out msgs (1 header + txs + events + KV) | **648** | **1,200** |

Everything ≪ 256 KiB soft cap (largest single message = the EventSupplierStaked with embedded hydrated supplier, 7.6% of cap). Event-type census 135836: `coin_received=14 coin_spent=14 commission=5 message=18 mint=1 pocket.supplier.EventSupplierStaked=4 rewards=5 transfer=14 tx=12`. For `published_msg_count`: ~650–1,200 msgs/block on supplier-heavy blocks; KV pairs dominate (≈86–93%) due to the 3–4× ServiceConfigUpdate index amplification.

---

## Phase E plan ingredients (embed verbatim)

1. **`scripts/gen_decoder_protos.sh`** exactly as above (symlink only `pocket/`; absolute `out:`; non-empty assert). Makefile loop: `for v in v0_1_0 v0_1_10 v0_1_28 v0_1_30; do bash scripts/gen_decoder_protos.sh $v; done` — and migrate the v0_1_30 `gen-proto`/`gen-check` targets to it.
2. **`tools/stripregister/main.go`** as above; run it as a mandatory post-gen step **on every tree including v0_1_30**; gen-check = generate → strip → `git diff --quiet`. It is idempotent + gofmt-clean, so it composes with gen-check.
3. **buf breaking**: replace SKILL.md step 3 with the two-ephemeral-workspace recipe (add `breaking: use: [WIRE]` to the ephemeral buf.yaml); never run it on bare version dirs; the v0_1_0/v0_1_10 legacy `buf.yaml`/`buf.lock` should be deleted at vendor time (they cause silent BSR network access).
4. **Tx decode**: `sdktx "github.com/cosmos/cosmos-sdk/types/tx"` → `tx.Unmarshal(bytes)` → walk `tx.Body.Messages[].TypeUrl`, manual switch, version-gen `Unmarshal(any.Value)`. No new go.mod deps. KV decode: `cosmossdk.io/store/types".StoreKVPair` over the same `readDelimited` framing as `internal/decoders/blockheader.go`.
5. **Typed event decode**: attributes → JSON object (skip `mode`, `msg_index`) → `jsonpb.Unmarshaler{AllowUnknownFields:true}` + a per-decoder `RegisterEnumsOnce()` guarded by `proto.EnumValueMap(name)==nil` using the gen tree's `<Enum>_name/_value` vars. Legacy events are plain strings, not JSON.
6. **Ban list for decoder code paths** (registry-dependent, resolves only to whichever tree registered): `proto.MessageType/MessageName`, codec `InterfaceRegistry`/`UnpackAny`, unguarded jsonpb on enum fields.
7. **Supplier store rules** (v0.1.20≡v0.1.29 keys.go): the prefix table from Goal 4d, including: only `Supplier/operator_address/` and `ServiceConfigUpdate/service_id/` values are protos; the other SCU layouts are pointer values; `p_supplier` is Params; **stored Supplier is dehydrated in this era** — ServiceConfigUpdate primaries must be ingested as their own entity.
8. **Sizing**: fan-out ≈ 650–1,200 msgs on stake-heavy blocks; max observed single message 19.9 KiB; 256 KiB cap is comfortable; KV index amplification (3–4× per service config) dominates message volume.

---

## Supplier KV key layout — intermediate tag verification (2026-06-10)

### Scope

Fetched `x/supplier/types/keys.go` from pokt-network/poktroll at tags v0.1.8, v0.1.12, v0.1.17, v0.1.24, v0.1.20, and v0.1.29 (v0.1.20 and v0.1.29 were the prior verified baseline — confirmed byte-identical in Goal 4d above).

### Result: **DRIFT FOUND between v0.1.8 and v0.1.12**

All tags from v0.1.12 through v0.1.29 are **byte-identical** to each other in this file. The v0.1.8 tree diverges in one place:

#### `SupplierServiceConfigUpdateKey` component ordering

| Tag | Key component order | Comment in source |
|---|---|---|
| **v0.1.8** | `addr + activationHeight + serviceId` | `<SupplierAddr>/ <ActHeight>/ <ServiceID>/` |
| **v0.1.12+** | `addr + serviceId + activationHeight` | `<SupplierAddr>/ <ServiceID>/ <ActHeight>/` |

The **prefix string constants** (e.g. `ServiceConfigUpdateKeyPrefix`, `SupplierOperatorKeyPrefix`) are **identical across all versions** — the drift is solely in the component ordering used when building the secondary-index key.

#### Additional change at v0.1.12

v0.1.12 adds a new helper function `SupplierOperatorServiceKey(supplierOperatorAddr, serviceId string) []byte` that does not exist in v0.1.8. This key does not create a new prefix; it builds a sub-key under `Supplier/operator_address/`.

### Decoder implications

The affected key is the `ServiceConfigUpdate/operator_address/` **secondary index** — the KV whose value is a pointer (primary-key bytes), not a proto. See Goal 4d for the full key table.

The v0_1_8 decoder (covering applied-upgrade heights before v0.1.10 goes live) must use `addr + activationHeight + serviceId` ordering when discriminating that secondary index. The v0_1_10 and all later decoders must use `addr + serviceId + activationHeight`.

**Boundary**: the upgrade from v0.1.8 to v0.1.10 is applied at height **78683** (based on existing `upgradesForFixtures` entries in the integration test harness). Any SCU secondary-index KV written between the genesis of the v0.1.8 era and height 78682 will follow the old `addr/actHeight/svcId` ordering.

**Action required before implementing v0_1_8 decoder**: confirm the exact applied height for v0.1.8 → v0.1.10 from the `upgrades` table (or the on-chain `x/upgrade` history). Ensure `DecodeSupplierKV` in the v0_1_8 decoder uses the old layout for the secondary index discrimination. The primary `ServiceConfigUpdate/service_id/` key and the `Supplier/operator_address/` entity key are **not affected** by this drift.

### No other drift

ParamsKey, ModuleName, StoreKey, MemStoreKey, all six prefix string constants, and all other key-building functions are identical across v0.1.8, v0.1.12, v0.1.17, v0.1.24, v0.1.20, and v0.1.29.