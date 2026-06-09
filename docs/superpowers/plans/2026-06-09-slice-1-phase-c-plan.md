# Slice 1 Phase C — Codegen Pipeline Validated End-to-End on ONE Version (v0.1.30) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the multi-version decoder codegen pipeline end-to-end on a single poktroll version (v0.1.30) — buf-generate compilable Go bindings for all 9 poktroll modules into `internal/decoders/v0_1_30/gen/`, hand-fill the block-header decoder (the one category Phase D's block consumer needs), and make the codegen pattern repeatable — so that scaling to 32 versions in Phase F carries no unknown toolchain risk.

**Architecture:** Two cleanly separable threads land in this phase. (1) **Codegen pipeline:** a fully-offline buf workspace (three local proto module roots: poktroll `v0_1_30`, the already-vendored `cosmos-sdk/v0_53_0`, and a new `third_party/proto/wkt` holding the well-known-type protos) plus a per-version `buf.gen` template whose managed mode rewrites every poktroll file's `go_package` under our module path so all 32 versions can coexist without import collisions. (2) **Block-header decoder:** the FilePlugin `block-{H}-meta` file is a uvarint-length-delimited stream whose **first record is a cometbft `abci.RequestFinalizeBlock`** — an upstream cometbft type (via the pokt-network fork `replace`), *version-invariant across all 32 releases*. So `DecodeBlockHeader` lives once as a shared `decoders` helper and each versioned `Decoder` delegates to it. The codegen output (poktroll `pocket.*` bindings) is committed and proven to compile here; it is consumed by the entity/tx/event decoders in Phase E+, not by the block-header path.

**Tech Stack:** Go 1.26 · buf v1.70.0 (pinned, `go install`, NOT a go.mod dependency) · protoc-gen-gocosmos v1.7.0 (gogoproto plugin) · cometbft (pokt-network fork via `replace`) for `abci/types` · cosmos/gogoproto · cosmos-sdk · cosmossdk.io/api · stdlib `testing` (NO testify — consistent with Phase B) · stdlib `encoding/binary` + `encoding/hex`. Codegen is fully offline (verified with `GOPROXY=off`): every proto import resolves from the three local module roots, zero buf.build/BSR access.

**Spec reference:** `docs/superpowers/specs/2026-06-08-slice-1-design.md` Section 9 **Phase C**; design constraints Sections 4.2 (Decoder interface), 4.7 (block consumer target), 7.1 (codegen), 7.4 (version handling); ADR-008 (versioned decoders), ADR-010 (block table / `(block_height, block_time)` axis), ADR-016 (codegen via buf).

**ADR constraints honored:** ADR-008 (per-version decoder package `internal/decoders/v{X}_{Y}_{Z}/` with `gen/` subdir; never modify a committed version; `gen/` read-only), ADR-010 (block header carries the consensus `(height, time)` axis; `time` is chain time, never indexer wall-clock), ADR-016 (buf for proto codegen).

**Pre-existing artifacts the plan builds on (verified on `main` @ `da9337b`):**
- `internal/decoders/doc.go`, `internal/types/doc.go`, `internal/router/doc.go` — package-doc stubs ONLY. revive enforces a single package comment per package, so new `.go` files added to `decoders`/`types` MUST NOT carry a package comment (the `doc.go` already has it). The brand-new `internal/decoders/v0_1_30` package DOES carry its package comment (exactly one file: `decoder.go`).
- `third_party/proto/poktroll/v0_1_30/` — 63 vendored `.proto` files across 9 modules (`pocket.application`, `.gateway`, `.migration`, `.proof`, `.service`, `.session`, `.shared`, `.supplier`, `.tokenomics`); gogoproto-based; `go_package` points at upstream `github.com/pokt-network/poktroll/x/<m>/types`.
- `third_party/proto/cosmos-sdk/v0_53_0/` — vendored cosmos/tendermint/amino protos, including `tendermint/abci/types.proto` (`RequestFinalizeBlock`). Has its own nested `buf.yaml` (v1) — tolerated by the v2 workspace (spike-verified).
- `archeology/samples/block-190974-meta` — a REAL captured FilePlugin meta file (2864 bytes, v0.1.30-era). Block header is version-invariant, so it is unambiguously valid as Phase C's golden fixture. Copied into `internal/decoders/testdata/`. (`archeology/` is a separate Go module — we copy a byte fixture, never its deps.)
- `schema/migrations/0001_init.sql` — defines the `block` table (target shape for `types.BlockHeader`). Highest migration is `0039_consumer_registry.sql`. **Phase C adds NO migration** (block header writes nothing new to schema; the block CONSUMER is Phase D).
- `.golangci.yml` — already excludes `internal/decoders/.*/gen/` from ALL linters (line 73). Generated code needs no lint care.
- `Makefile` — has `ci` (= `vet fmt-check lint test`), `verify-migrations`, etc. NO `gen-proto`/`gen-check` targets yet (this plan adds them; `add-decoder-version` already references `make gen-proto`/`make gen-check`).
- `go.mod` — Phase B baseline (cobra/viper/pgx/nats/goose/prometheus/testcontainers; `prometheus v1.23.2`). `go.sum` committed and `go mod verify`-clean. The decode deps (cosmos/cometbft/proto) are absent and return in this phase.
- `.claude/skills/add-decoder-version/SKILL.md` — the codegen+scaffold home (per the design decision for this phase). Its current steps 4–7 (buf config / generate / scaffold) are aspirational stubs; Task 5 makes them real.

---

## Hard rules for the executor (read once, obey throughout)

1. **No `Co-Authored-By` / AI-attribution footer** in any commit message. Project rule (memory `feedback_no_claude_signature`).
2. **`HANDOFF.md`, `RESUME.md`, `SESSION-LOG-*.md` are LOCAL-ONLY** — never `git add`/commit them.
3. **No `time.Now()` / `clock_timestamp()` as a queryable axis** (`forbidigo` enforces). The block header's `Time` is the *chain consensus* time (`RequestFinalizeBlock.Time`), never indexer wall-clock. Decoder code never calls `time.Now()` (decoders rule + Invariant 1).
4. **Decoders: a version is a NEW directory, never a modification (ADR-008).** `internal/decoders/v0_1_30/` is created once here. `internal/decoders/v*/gen/` is buf-generated and read-only — never hand-edit; regenerate via `make gen-proto`.
5. **`archeology/` is a separate Go module (`github.com/pokt-network/pocketscribe/archeology`) and MUST NOT contaminate the main module.** Verified clean on `main`: `go list -m all` shows zero `poktroll`/`archeology` entries, no `go.work` exists, and `go build ./...` never compiles archeology. Preserve this absolutely:
   - **NEVER** run `go get github.com/pokt-network/poktroll...` and **NEVER** create a `go.work` file. Either one unions archeology's shim (`archeology/patches/morse_claimable_account_shim.go`, which imports `poktroll/app/pocket`) into the main graph — this is exactly what broke the Phase C spike's throwaway worktree. The main module never imports poktroll directly.
   - The codegen's managed-mode `go_package` override IS the anti-contamination mechanism: it rewrites every generated `gen/` import to **our** module path (`.../internal/decoders/v0_1_30/gen/...`), NOT `github.com/pokt-network/poktroll/x/...`. If that override regresses, `gen/` would import poktroll and `tidy` would drag in the whole poktroll app graph. Tasks 2 and 6 guard against this explicitly.
   - Copying a captured byte fixture (`block-190974-meta`) into `internal/decoders/testdata/` is fine — it is data, not code or deps.
6. **Adding a dependency (Hard rule 9 from Phase B).** When a task first imports an external module, pin it explicitly: `go get <module>@<version>`, then `go mod tidy`. Add the two `replace` directives BEFORE the first `tidy` that pulls the chain graph. **Verified pins (proxy- and build-checked by the Phase C spike):**
   - `replace github.com/cometbft/cometbft => github.com/pokt-network/cometbft v0.38.17-0.20250808222235-91d271231811` (the fork; in sumdb, no `GONOSUMDB` needed)
   - `replace github.com/syndtr/goleveldb => github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7`
   - `go get`: `github.com/cosmos/gogoproto@v1.7.0`, `github.com/cosmos/cosmos-sdk@v0.53.0`, `cosmossdk.io/api@v0.9.2`, `github.com/cometbft/cometbft@v0.38.17`.
   - **Accept whatever `go mod tidy` resolves** (MVS may bump patch versions, e.g. cosmos-sdk → v0.53.7, gogoproto → v1.7.2; generated import paths and behavior are identical across these patches). Do NOT hand-pin to fight MVS — **with ONE verified exception**: pin `github.com/bytedance/sonic@v1.15.2` (Task 2) before `tidy`. MVS otherwise lands on sonic v1.13.2, which fails to compile on Go 1.26 (`undefined: GoMapIterator`); only v1.15.x is Go-1.26-compatible, and `tidy` keeps the explicit pin. Record the resolved versions in the commit. The dep footprint grows large (cosmos-sdk/types is heavy) — this is inherent to decoding cosmos protos and is expected.
   - **DO NOT add** `cosmossdk.io/store` (that is for `block-{H}-data` / StoreKVPair decoding — Phase D/E, not block-header), and **DO NOT add** testify/uuid/goldie/otel directly (not imported by Phase C's paths).
7. **buf and protoc-gen-gocosmos are dev tools, NOT go.mod dependencies.** Install them via `go install <pkg>@<version>` into `GOPATH/bin` (the `make tools-proto` target). This keeps the module graph clean (buf has a huge dep tree).
8. **Tests use the stdlib `testing` package only** (no testify), consistent with Phase B. Golden assertions are exact-value comparisons against the real fixture.
9. **`make ci` (= `vet fmt-check lint test`) must be green at the end of every task.** It is container-free and tool-free (does NOT run buf). Also keep the integration build lint-clean: `golangci-lint run --build-tags=integration ./...` → 0 issues (verified in Task 6). `make gen-check`/`make gen-proto` require buf and are NOT part of `make ci`.
10. **DRY.** The block-header decode is version-invariant — it exists exactly once (`internal/decoders/blockheader.go`); every version delegates. The FilePlugin framing read (`readDelimited`) exists exactly once.

---

## File structure (what each new file owns)

| File | Responsibility | Task |
|---|---|---|
| `third_party/proto/wkt/{gogoproto,cosmos_proto,google/api,google/protobuf}/*.proto` | Vendored well-known-type protos so codegen resolves all imports offline | 1 |
| `buf.yaml` (root, REPLACED) | buf v2 workspace: the three local proto module roots | 1 |
| `buf.gen.poktroll-v0_1_30.yaml` (root) | Per-version gen template: gocosmos plugin + managed-mode `go_package` remap for 9 modules | 1 |
| `Makefile` (MODIFIED) | `tools-proto`, `gen-proto`, `gen-check` targets | 1 |
| `internal/decoders/v0_1_30/gen/**` | buf-generated poktroll bindings (committed, read-only) | 2 |
| `internal/types/block.go` | Canonical `BlockHeader` struct (maps to the `block` table) | 3 |
| `internal/decoders/decoder.go` | The shared `Decoder` interface (minimal: `Version`, `DecodeBlockHeader`) | 3 |
| `internal/decoders/blockheader.go` | Shared, version-invariant `DecodeBlockHeader` + `readDelimited` framing | 3 |
| `internal/decoders/testdata/block-190974-meta` | Real golden fixture (copied from archeology) | 3 |
| `internal/decoders/blockheader_test.go` | 100%-coverage golden + synthetic + error tests for the shared decoder | 3 |
| `internal/decoders/v0_1_30/decoder.go` | The v0.1.30 adapter: `Decoder` struct, `Version()`, `DecodeBlockHeader` (delegates) | 4 |
| `internal/decoders/v0_1_30/decoder_test.go` | Interface-satisfaction + version + delegation tests | 4 |
| `scripts/scaffold_decoder.sh` | Emits a new version's `decoder.go` skeleton (non-destructive) | 5 |
| `.claude/skills/add-decoder-version/SKILL.md` (MODIFIED) | The verified, repeatable codegen+scaffold recipe | 5 |

---

## Task 1: buf toolchain + vendored well-known-type protos + buf workspace config

Sets up the **offline** codegen pipeline: install pinned buf + the gogo plugin, vendor the 3 well-known-type proto trees the poktroll protos import (these are NOT in the existing vendored trees), replace the broken root `buf.yaml` with a working 3-module workspace, add the per-version gen template, and wire `make` targets. No Go code and no `go.mod` changes in this task — `go mod download` only populates the cache.

**Files:**
- Create: `third_party/proto/wkt/gogoproto/gogo.proto`, `third_party/proto/wkt/cosmos_proto/cosmos.proto`, `third_party/proto/wkt/google/api/annotations.proto`, `third_party/proto/wkt/google/api/http.proto`, `third_party/proto/wkt/google/protobuf/{timestamp,any,duration,descriptor}.proto`
- Create: `buf.gen.poktroll-v0_1_30.yaml`
- Modify (replace contents): `buf.yaml`
- Modify: `Makefile`

- [ ] **Step 1: Install pinned codegen tools**

These are dev tools, not module deps. They go into `GOPATH/bin`.

Run:
```bash
go install github.com/bufbuild/buf/cmd/buf@v1.70.0
go install github.com/cosmos/gogoproto/protoc-gen-gocosmos@v1.7.0
"$(go env GOPATH)/bin/buf" --version
```
Expected: `buf --version` prints `1.70.0`. (`protoc-gen-gocosmos` is now on `PATH` for buf's `local:` plugin lookup.)

- [ ] **Step 2: Vendor the well-known-type protos into `third_party/proto/wkt/` (offline-ready)**

The poktroll protos import `gogoproto/gogo.proto`, `cosmos_proto/cosmos.proto`, `google/api/annotations.proto`, and `google/protobuf/*` — none are in the existing vendored trees. Copy them from the module cache so codegen needs zero network. `go mod download <module>@<version>` populates the cache WITHOUT touching `go.mod`.

Run:
```bash
go mod download github.com/cosmos/gogoproto@v1.7.0
go mod download github.com/cosmos/cosmos-proto@v1.0.0-beta.5
go mod download github.com/grpc-ecosystem/grpc-gateway@v1.16.0

CACHE="$(go env GOMODCACHE)"
mkdir -p third_party/proto/wkt/gogoproto \
         third_party/proto/wkt/cosmos_proto \
         third_party/proto/wkt/google/api \
         third_party/proto/wkt/google/protobuf

cp "$CACHE/github.com/cosmos/gogoproto@v1.7.0/gogoproto/gogo.proto" \
   third_party/proto/wkt/gogoproto/gogo.proto
cp "$CACHE/github.com/cosmos/cosmos-proto@v1.0.0-beta.5/proto/cosmos_proto/cosmos.proto" \
   third_party/proto/wkt/cosmos_proto/cosmos.proto
cp "$CACHE/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis/google/api/annotations.proto" \
   third_party/proto/wkt/google/api/annotations.proto
cp "$CACHE/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis/google/api/http.proto" \
   third_party/proto/wkt/google/api/http.proto
for f in timestamp any duration descriptor; do
  cp "$CACHE/github.com/cosmos/gogoproto@v1.7.0/protobuf/google/protobuf/$f.proto" \
     "third_party/proto/wkt/google/protobuf/$f.proto"
done

chmod -R u+w third_party/proto/wkt/
```
Expected: all `cp` succeed (the module cache is read-only; `chmod -R u+w` makes the copies writable/committable).

- [ ] **Step 3: Verify the vendored set is complete**

Run:
```bash
find third_party/proto/wkt -name '*.proto' | sort
```
Expected (exactly these 8 files):
```
third_party/proto/wkt/cosmos_proto/cosmos.proto
third_party/proto/wkt/gogoproto/gogo.proto
third_party/proto/wkt/google/api/annotations.proto
third_party/proto/wkt/google/api/http.proto
third_party/proto/wkt/google/protobuf/any.proto
third_party/proto/wkt/google/protobuf/descriptor.proto
third_party/proto/wkt/google/protobuf/duration.proto
third_party/proto/wkt/google/protobuf/timestamp.proto
```

- [ ] **Step 4: Replace the root `buf.yaml`; remove the stale `buf.gen.yaml`**

The existing `buf.yaml` is broken (lists a non-existent `internal/proto` module and a wrong `cosmos-sdk/v0.53` path). Replace its entire contents with the spike-verified 3-module workspace. (Future versions add their module path here when onboarded — see `add-decoder-version`.)

Also remove the stale root `buf.gen.yaml` — it targets the non-existent `internal/proto/gen` via a **remote** BSR plugin (`buf.build/protocolbuffers/go`), which contradicts the offline guarantee and would be the default template for a bare `buf generate`. `make gen-proto` always passes `--template buf.gen.poktroll-v0_1_30.yaml`, so nothing depends on it. Phase D recreates an offline envelope gen config when it introduces `internal/proto`.

```bash
git rm buf.gen.yaml
```

`buf.yaml`:
```yaml
# buf v2 workspace. Each vendored proto tree is a module root. Per-version
# managed-mode go_package rewriting + plugin invocation live in the gen
# templates (buf.gen.poktroll-<v>.yaml). Codegen is fully offline: every import
# resolves from these three local roots (no buf.build / BSR access).
#
# New poktroll versions add their module path here when onboarded
# (see .claude/skills/add-decoder-version/SKILL.md).
version: v2
modules:
  - path: third_party/proto/poktroll/v0_1_30
  - path: third_party/proto/cosmos-sdk/v0_53_0
  - path: third_party/proto/wkt
lint:
  use:
    - STANDARD
breaking:
  use:
    - WIRE
```

- [ ] **Step 5: Add the per-version gen template**

`buf.gen.poktroll-v0_1_30.yaml`:
```yaml
# buf generate template for poktroll v0_1_30 -> internal/decoders/v0_1_30/gen/
#
# Uses the locally-installed protoc-gen-gocosmos (gogo). Managed mode REWRITES
# the go_package of every poktroll file to live under our module so that many
# poktroll versions can coexist without import-path collisions. Managed mode is
# DISABLED for the cosmos-sdk + wkt trees so their imports keep their upstream
# go_package (github.com/cosmos/cosmos-sdk/types, .../gogoproto, etc.), which
# resolve to the real Go modules we already depend on.
version: v2

plugins:
  - local: protoc-gen-gocosmos
    out: internal/decoders/v0_1_30/gen
    opt:
      - plugins=grpc
      - paths=source_relative
    # Only emit poktroll-owned files; cosmos/google/tendermint imports come from
    # their real Go modules.
    include_imports: false
    include_wkt: false

managed:
  enabled: true
  disable:
    # Keep upstream go_package for everything that is NOT poktroll. Their
    # go_package already points at the real Go modules we depend on, so managed
    # mode must NOT rewrite them (or the poktroll imports would break).
    - path: cosmos
    - path: tendermint
    - path: amino
    - path: gogoproto
    - path: cosmos_proto
    - path: google
  override:
    # Each poktroll module's protos -> internal/decoders/v0_1_30/gen/<module>
    - file_option: go_package
      path: pocket/application
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/application
    - file_option: go_package
      path: pocket/gateway
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/gateway
    - file_option: go_package
      path: pocket/migration
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/migration
    - file_option: go_package
      path: pocket/proof
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/proof
    - file_option: go_package
      path: pocket/service
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/service
    - file_option: go_package
      path: pocket/session
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/session
    - file_option: go_package
      path: pocket/shared
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/shared
    - file_option: go_package
      path: pocket/supplier
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/supplier
    - file_option: go_package
      path: pocket/tokenomics
      value: github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30/gen/pocket/tokenomics
```

- [ ] **Step 6: Add the Makefile targets**

Append `tools-proto gen-proto gen-check` as a new continued line in the existing multi-line `.PHONY` block (currently lines 7–13, ending in `clean`), preserving the backslash chain (i.e. add a `\` to the current last entry and put the three new names on the following line). Then add a new section (place after the "Schema migrations" section):

```makefile
# ─── Proto codegen (buf) ───────────────────────────────────────────────────

BUF_VERSION       := v1.70.0
GOGOPROTO_VERSION := v1.7.0
PROTO_BIN         := $(shell go env GOPATH)/bin

tools-proto: ## Install pinned buf + protoc-gen-gocosmos into GOPATH/bin
	@go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	@go install github.com/cosmos/gogoproto/protoc-gen-gocosmos@$(GOGOPROTO_VERSION)
	@echo "Installed buf $(BUF_VERSION) + protoc-gen-gocosmos $(GOGOPROTO_VERSION) into $(PROTO_BIN)"

gen-proto: tools-proto ## Generate Go decoder bindings for v0_1_30 from vendored protos (offline)
	@PATH="$(PROTO_BIN):$$PATH" buf generate \
	  --template buf.gen.poktroll-v0_1_30.yaml \
	  third_party/proto/poktroll/v0_1_30
	@echo "Generated internal/decoders/v0_1_30/gen/"

gen-check: ## Verify committed generated code matches the protos (regenerate + diff)
	@$(MAKE) gen-proto >/dev/null
	@if ! git diff --quiet -- internal/decoders/v0_1_30/gen; then \
	  echo "generated code is stale; run 'make gen-proto' and commit the result:"; \
	  git --no-pager diff --stat -- internal/decoders/v0_1_30/gen; \
	  exit 1; \
	fi
	@echo "generated code up to date."
```

- [ ] **Step 7: Verify the workspace resolves all imports offline**

`buf build` validates that every import in every module resolves — the proof the WKT vendoring + workspace are correct, with zero network.

Run:
```bash
GOPROXY=off PATH="$(go env GOPATH)/bin:$PATH" buf build --config buf.yaml -o /dev/null && echo "buf build OK (offline)"
```
Expected: `buf build OK (offline)` (exit 0). If buf reports an unresolved import, a WKT proto is missing from Step 2 — add it and retry. (Harmless gogo lint-style warnings about `nullable=false` on repeated native types may appear; they are not errors.)

- [ ] **Step 8: Commit**

```bash
git add third_party/proto/wkt buf.yaml buf.gen.poktroll-v0_1_30.yaml Makefile
git commit -m "build(proto): offline buf workspace + WKT vendoring + gen targets"
```

> Note: `go.mod`/`go.sum` are intentionally unchanged here — `go mod download` only populates the cache; the module deps land in Task 2 when the generated code (which imports them) exists.

---

## Task 2: Generate v0.1.30 bindings and make them compile (the core derisk)

Run the codegen, add exactly the deps the generated code imports, and prove `internal/decoders/v0_1_30/gen/` compiles and vets clean. This is the spec's central de-risking deliverable. The generated code is committed (ADR-008) and consumed by Phase E+; nothing in Phase C imports it except its own compilation.

**Files:**
- Create (generated, committed): `internal/decoders/v0_1_30/gen/pocket/**/*.go` (exactly 63 files, deterministic with pinned buf + gocosmos)
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Generate the bindings**

Run:
```bash
make gen-proto
find internal/decoders/v0_1_30/gen -name '*.go' | wc -l
```
Expected: generation succeeds; exactly 63 `.go` files produced under `internal/decoders/v0_1_30/gen/pocket/<module>/` (deterministic — re-running yields byte-identical output, which `make gen-check` relies on). (Pass the poktroll dir as a positional input, which `gen-proto` already does — buf rejects `--path` for a workspace module.)

- [ ] **Step 2: Add the `replace` directives, then fetch the deps the generated code imports**

The generated code imports `github.com/cosmos/gogoproto/...`, `github.com/cosmos/cosmos-sdk/types` (for `Coin`), and `cosmossdk.io/api/...` (for `module.proto`). The `replace`s must be present before the first `tidy` that pulls the chain graph (cosmos-sdk transitively requires cometbft + goleveldb).

Run:
```bash
go mod edit -replace github.com/cometbft/cometbft=github.com/pokt-network/cometbft@v0.38.17-0.20250808222235-91d271231811
go mod edit -replace github.com/syndtr/goleveldb=github.com/syndtr/goleveldb@v1.0.1-0.20210819022825-2ae1ddf74ef7
go get github.com/cosmos/gogoproto@v1.7.0
go get github.com/cosmos/cosmos-sdk@v0.53.0
go get cosmossdk.io/api@v0.9.2
# Go 1.26 compat (verified): cosmos-sdk/types -> cosmossdk.io/log pulls
# github.com/bytedance/sonic, and MVS otherwise lands on v1.13.2, which does NOT
# compile on Go 1.26 ("undefined: GoMapIterator"). Pin the first Go-1.26-compatible
# release; `go mod tidy` keeps this explicit pin (it does not revert it).
go get github.com/bytedance/sonic@v1.15.2
go mod tidy
```
Expected: completes without error. `go mod tidy` may bump patch versions via MVS (e.g. cosmos-sdk → v0.53.7, gogoproto → v1.7.2, and it pulls cometbft as an indirect dep through cosmos-sdk — the fork `replace` applies). Accept the result, EXCEPT the sonic pin above which must stay at v1.15.2 (`go mod why github.com/bytedance/sonic` shows the chain `gen/pocket/* → cosmos-sdk/types → cosmossdk.io/log → sonic`).

- [ ] **Step 3: Prove the generated code compiles and vets clean**

Run:
```bash
go build ./internal/decoders/v0_1_30/gen/... && echo "gen build OK"
go vet ./internal/decoders/v0_1_30/gen/... && echo "gen vet OK"
```
Expected: both print OK (exit 0). (This is the spike-proven outcome: 64 files, clean compile, with intra-poktroll imports remapped under our module and cosmos/gogo imports resolving to the real modules.)

- [ ] **Step 3b: Anti-contamination guard — generated code imports OUR module, never poktroll (Hard rule 5)**

The managed-mode `go_package` override must have rewritten every poktroll import to our module path. If any `gen/` file still imports `github.com/pokt-network/poktroll`, the override is broken and `go mod tidy` would drag the entire poktroll app graph (and the archeology-style deps) into the main module.

Run (check ACTUAL imports via `go list`, NOT a text grep — generated `.pb.go` files contain `// See: https://github.com/pokt-network/poktroll/...` comment URLs that a plain `grep` would false-positive on):
```bash
if go list -f '{{range .Imports}}{{println .}}{{end}}' ./internal/decoders/v0_1_30/gen/... | grep -q 'github.com/pokt-network/poktroll'; then \
  echo "CONTAMINATED: gen/ imports poktroll — the go_package override in buf.gen.poktroll-v0_1_30.yaml is wrong; STOP"; exit 1; \
else echo "OK: gen/ imports our module path, not poktroll"; fi
go list -m all | grep -i 'github.com/pokt-network/poktroll' && { echo "CONTAMINATED: main module graph contains poktroll; STOP"; exit 1; } || echo "OK: main module graph has no poktroll"
```
Expected: `OK: gen/ imports our module path, not poktroll` and `OK: main module graph has no poktroll`. If either fails, STOP and fix the override before proceeding — do not commit a contaminated module.

- [ ] **Step 4: Confirm `make ci` stays green**

`gen/` is excluded from golangci-lint (`.golangci.yml` line 73: `internal/decoders/.*/gen/`), so lint is unaffected; `go build`/`go vet`/`go test ./...` include `gen/` and must pass.

Run:
```bash
make ci
```
Expected: PASS (vet, fmt-check, lint, test all green).

- [ ] **Step 5: Commit**

```bash
git add internal/decoders/v0_1_30/gen go.mod go.sum
git commit -m "feat(decoders): buf-generate compilable v0_1_30 poktroll bindings"
```

> Note: the generated `gen/` files are committed and read-only per ADR-008. They are the foundation Phase E's entity/tx/event decoders consume; Phase C only proves they compile.

---

## Task 3: Canonical `types.BlockHeader`, the `Decoder` interface, and the shared version-invariant block-header decoder (TDD)

Defines the canonical block-header type, the minimal shared `Decoder` interface (per the design decision: grow additively), and the *single* version-invariant block-header decoder + FilePlugin framing read — driven by a golden test against the real `block-190974` fixture.

**Files:**
- Create: `internal/types/block.go`
- Create: `internal/decoders/decoder.go`
- Create: `internal/decoders/blockheader.go`
- Create: `internal/decoders/testdata/block-190974-meta` (copied fixture)
- Create: `internal/decoders/blockheader_test.go`

- [ ] **Step 1: Add the cometbft dependency (first import is the shared decoder)**

The fork `replace` was added in Task 2. Now pin the direct require.

Run:
```bash
go get github.com/cometbft/cometbft@v0.38.17
```
Expected: resolves to the pokt-network fork via the `replace` (no error).

- [ ] **Step 2: Copy the real golden fixture**

Run:
```bash
mkdir -p internal/decoders/testdata
cp archeology/samples/block-190974-meta internal/decoders/testdata/block-190974-meta
ls -la internal/decoders/testdata/block-190974-meta
```
Expected: a 2864-byte file. (Byte fixture only; `archeology/`'s module/deps are untouched.)

- [ ] **Step 3: Define the canonical `BlockHeader` type**

`internal/types/block.go` (NO package comment — `internal/types/doc.go` already has it):
```go
package types

import "time"

// BlockHeader is the canonical consensus-header projection written to the
// `block` table (one row per height). Height and Time are the queryable axis
// mandated by invariant #1 (chain consensus header, never indexer wall-clock).
// Hash and ProposerAddress are hex-encoded lowercase to match the table's TEXT
// columns. The block header is version-invariant across poktroll releases, so it
// carries no proto_version; chain_id is injected from network config (it is not
// present in the per-block ABCI header, and the `block` table has no chain_id
// column).
type BlockHeader struct {
	Height          int64     // block.height (BIGINT PRIMARY KEY)
	Time            time.Time // block.time (TIMESTAMPTZ) — consensus header time
	Hash            string    // block.hash (TEXT, hex lowercase)
	ProposerAddress string    // block.proposer_address (TEXT, hex lowercase; 20-byte consensus addr)
	TxCount         int       // block.tx_count (INTEGER) = len(RequestFinalizeBlock.Txs)
}
```

- [ ] **Step 4: Define the minimal `Decoder` interface**

`internal/decoders/decoder.go` (NO package comment — `internal/decoders/doc.go` already has it):
```go
package decoders

import "github.com/pokt-network/pocketscribe/internal/types"

// Decoder is implemented by every internal/decoders/v{X}_{Y}_{Z} package and is
// the contract the router dispatches on per block height (ADR-008). The interface
// grows ADDITIVELY: each phase adds a method alongside its implementation. Slice 1
// Phase C commits only the two version-agnostic essentials; Phase D+ add
// DecodeTx / DecodeStateEntity / DecodeEvent when those categories are built.
type Decoder interface {
	// Version returns the canonical decoder version tag, e.g. "v0_1_30".
	Version() string
	// DecodeBlockHeader parses a FilePlugin `block-{H}-meta` payload into the
	// canonical BlockHeader. The header is version-invariant, so every version
	// delegates to the shared DecodeBlockHeader function in this package.
	DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)
}
```

- [ ] **Step 5: Write the failing test for the shared decoder**

`internal/decoders/blockheader_test.go` (`package decoders`, stdlib `testing`):
```go
package decoders

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
)

// frameDelimited marshals a gogo proto message with the SAME framing the
// FilePlugin / DecodeBlockHeader expects: a base-128 uvarint length prefix
// followed by the gogo Marshal() output. It uses the cometbft type's own Marshal
// (gogo), NOT google.golang.org/protobuf.
func frameDelimited(t *testing.T, req *abci.RequestFinalizeBlock) []byte {
	t.Helper()
	body, err := req.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(len(body)))
	out := make([]byte, 0, n+len(body))
	out = append(out, prefix[:n]...)
	out = append(out, body...)
	return out
}

// TestDecodeBlockHeaderRealSample is the golden test: it decodes real chain bytes
// (FilePlugin block-190974-meta) and asserts the exact projected values.
func TestDecodeBlockHeaderRealSample(t *testing.T) {
	raw, err := os.ReadFile("testdata/block-190974-meta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	hdr, err := DecodeBlockHeader(raw)
	if err != nil {
		t.Fatalf("DecodeBlockHeader: %v", err)
	}
	if hdr.Height != 190974 {
		t.Fatalf("Height = %d, want 190974", hdr.Height)
	}
	if got := hdr.Time.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"); got != "2025-07-07T19:25:16.918434231Z" {
		t.Fatalf("Time = %q, want 2025-07-07T19:25:16.918434231Z", got)
	}
	const wantHash = "dd01f05916fc208c83bc273556f94f639852fee39967e2f99880c993b2740daa"
	if hdr.Hash != wantHash {
		t.Fatalf("Hash = %q, want %q", hdr.Hash, wantHash)
	}
	const wantProposer = "c067e15f0cb7d2ab48ebc0897d9b41e526700979"
	if hdr.ProposerAddress != wantProposer {
		t.Fatalf("ProposerAddress = %q, want %q", hdr.ProposerAddress, wantProposer)
	}
	if hdr.TxCount != 0 {
		t.Fatalf("TxCount = %d, want 0", hdr.TxCount)
	}
}

// TestDecodeBlockHeaderSyntheticRoundTrip constructs a header, frames it exactly
// as the FilePlugin would, decodes it, and asserts the projection round-trips.
// This guards the gogo-vs-google marshal trap and the hex projection.
func TestDecodeBlockHeaderSyntheticRoundTrip(t *testing.T) {
	// Hash is 32 bytes, ProposerAddress is 20 bytes (consensus sizes).
	want := &abci.RequestFinalizeBlock{
		Height:          42,
		Time:            mustUTC(t, "2026-06-09T12:34:56Z"),
		Hash:            []byte("0123456789abcdef0123456789abcdef"),
		ProposerAddress: []byte("proposeraddr00000000"),
		Txs:             [][]byte{[]byte("tx-a"), []byte("tx-b"), []byte("tx-c")},
	}
	hdr, err := DecodeBlockHeader(frameDelimited(t, want))
	if err != nil {
		t.Fatalf("DecodeBlockHeader: %v", err)
	}
	if hdr.Height != 42 {
		t.Fatalf("Height = %d, want 42", hdr.Height)
	}
	if !hdr.Time.Equal(want.Time) {
		t.Fatalf("Time = %s, want %s", hdr.Time, want.Time)
	}
	wantHash := hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if hdr.Hash != wantHash {
		t.Fatalf("Hash = %q, want %q", hdr.Hash, wantHash)
	}
	if hdr.TxCount != 3 {
		t.Fatalf("TxCount = %d, want 3", hdr.TxCount)
	}
}

// TestReadDelimitedFraming documents the exact framing: a uvarint prefix then
// payload, with trailing bytes (subsequent records) left untouched.
func TestReadDelimitedFraming(t *testing.T) {
	raw, err := os.ReadFile("testdata/block-190974-meta")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	payload, consumed, err := readDelimited(raw)
	if err != nil {
		t.Fatalf("readDelimited: %v", err)
	}
	if len(payload) != 304 {
		t.Fatalf("payload len = %d, want 304", len(payload))
	}
	if consumed != 306 {
		t.Fatalf("consumed = %d, want 306 (2-byte uvarint + 304 payload)", consumed)
	}
	if consumed >= len(raw) {
		t.Fatal("meta file must have additional records after the header")
	}
}

func TestDecodeBlockHeaderEmptyMeta(t *testing.T) {
	if _, err := DecodeBlockHeader(nil); err == nil {
		t.Fatal("expected error for empty meta bytes")
	}
}

func TestDecodeBlockHeaderTruncated(t *testing.T) {
	// uvarint says 10 bytes follow, but only 2 are present.
	if _, err := DecodeBlockHeader([]byte{0x0a, 0x01, 0x02}); err == nil {
		t.Fatal("expected error for truncated record")
	}
}

func TestDecodeBlockHeaderOverflowPrefix(t *testing.T) {
	// 11 continuation bytes overflow a 64-bit uvarint.
	bad := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	if _, err := DecodeBlockHeader(bad); err == nil {
		t.Fatal("expected error for overflowing length prefix")
	}
}

func TestDecodeBlockHeaderRecordTooLarge(t *testing.T) {
	// A uvarint encoding a length above maxRecordSize must be rejected before alloc.
	prefix := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(prefix, uint64(maxRecordSize)+1)
	if _, err := DecodeBlockHeader(prefix[:n]); err == nil {
		t.Fatal("expected error for record exceeding max size")
	}
}

func TestDecodeBlockHeaderBadProto(t *testing.T) {
	// A well-framed record whose payload is not a valid RequestFinalizeBlock.
	// 0x08 = field 1, varint wiretype, with no value -> Unmarshal errors.
	framed := append([]byte{0x01}, 0x08)
	if _, err := DecodeBlockHeader(framed); err == nil {
		t.Fatal("expected error for invalid proto payload")
	}
}

func TestDecodeBlockHeaderNonPositiveHeight(t *testing.T) {
	zero := &abci.RequestFinalizeBlock{Height: 0, Time: mustUTC(t, "2026-01-01T00:00:00Z")}
	if _, err := DecodeBlockHeader(frameDelimited(t, zero)); err == nil {
		t.Fatal("expected error for non-positive height")
	}
}

func mustUTC(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return ts.UTC()
}
```

> Note for the implementer: paste the test verbatim — both `encoding/hex` (clean hash assertion via `hex.EncodeToString`) and `time` (used by `mustUTC`) are imported and used. The struct literal carries no trailing inline comments (to stay gofmt-clean). If your editor reflows anything, run `make fmt` before `make ci`.

- [ ] **Step 6: Run the test; verify it fails**

Run: `go test ./internal/decoders/`
Expected: FAIL — `undefined: DecodeBlockHeader`, `undefined: readDelimited`, `undefined: maxRecordSize`.

- [ ] **Step 7: Implement the shared decoder + framing**

`internal/decoders/blockheader.go` (NO package comment):
```go
package decoders

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	abci "github.com/cometbft/cometbft/abci/types"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// maxRecordSize bounds the first length-delimited record we read from a meta
// file. A real RequestFinalizeBlock is a few KiB; 64 MiB is a generous ceiling
// that still rejects a corrupt/garbage length prefix before allocating.
const maxRecordSize = 64 << 20

// readDelimited reads ONE length-delimited protobuf record from the front of buf
// using the framing the Cosmos SDK FilePlugin writes: a base-128 uvarint length
// prefix (binary.PutUvarint) followed by exactly that many payload bytes. It
// returns the payload (a sub-slice of buf, not copied) and the bytes consumed
// (prefix + payload). The block header is the FIRST record of `block-{H}-meta`.
func readDelimited(buf []byte) (payload []byte, consumed int, err error) {
	length, n := binary.Uvarint(buf)
	if n == 0 {
		return nil, 0, errors.New("decoders: meta record truncated reading length prefix")
	}
	if n < 0 {
		return nil, 0, errors.New("decoders: meta record length prefix overflows 64 bits")
	}
	if length > maxRecordSize {
		return nil, 0, fmt.Errorf("decoders: meta record length %d exceeds max %d", length, maxRecordSize)
	}
	end := n + int(length)
	if end > len(buf) {
		return nil, 0, fmt.Errorf("decoders: meta record truncated: need %d bytes, have %d", end, len(buf))
	}
	return buf[n:end], end, nil
}

// DecodeBlockHeader parses the FIRST length-delimited record of a FilePlugin
// `block-{H}-meta` file as a cometbft abci RequestFinalizeBlock and projects the
// consensus-header fields PocketScribe needs (invariant #1: height + time are the
// queryable axis). The block header is version-invariant across every poktroll
// release — it is cometbft ABCI 2.0, byte-identical for all 32 vendored versions
// — so every versioned decoder delegates here rather than reimplementing it.
//
// The abci import resolves to the pokt-network cometbft fork via the go.mod
// replace directive. Hash and ProposerAddress are hex-encoded (lowercase) to
// match the `block` table TEXT columns.
func DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	payload, _, err := readDelimited(metaBytes)
	if err != nil {
		return nil, err
	}

	var req abci.RequestFinalizeBlock
	// Unmarshal is the gogo-generated method on the cometbft type. Do NOT use
	// google.golang.org/protobuf here: these are gogoproto messages with a
	// stdtime Time field and the two runtimes are not interchangeable.
	if err := req.Unmarshal(payload); err != nil {
		return nil, fmt.Errorf("decoders: decode RequestFinalizeBlock: %w", err)
	}
	if req.Height <= 0 {
		return nil, fmt.Errorf("decoders: decoded non-positive height %d", req.Height)
	}

	return &types.BlockHeader{
		Height:          req.Height,
		Time:            req.Time,
		Hash:            hex.EncodeToString(req.Hash),
		ProposerAddress: hex.EncodeToString(req.ProposerAddress),
		TxCount:         len(req.Txs),
	}, nil
}
```

- [ ] **Step 8: Run the test; verify it passes with 100% coverage**

Run:
```bash
go test -cover ./internal/decoders/
```
Expected: PASS; coverage `100.0% of statements` for the `decoders` package (every branch of `readDelimited` + `DecodeBlockHeader` is exercised). The real-sample test logs/asserts height 190974, time `2025-07-07T19:25:16.918434231Z`, hash `dd01f0…740daa`, proposer `c067e1…0979`.

- [ ] **Step 9: Confirm `make ci` green and commit**

Run: `make fmt && make ci`
Expected: `make fmt` is a no-op (the code above is gofmt-clean as written); `make ci` PASS (vet, fmt-check, lint, test).

```bash
git add internal/types/block.go internal/decoders/decoder.go internal/decoders/blockheader.go \
        internal/decoders/blockheader_test.go internal/decoders/testdata/block-190974-meta go.mod go.sum
git commit -m "feat(decoders): canonical BlockHeader + version-invariant block-header decoder"
```

---

## Task 4: The v0.1.30 decoder adapter (TDD)

Creates the per-version decoder package. In Phase C it implements only the version-invariant block header (by delegating to the shared decoder) and `Version()`; it satisfies the `decoders.Decoder` interface. Its `gen/` subpackage (Task 2) is consumed by later phases' entity decoders.

**Files:**
- Create: `internal/decoders/v0_1_30/decoder.go`
- Create: `internal/decoders/v0_1_30/decoder_test.go`

- [ ] **Step 1: Write the failing test**

`internal/decoders/v0_1_30/decoder_test.go` (`package v0_1_30`, stdlib `testing`):
```go
package v0_1_30

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
)

// Decoder must satisfy the shared decoders.Decoder interface.
var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_30" {
		t.Fatalf("Version() = %q, want v0_1_30", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	// Truncated meta bytes must surface the shared decoder's error through the
	// delegation (proves the method is wired without duplicating the fixture test).
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}
```

- [ ] **Step 2: Run it; verify it fails**

Run: `go test ./internal/decoders/v0_1_30/`
Expected: FAIL — `undefined: Decoder`.

- [ ] **Step 3: Implement the adapter**

`internal/decoders/v0_1_30/decoder.go` (this file CARRIES the package comment — it is the only `.go` in the new `v0_1_30` package besides generated `gen/`):
```go
// Package v0_1_30 is the decoder for poktroll release v0.1.30. The buf-generated
// proto bindings live in the gen/ subpackage (read-only; regenerate via
// `make gen-proto`); this file is the hand-written adapter binding them to the
// canonical types in internal/types. Phase C implements only the version-invariant
// block header (delegated to the shared decoders helper); tx/state/event
// categories arrive in later phases. New versions are NEW packages — this one is
// never repurposed (ADR-008).
package v0_1_30

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.30.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_30" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder. The cometbft ABCI block header is identical across all poktroll
// versions, so there is nothing version-specific to do here.
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
```

- [ ] **Step 4: Run the test; verify it passes with full coverage**

Run:
```bash
go test -cover ./internal/decoders/v0_1_30/
```
Expected: PASS; `100.0% of statements` for the hand-written `decoder.go` (the `gen/` subpackage is a separate package and excluded from this package's coverage; it is generated code and not subject to the 100% decoder-coverage mandate).

- [ ] **Step 5: Confirm `make ci` green and commit**

Run: `make ci`
Expected: PASS.

```bash
git add internal/decoders/v0_1_30/decoder.go internal/decoders/v0_1_30/decoder_test.go
git commit -m "feat(decoders): v0_1_30 adapter implementing the Decoder interface"
```

---

## Task 5: Make the codegen + scaffold pattern repeatable (`add-decoder-version`)

Captures the verified recipe so the next version is onboardable without rediscovery — the "codegen pattern proven repeatable" exit criterion. Rewrites the aspirational steps in `add-decoder-version` with what actually works, and adds a non-destructive scaffold script that emits a new version's `decoder.go` skeleton.

**Files:**
- Create: `scripts/scaffold_decoder.sh`
- Modify: `.claude/skills/add-decoder-version/SKILL.md`

- [ ] **Step 1: Create the scaffold script**

`scripts/scaffold_decoder.sh`:
```bash
#!/usr/bin/env bash
# scaffold_decoder.sh v0_1_31 — emit a new versioned decoder adapter skeleton.
#
# Non-destructive: prints the decoder.go skeleton to stdout. The operator pipes
# it to internal/decoders/<vdir>/decoder.go ONLY if that file does not yet exist
# (ADR-008: a committed decoder version is never overwritten). The skeleton
# implements the current minimal Decoder interface (Version + DecodeBlockHeader,
# which delegates to the shared version-invariant decoder). As later phases add
# interface methods, extend this template alongside them.
set -euo pipefail

VDIR="${1:-}"
if [ -z "$VDIR" ]; then
  echo "usage: $0 v{X}_{Y}_{Z}   (e.g. v0_1_31)" >&2
  exit 2
fi
if ! [[ "$VDIR" =~ ^v[0-9]+_[0-9]+_[0-9]+$ ]]; then
  echo "invalid version dir: $VDIR (expected v{X}_{Y}_{Z}, e.g. v0_1_31)" >&2
  exit 2
fi

cat <<EOF
// Package ${VDIR} is the decoder for poktroll release ${VDIR}. The buf-generated
// proto bindings live in the gen/ subpackage (read-only; regenerate via
// \`make gen-proto\`); this file is the hand-written adapter binding them to the
// canonical types in internal/types. New versions are NEW packages — this one is
// never repurposed (ADR-008).
package ${VDIR}

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll ${VDIR}.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "${VDIR}" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
EOF
```

Make it executable:
```bash
chmod +x scripts/scaffold_decoder.sh
```

- [ ] **Step 2: Verify the scaffold emits a valid, gofmt-clean skeleton**

Run:
```bash
scripts/scaffold_decoder.sh v0_1_31 | gofmt && echo "scaffold gofmt OK"
scripts/scaffold_decoder.sh v0_1_31 | grep -q 'func (Decoder) DecodeBlockHeader' && echo "has DecodeBlockHeader"
scripts/scaffold_decoder.sh badarg 2>&1 | grep -q 'invalid version dir' && echo "rejects bad arg"
```
Expected: `scaffold gofmt OK`, `has DecodeBlockHeader`, `rejects bad arg` (gofmt accepting the output proves it is syntactically valid Go).

- [ ] **Step 3: Rewrite `add-decoder-version` steps 4–7 with the verified recipe**

In `.claude/skills/add-decoder-version/SKILL.md`, replace the existing **Step 4 (Add buf config entry)**, **Step 5 (Generate Go types)**, **Step 6 (Scaffold decoder)**, and **Step 7 (Add unit test)** with the following (keep the surrounding steps 1–3 and 8–11):

```markdown
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
```

- [ ] **Step 4: Prove the codegen is idempotent (repeatable)**

Re-running codegen on an unchanged proto set must produce a byte-identical `gen/` — the concrete proof of repeatability.

Run:
```bash
make gen-check
```
Expected: `generated code up to date.` (regenerates and finds no git diff under `internal/decoders/v0_1_30/gen`).

- [ ] **Step 5: Confirm `make ci` green and commit**

Run: `make ci`
Expected: PASS (the SKILL.md + script changes do not affect the Go build; scaffold_decoder.sh is excluded from lint as a shell script).

```bash
git add scripts/scaffold_decoder.sh .claude/skills/add-decoder-version/SKILL.md
git commit -m "feat(skill): repeatable offline buf codegen + decoder scaffold in add-decoder-version"
```

---

## Task 6: Finalize — full verification + mark Phase C complete

Tidy the module, run the complete gauntlet (including the integration-build lint and coverage), confirm idempotent codegen, and mark the spec.

**Files:**
- Modify: `go.mod`, `go.sum` (only if `tidy` changes them)
- Modify: `docs/superpowers/specs/2026-06-08-slice-1-design.md` (Phase C complete marker)

- [ ] **Step 1: Tidy and confirm the module is clean**

Run:
```bash
go mod tidy
go mod verify
git diff --stat go.mod go.sum
```
Expected: `all modules verified`. `go mod tidy` is a no-op or a minor settle (commit it if it changes anything).

- [ ] **Step 2: Full CI gauntlet**

Run:
```bash
make ci
```
Expected: PASS (vet, fmt-check, lint, test).

- [ ] **Step 3: Integration-build lint clean (Hard rule 9)**

Run:
```bash
golangci-lint run --build-tags=integration ./...
```
Expected: 0 issues.

- [ ] **Step 4: Decoder coverage (CLAUDE.md mandate: 100% on decoders)**

Run:
```bash
go test -cover ./internal/decoders/...
```
Expected: `internal/decoders` and `internal/decoders/v0_1_30` report `100.0% of statements` (the `gen/` subpackage is generated and exempt; it has no `_test.go` and is not counted toward the hand-written-decoder mandate).

- [ ] **Step 5: Idempotent codegen + generated code compiles from clean**

Run:
```bash
make gen-check
go build ./...
```
Expected: `generated code up to date.`; `go build ./...` exit 0 (the whole main module, including `gen/`, builds).

- [ ] **Step 5b: Final anti-contamination regression guard (Hard rule 5)**

Confirm Phase C did not leak `archeology`/`poktroll` into the main module graph, and that the module boundary + workspace are intact.

Run:
```bash
go list -m all | grep -iE 'pokt-network/poktroll|pocketscribe/archeology' && { echo "CONTAMINATED — STOP"; exit 1; } || echo "OK: no poktroll/archeology in main module graph"
test -f go.work && { echo "go.work must NOT exist — STOP"; exit 1; } || echo "OK: no go.work"
go mod verify
```
Expected: `OK: no poktroll/archeology in main module graph`, `OK: no go.work`, `all modules verified`.

- [ ] **Step 6: Mark the spec Phase C complete**

In `docs/superpowers/specs/2026-06-08-slice-1-design.md`, after the `**Phase B complete**:` line at the end of the file, add:

```markdown
**Phase C complete**: branch slice-1/phase-c — offline buf codegen pipeline (buf v1.70.0 + protoc-gen-gocosmos, WKT protos vendored under third_party/proto/wkt, managed-mode go_package remap) generates compilable v0_1_30 poktroll bindings into internal/decoders/v0_1_30/gen/; canonical types.BlockHeader + shared version-invariant block-header decoder (cometbft abci RequestFinalizeBlock, golden-tested against real block-190974 fixture, 100% coverage); v0_1_30 adapter implements the minimal Decoder interface; codegen made repeatable via make gen-proto/gen-check + scaffold_decoder.sh in add-decoder-version. make ci green; integration-build lint clean. 2026-06-09.
```

- [ ] **Step 7: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md go.mod go.sum
git commit -m "docs(spec): mark Slice 1 Phase C complete"
```

---

## Self-review checklist (run before declaring the plan done)

**Spec coverage (§9 Phase C):**
- "Extend the skill to emit a Go decoder adapter scaffold" → Task 5 (in `add-decoder-version`, per the design decision; `scaffold_decoder.sh` + recipe).
- "Run codegen against `third_party/proto/poktroll/v0_1_30/` → `internal/decoders/v0_1_30/`" → Tasks 1–2.
- "Hand-fill the adapter for block-header decoding only" → Tasks 3–4 (shared decoder + v0_1_30 delegation).
- "Unit tests: shape comparison against a captured snapshot of the proto fields" → the block header is a cometbft type (`RequestFinalizeBlock`), NOT a poktroll message, so it is intentionally absent from `docs/research/.shapes/v0_1_30.json`. Spec line 444 narrows the Phase C exit to "unit-level decoder tests for v0.1.30 block header only"; Task 3 satisfies it with a value-level golden test against the real `block-190974` fixture (a stronger check than a field-shape comparison). The poktroll-proto shape snapshots are exercised by the entity decoders in Phase E, not here.
- Exit: `internal/decoders/v0_1_30/` compiles (Task 2 + 4), block-header decode passes a unit test (Task 3), codegen pattern proven repeatable (Task 5) → Task 6 verifies all.

**Invariants:** No `time.Now()` in any decoder; `BlockHeader.Time` is chain time. No new migration / no schema change. `archeology/` deps untouched. ADR-008 honored (new dir, `gen/` read-only). DRY: one block-header decoder, one framing reader.

**Type/name consistency:** `types.BlockHeader{Height,Time,Hash,ProposerAddress,TxCount}` used identically in Tasks 3 (def + shared decoder) and 4 (interface). `decoders.Decoder{Version, DecodeBlockHeader}` defined in Task 3, satisfied in Task 4. `DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)` signature identical across interface, shared func, and adapter.

**No placeholders:** every code/config block is complete and copy-paste-ready; every `Run:`/`Expected:` is concrete.
