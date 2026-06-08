#!/usr/bin/env bash
# Build the patched "-archeology" pocketd binary for a specific poktroll version.
#
# What this does:
#   1. Shallow-clone poktroll @ tag <version> into fork/poktroll-<version>/.
#   2. Copy app/streaming_file.go from fork/streaming_file.go (the patch source).
#   3. Apply fork/app-go-archeology.patch to app/app.go (adds the 5-line hook).
#   4. `go build` → binaries/<version>-archeology/pocketd.
#
# Idempotent: skips clone if already present; skips build if binary exists.
# To force rebuild: `rm -rf fork/poktroll-<version> binaries/<version>-archeology`.
#
# Usage:
#   scripts/build-archeology.sh <version>
#   scripts/build-archeology.sh v0.1.2
#
# Special case for v0.1.0: the source tree is already patched and built;
# this script skips it.

set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

VERSION="${1:?usage: build-archeology.sh <version>}"

FORK_DIR="$EXP_ROOT/fork/poktroll-${VERSION}"
BIN_DIR="$BINARIES_DIR/${VERSION}-archeology"
BIN_PATH="$BIN_DIR/pocketd"

if [[ -x "$BIN_PATH" ]]; then
  log "binary $VERSION-archeology already built at $BIN_PATH"
  "$BIN_PATH" version 2>&1 | head -3 || true
  exit 0
fi

# ─── 1. Clone ─────────────────────────────────────────────────────────────
if [[ ! -d "$FORK_DIR" ]]; then
  log "cloning poktroll @ $VERSION into $FORK_DIR"
  git clone --depth 1 --branch "$VERSION" \
    https://github.com/pokt-network/poktroll.git "$FORK_DIR" \
    >/dev/null 2>&1
  ok "cloned $VERSION"
else
  log "fork dir $FORK_DIR already present; reusing"
fi

# ─── 2. Drop in our streamer ──────────────────────────────────────────────
PATCH_SRC="$EXP_ROOT/fork/streaming_file.go"
PATCH_DIFF="$EXP_ROOT/fork/app-go-archeology.patch"
[[ -f "$PATCH_SRC" ]] || die "missing $PATCH_SRC — extract it from fork/poktroll-v0.1.0/app/"
[[ -f "$PATCH_DIFF" ]] || die "missing $PATCH_DIFF — extract it from fork/poktroll-v0.1.0/"

cp "$PATCH_SRC" "$FORK_DIR/app/streaming_file.go"

# ─── 3. Apply the app.go diff ─────────────────────────────────────────────
# Use git apply for cleanliness. If the patch doesn't apply (because app.go
# drifted between versions), fall back to a manual fuzzy patch.
cd "$FORK_DIR"
if git apply --check "$PATCH_DIFF" 2>/dev/null; then
  git apply "$PATCH_DIFF"
  ok "patch applied cleanly to $VERSION"
else
  warn "exact patch did not apply — attempting fuzzy 3-way merge"
  if ! git apply --3way "$PATCH_DIFF" 2>/dev/null; then
    warn "fuzzy patch failed too — falling back to manual sed insertion"
    # Insert the hook line right before the existing RegisterStreamingServices call.
    if grep -q "RegisterInProcessFileStreamer" app/app.go; then
      log "hook already present — skipping"
    elif grep -q "app.RegisterStreamingServices(appOpts, app.kvStoreKeys())" app/app.go; then
      python3 - <<'PY'
import io, pathlib
p = pathlib.Path("app/app.go")
src = p.read_text()
needle = "if err := app.RegisterStreamingServices(appOpts, app.kvStoreKeys()); err != nil {"
hook = (
    "\t// Register in-process file streamer first (PocketScribe archeology).\n"
    "\t// No-op unless `[streaming.file] write_dir = \"...\"` is set in app.toml.\n"
    "\tif err := RegisterInProcessFileStreamer(app.BaseApp, appOpts, app.kvStoreKeys()); err != nil {\n"
    "\t\treturn nil, err\n"
    "\t}\n\n\t"
)
idx = src.index(needle)
src = src[:idx] + hook + src[idx:]
p.write_text(src)
PY
      ok "manual hook inserted into app/app.go"
    else
      die "cannot locate insertion point in app/app.go — the file structure is too different from v0.1.0"
    fi
  fi
fi

# ─── 4. Build ─────────────────────────────────────────────────────────────
# Force the exact Go toolchain declared in go.mod (e.g. 1.24.3) — newer
# toolchains break deps like bytedance/sonic@v1.13.2 with "undefined:
# GoMapIterator" because of runtime internal symbol renames.
GO_REQUIRED="$(grep -E '^go [0-9]' go.mod | head -1 | awk '{print $2}')"
if [[ -n "$GO_REQUIRED" ]]; then
  log "go build $VERSION-archeology with GOTOOLCHAIN=go${GO_REQUIRED} (this may take 30s-2min)"
  GOTOOLCHAIN_OVERRIDE="GOTOOLCHAIN=go${GO_REQUIRED}"
else
  log "go build $VERSION-archeology (no go.mod toolchain hint; using system go)"
  GOTOOLCHAIN_OVERRIDE=""
fi
mkdir -p "$BIN_DIR"
env $GOTOOLCHAIN_OVERRIDE go build -o "$BIN_PATH" ./cmd/pocketd
ok "built $BIN_PATH"
"$BIN_PATH" version 2>&1 | head -3 || true

cd "$EXP_ROOT"
log "next: scripts/build-archeology.sh <next-version>, or run-chapter.sh continues"
