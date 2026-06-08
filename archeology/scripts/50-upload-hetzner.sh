#!/usr/bin/env bash
# Upload a version's artifacts (binary + datadir + fileplugin tarballs) to
# Hetzner Object Storage.
#
# Layout in the bucket:
#   mainnet/{version}/pocketd-archeology.xz             ← compressed binary
#   mainnet/{version}/pocketd-archeology.xz.sha256
#   mainnet/{version}/h{H}-datadir.tar.xz               ← chain state
#   mainnet/{version}/h{H}-datadir.tar.xz.sha256
#   mainnet/{version}/h{H}-fileplugin.tar.xz            ← canonical FilePlugin
#   mainnet/{version}/h{H}-fileplugin.tar.xz.sha256
#
# Plus:
#   patch/app-go-archeology.patch
#   patch/streaming_file.go
#   patch/README.md                                     ← chain-of-custody
#   mainnet/MANIFEST.md
#   mainnet/manifest.json                               ← machine-readable
#
# Usage:
#   scripts/50-upload-hetzner.sh <version> <height>     # upload one version
#   scripts/50-upload-hetzner.sh --patch                # upload the patch dir
#   scripts/50-upload-hetzner.sh --manifest             # upload MANIFEST.md + manifest.json
#   scripts/50-upload-hetzner.sh --all                  # upload everything in snapshots/
#
# Reads credentials from .env (HETZNER_*, RCLONE_REMOTE). Idempotent:
# rclone --checksum compares sha256 before re-uploading.

set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

# ─── Verify rclone remote is configured ─────────────────────────────────
if ! rclone listremotes 2>/dev/null | grep -q "^${RCLONE_REMOTE}:$"; then
  log "rclone remote '$RCLONE_REMOTE' missing — creating from .env"
  rclone config create "$RCLONE_REMOTE" s3 \
    provider=Hetzner \
    endpoint="https://${HETZNER_ENDPOINT}" \
    region="$HETZNER_REGION" \
    access_key_id="$HETZNER_ACCESS_KEY" \
    secret_access_key="$HETZNER_SECRET_KEY" \
    acl=private --no-output 2>&1 || die "rclone config failed"
fi

# Sanity ping
rclone lsd "$RCLONE_REMOTE:$HETZNER_BUCKET" >/dev/null 2>&1 \
  || die "cannot reach $RCLONE_REMOTE:$HETZNER_BUCKET — check creds + endpoint"

REMOTE_ROOT="$RCLONE_REMOTE:$HETZNER_BUCKET"

# ─── Subcommand: upload the patch dir ───────────────────────────────────
upload_patch() {
  local PATCH_TMP
  PATCH_TMP="$(mktemp -d)"
  trap 'rm -rf "$PATCH_TMP"' RETURN

  log "staging patch files to $PATCH_TMP"
  cp "$EXP_ROOT/fork/app-go-archeology.patch" "$PATCH_TMP/"
  cp "$EXP_ROOT/fork/streaming_file.go" "$PATCH_TMP/"
  cat > "$PATCH_TMP/README.md" <<'EOF'
# PocketScribe Archeology Patch

Two files, applied to every poktroll release tag in `[v0.1.0, v0.1.33]` to
produce a `pocketd-archeology` binary that emits a per-block FilePlugin
output stream during sync.

- `app-go-archeology.patch` — adds a 5-line hook to `app/app.go` calling
  `RegisterInProcessFileStreamer` before the existing
  `RegisterStreamingServices`. Additive; does not change any other behavior.
- `streaming_file.go` — new file at `app/streaming_file.go` implementing
  `storetypes.ABCIListener`. Writes one pair `block-{H}-meta` +
  `block-{H}-data` per block, with messages varint-length-prefixed proto.

## Why this exists

Cosmos SDK v0.50's `[streaming.abci]` config expects an external HashiCorp
go-plugin binary (looked up via `os.Getenv("COSMOS_SDK_<NAME>")`). When the
env var is empty, the binary falls back to `sh -c ""` and panics. poktroll
v0.1.x does not ship a streaming plugin binary, so the stock release
silently drops every block's KV writes.

This patch adds an in-process streamer that:
- Has no external dependency (single Go file, no go-plugin).
- Reads config from `[streaming.file]` in `app.toml`.
- No-op when `streaming.file.write_dir` is empty (zero overhead).

## Reproducing the archeology binary

```bash
git clone --depth 1 --branch v0.1.X https://github.com/pokt-network/poktroll
cd poktroll
cp /path/to/streaming_file.go app/
git apply /path/to/app-go-archeology.patch
GOTOOLCHAIN=go$(grep -E '^go [0-9]' go.mod | awk '{print $2}') go build -o pocketd-archeology ./cmd/pocketd
```

For v0.1.15 / v0.1.16 (the discontinuity-window versions), see the `shim/`
directory in the bucket — those binaries also include hardcoded canonical
historical outcomes for the non-deterministic blocks.

## Verifying our published archive

Every uploaded tarball has a sha256 sidecar (`.sha256` file next to the
`.tar.xz`). To verify a download:

```bash
sha256sum -c file.tar.xz.sha256
```

To re-derive the FilePlugin output from genesis yourself: apply this patch
to every poktroll tag from v0.1.0 to v0.1.33, sync each one to its
`runs_until` height (chain-authoritative, see `versions.yaml`), and compare
sha256 of your `block-{H}-data` and `block-{H}-meta` files against ours.

## License

The patch and `streaming_file.go` are released under the same license as
poktroll itself (MIT). No additional terms.
EOF

  log "uploading patch/ to $REMOTE_ROOT/patch/"
  rclone copy "$PATCH_TMP/" "$REMOTE_ROOT/patch/" \
    --checksum --transfers 4 --stats=0
  ok "patch uploaded"
}

# ─── Subcommand: upload one version ─────────────────────────────────────
upload_version() {
  local VERSION="$1" HEIGHT="$2"
  local NAME="${VERSION}-h${HEIGHT}"

  local DATADIR="$SNAPSHOTS_DIR/${NAME}-datadir.tar.xz"
  local DATASHA="$DATADIR.sha256"
  local FPDIR="$SNAPSHOTS_DIR/${NAME}-fileplugin.tar.xz"
  local FPSHA="$FPDIR.sha256"
  # Prefer the -archeology-shim binary if it exists (v0.1.15/v0.1.16); fall back
  # to the regular -archeology binary. Shim binaries are uploaded to a separate
  # `shim/` namespace in the bucket so it's clear they're NOT for production.
  local BIN BIN_XZ BIN_SHA BIN_REMOTE_DIR IS_SHIM
  if [[ -x "$BINARIES_DIR/${VERSION}-archeology-shim/pocketd" ]]; then
    BIN="$BINARIES_DIR/${VERSION}-archeology-shim/pocketd"
    BIN_XZ="$SNAPSHOTS_DIR/${VERSION}-pocketd-archeology-shim.xz"
    IS_SHIM=true
  else
    BIN="$BINARIES_DIR/${VERSION}-archeology/pocketd"
    BIN_XZ="$SNAPSHOTS_DIR/${VERSION}-pocketd-archeology.xz"
    IS_SHIM=false
  fi
  BIN_SHA="$BIN_XZ.sha256"

  [[ -f "$DATADIR" ]] || die "missing $DATADIR"
  [[ -f "$DATASHA" ]] || die "missing $DATASHA"
  [[ -f "$FPDIR" ]]   || die "missing $FPDIR"
  [[ -f "$FPSHA" ]]   || die "missing $FPSHA"
  [[ -x "$BIN" ]]     || die "missing binary $BIN"

  # Compress + sha256 the binary if not already done
  if [[ ! -f "$BIN_XZ" ]]; then
    log "compressing binary $VERSION-archeology → $BIN_XZ (xz -9 -T0, ~1min)"
    xz -9 -T0 -c "$BIN" > "$BIN_XZ"
    sha256sum "$BIN_XZ" > "$BIN_SHA"
    ok "binary compressed: $(du -h "$BIN_XZ" | cut -f1)"
  fi

  local REMOTE_DIR="$REMOTE_ROOT/mainnet/${VERSION}/"
  log "uploading $VERSION artifacts to $REMOTE_DIR"
  rclone copy "$DATADIR" "$REMOTE_DIR" --checksum --transfers 4 --stats=0
  rclone copy "$DATASHA" "$REMOTE_DIR" --checksum --stats=0
  rclone copy "$FPDIR"   "$REMOTE_DIR" --checksum --transfers 4 --stats=0
  rclone copy "$FPSHA"   "$REMOTE_DIR" --checksum --stats=0
  # Shim binaries go to a separate `shim/` namespace
  local BIN_REMOTE_DIR
  if [[ "$IS_SHIM" == "true" ]]; then
    BIN_REMOTE_DIR="$REMOTE_ROOT/shim/"
  else
    BIN_REMOTE_DIR="$REMOTE_DIR"
  fi
  rclone copy "$BIN_XZ"  "$BIN_REMOTE_DIR" --checksum --transfers 4 --stats=0
  rclone copy "$BIN_SHA" "$BIN_REMOTE_DIR" --checksum --stats=0

  # Update manifest in-place
  local DD_SHA FP_SHA BIN_SHA_VAL DD_SIZE FP_SIZE BIN_SIZE
  DD_SHA="$(cut -d' ' -f1 < "$DATASHA")"
  FP_SHA="$(cut -d' ' -f1 < "$FPSHA")"
  BIN_SHA_VAL="$(cut -d' ' -f1 < "$BIN_SHA")"
  DD_SIZE="$(du -h "$DATADIR" | cut -f1)"
  FP_SIZE="$(du -h "$FPDIR" | cut -f1)"
  BIN_SIZE="$(du -h "$BIN_XZ" | cut -f1)"

  ok "$VERSION uploaded:"
  echo "  datadir:    $REMOTE_DIR$(basename "$DATADIR") ($DD_SIZE, sha256 ${DD_SHA:0:16}...)"
  echo "  fileplugin: $REMOTE_DIR$(basename "$FPDIR") ($FP_SIZE, sha256 ${FP_SHA:0:16}...)"
  echo "  binary:     $REMOTE_DIR$(basename "$BIN_XZ") ($BIN_SIZE, sha256 ${BIN_SHA_VAL:0:16}...)"
}

# ─── Subcommand: upload manifest ────────────────────────────────────────
upload_manifest() {
  [[ -f "$EXP_ROOT/MANIFEST.md" ]] || die "no MANIFEST.md yet"
  log "uploading MANIFEST.md to $REMOTE_ROOT/mainnet/"
  rclone copy "$EXP_ROOT/MANIFEST.md" "$REMOTE_ROOT/mainnet/" --checksum --stats=0
  # TODO: generate manifest.json from MANIFEST.md (or directly from snapshots/ + bucket contents)
  ok "manifest uploaded"
}

# ─── Subcommand: upload everything ──────────────────────────────────────
upload_all() {
  upload_patch
  for tar_file in "$SNAPSHOTS_DIR"/*-datadir.tar.xz; do
    [[ -f "$tar_file" ]] || continue
    local NAME
    NAME="$(basename "$tar_file" -datadir.tar.xz)"   # e.g. v0.1.0-h78620
    local VERSION="${NAME%-h*}"
    local HEIGHT="${NAME##*-h}"
    upload_version "$VERSION" "$HEIGHT"
  done
  upload_manifest
}

# ─── Dispatch ────────────────────────────────────────────────────────────
case "${1:-}" in
  --patch)    upload_patch ;;
  --manifest) upload_manifest ;;
  --all)      upload_all ;;
  v*)         upload_version "$1" "${2:?usage: 50-upload-hetzner.sh <version> <height>}" ;;
  *)
    cat >&2 <<EOF
usage:
  scripts/50-upload-hetzner.sh <version> <height>   # upload one version
  scripts/50-upload-hetzner.sh --patch              # upload patch/ directory
  scripts/50-upload-hetzner.sh --manifest           # upload MANIFEST.md
  scripts/50-upload-hetzner.sh --all                # upload everything
EOF
    exit 64
    ;;
esac
