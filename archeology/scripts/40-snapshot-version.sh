#!/usr/bin/env bash
# Snapshot the per-version artifacts after a clean halt:
#   1. Datadir tarball (full chain state up to halt height — boot-anywhere)
#   2. FilePlugin output tarball (THE canonical thing we are preserving)
#   3. sha256 sidecars for both
#   4. Manifest entry appended to MANIFEST.md
#
# Usage: scripts/40-snapshot-version.sh <version> <height>
#
# Both tarballs are written to snapshots/. The fileplugin output dir is
# DRAINED after the tar succeeds so the next version's run starts with a
# clean slate per version. (Datadir is NOT drained — it accumulates.)
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

VERSION="${1:?usage: 40-snapshot-version.sh <version> <height>}"
HEIGHT="${2:?usage: 40-snapshot-version.sh <version> <height>}"

NAME="${VERSION}-h${HEIGHT}"
mkdir -p "$SNAPSHOTS_DIR"

# Refuse to snapshot if a node is still running — open file descriptors
# would yield a corrupt tarball.
if node_running; then
  die "node still running (pid $(node_pid)); refusing to snapshot"
fi

# ─── Compression policy ─────────────────────────────────────────────────
# We use xz -9 -T0 (parallel, max compression) for EVERY artifact. This
# saves ~20% over gzip on binaries and ~30-40% on chain-state tarballs.
# Trade-off: ~7x slower compress, slightly slower decompress. Acceptable
# given that we compress ONCE and store/upload long-term.
COMPRESS="xz -9 -T0"
EXT="tar.xz"

# ─── Datadir tarball ─────────────────────────────────────────────────────
DATADIR_TAR="$SNAPSHOTS_DIR/${NAME}-datadir.${EXT}"
if [[ -f "$DATADIR_TAR" ]]; then
  warn "$DATADIR_TAR already exists — replacing"
  rm -f "$DATADIR_TAR" "$DATADIR_TAR.sha256"
fi
log "tarring + xz-compressing datadir → $DATADIR_TAR (may take 1-5 min)"
# Tar with --sort=name so tarball is deterministic byte-for-byte.
tar --sort=name -cf - -C "$NODE_HOME" . | $COMPRESS > "$DATADIR_TAR"
sha256sum "$DATADIR_TAR" > "${DATADIR_TAR}.sha256"
DD_SIZE="$(du -h "$DATADIR_TAR" | cut -f1)"
DD_SHA="$(cut -d' ' -f1 < "${DATADIR_TAR}.sha256")"
ok "datadir tarball: $DD_SIZE, sha256 $DD_SHA"

# ─── FilePlugin output tarball ───────────────────────────────────────────
FP_TAR="$SNAPSHOTS_DIR/${NAME}-fileplugin.${EXT}"
if [[ -f "$FP_TAR" ]]; then
  warn "$FP_TAR already exists — replacing"
  rm -f "$FP_TAR" "$FP_TAR.sha256"
fi

if [[ ! -d "$FILEPLUGIN_OUTPUT" ]] || [[ -z "$(ls -A "$FILEPLUGIN_OUTPUT" 2>/dev/null || true)" ]]; then
  warn "FilePlugin output directory is empty — node may not have produced any files. Inspect run log."
  FP_SIZE="0"
  FP_SHA="(none)"
  FP_COUNT=0
else
  FP_COUNT="$(find "$FILEPLUGIN_OUTPUT" -type f | wc -l)"
  log "tarring + xz-compressing FilePlugin ($FP_COUNT files) → $FP_TAR"
  tar --sort=name -cf - -C "$FILEPLUGIN_OUTPUT" . | $COMPRESS > "$FP_TAR"
  sha256sum "$FP_TAR" > "${FP_TAR}.sha256"
  FP_SIZE="$(du -h "$FP_TAR" | cut -f1)"
  FP_SHA="$(cut -d' ' -f1 < "${FP_TAR}.sha256")"
  ok "fileplugin tarball: $FP_SIZE, sha256 $FP_SHA, $FP_COUNT files"
fi

# ─── Drain fileplugin-output/ for next version ───────────────────────────
# Keep datadir accumulating; reset fileplugin-output between versions so we
# get one tarball per version covering only that version's heights.
if (( FP_COUNT > 0 )); then
  log "draining $FILEPLUGIN_OUTPUT for next version"
  find "$FILEPLUGIN_OUTPUT" -mindepth 1 -delete
  ok "fileplugin-output cleared"
fi

# ─── Manifest entry ──────────────────────────────────────────────────────
MANIFEST="$EXP_ROOT/MANIFEST.md"
if [[ ! -f "$MANIFEST" ]]; then
  cat > "$MANIFEST" <<'HEAD'
# Sync-from-genesis artifact manifest

| Version | Height range | Datadir tarball | sha256 | Size | FilePlugin tarball | sha256 | Size | Files | Captured (UTC) | S3 path |
|---|---|---|---|---|---|---|---|---|---|---|
HEAD
fi

# Figure out the height range for this version (lower bound = previous version's runs_until+1 or 1)
PREV_HEIGHT="$(state_get prev_chapter_end_height 2>/dev/null || echo 0)"
LOWER=$((PREV_HEIGHT + 1))
if (( LOWER == 1 )); then LOWER=1; fi
RANGE="${LOWER}..${HEIGHT}"

printf '| %s | %s | `%s` | %s | %s | `%s` | %s | %s | %s | %s | _pending_ |\n' \
  "$VERSION" "$RANGE" "$(basename "$DATADIR_TAR")" "$DD_SHA" "$DD_SIZE" \
  "$(basename "$FP_TAR")" "$FP_SHA" "$FP_SIZE" "$FP_COUNT" \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >> "$MANIFEST"

state_set prev_chapter_end_height "$HEIGHT"
ok "manifest entry appended"

# ─── Also keep the human-readable results.md log going ───────────────────
{
  echo ""
  echo "## Snapshot: ${NAME} ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "- version: ${VERSION}"
  echo "- height_reached: ${HEIGHT}"
  echo "- height_range: ${RANGE}"
  echo "- datadir: $(basename "$DATADIR_TAR") ($DD_SIZE, sha256 $DD_SHA)"
  echo "- fileplugin: $(basename "$FP_TAR") ($FP_SIZE, sha256 $FP_SHA, $FP_COUNT files)"
} >> "$EXP_ROOT/results.md"

ok "snapshot $NAME complete. Next: scripts/50-upload-hetzner.sh (DEFERRED until first run reviewed)"
