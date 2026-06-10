#!/usr/bin/env bash
# curate_fixtures.sh — stage one poktroll version's archived FilePlugin output
# for fixture curation.
#
# Usage: scripts/curate_fixtures.sh <version-tag>        # e.g. v0.1.13
#
# 1. Finds <ver>-h*-fileplugin.tar.xz in /tmp; downloads from the Hetzner
#    archeology bucket (+sha256 verify) if absent.
# 2. Extracts to /tmp/fixtures-<ver>/ (flattened).
# 3. Prints the per-height activity index (`fixtureextract scan`).
#
# Pick heights per spec §8.1 (boundary / max-activity / quiet), then for each:
#   cp /tmp/fixtures-<ver>/block-<H>-{meta,data} test/fixtures/<era-dir>/
#   go run ./tools/fixtureextract <H> test/fixtures/<era-dir> \
#       > test/fixtures/<era-dir>/block-<H>-expected.json
# Era dirs: see the table in docs/superpowers/plans/2026-06-10-slice-1-phase-f-plan.md
# and test/fixtures/README.md.
set -euo pipefail
VER="${1:?usage: curate_fixtures.sh <version-tag>}"
BUCKET="pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet"

TARBALL=$(ls /tmp/"${VER}"-h*-fileplugin.tar.xz 2>/dev/null | head -1 || true)
if [ -z "$TARBALL" ]; then
  REMOTE=$(rclone lsf "$BUCKET/$VER/" 2>/dev/null | grep -- '-fileplugin\.tar\.xz$' | head -1 || true)
  if [ -z "$REMOTE" ]; then
    echo "ERROR: no fileplugin tarball for $VER in the bucket." >&2
    echo "       (archeology for this version may still be running on multi-1 — retry later.)" >&2
    exit 1
  fi
  echo "downloading $REMOTE …" >&2
  rclone copyto "$BUCKET/$VER/$REMOTE" "/tmp/$REMOTE"
  rclone copyto "$BUCKET/$VER/$REMOTE.sha256" "/tmp/$REMOTE.sha256"
  EXPECTED=$(awk '{print $1}' "/tmp/$REMOTE.sha256")
  ACTUAL=$(sha256sum "/tmp/$REMOTE" | awk '{print $1}')
  [ "$EXPECTED" = "$ACTUAL" ] || { echo "ERROR: sha256 mismatch for $REMOTE" >&2; exit 1; }
  TARBALL="/tmp/$REMOTE"
fi

DEST="/tmp/fixtures-${VER}"
if [ ! -d "$DEST" ] || [ -z "$(ls -A "$DEST" 2>/dev/null)" ]; then
  mkdir -p "$DEST"
  echo "extracting $TARBALL → $DEST …" >&2
  tar -xJf "$TARBALL" -C "$DEST"
  # Tarball layouts vary; flatten any nested block files.
  find "$DEST" -mindepth 2 \( -name 'block-*-meta' -o -name 'block-*-data' \) \
    -exec mv -n {} "$DEST/" \;
fi

go run ./tools/fixtureextract scan "$DEST"
