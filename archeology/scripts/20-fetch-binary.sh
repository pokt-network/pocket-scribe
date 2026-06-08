#!/usr/bin/env bash
# Download a specific poktroll release binary from GitHub.
# Cached under ./binaries/<version>/pocketd.
#
# Usage: scripts/20-fetch-binary.sh <version>
#   e.g. scripts/20-fetch-binary.sh v0.1.0
#
# Assets are discovered via the GitHub releases API (no URL guessing).
# We prefer linux_amd64; arm64 hosts need to edit ASSET_PATTERN below.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

VERSION="${1:?usage: 20-fetch-binary.sh <version>}"
TARGET_DIR="${BINARIES_DIR}/${VERSION}"
TARGET="${TARGET_DIR}/pocketd"
ASSET_PATTERN="linux_amd64"

if [[ -x "$TARGET" ]]; then
  # Already cached. Print version for sanity check.
  log "binary $VERSION already cached at $TARGET"
  "$TARGET" version 2>&1 | head -3 || true
  exit 0
fi

mkdir -p "$TARGET_DIR"

log "discovering download URL for poktroll $VERSION from GitHub releases API"
RELEASES_JSON="$(curl -sf "https://api.github.com/repos/pokt-network/poktroll/releases/tags/${VERSION}" \
  || die "GitHub release tag $VERSION not found")"

# Find the asset matching ASSET_PATTERN. We accept .tar.gz, .zip, or bare binary.
ASSET_URL="$(echo "$RELEASES_JSON" \
  | jq -r --arg pat "$ASSET_PATTERN" \
       '.assets[] | select(.name | test($pat; "i")) | select(.name | test("\\.(tar\\.gz|tgz|zip)$")) | .browser_download_url' \
  | head -1)"

if [[ -z "$ASSET_URL" ]]; then
  err "no release asset matched pattern '$ASSET_PATTERN' for $VERSION"
  err "available assets:"
  echo "$RELEASES_JSON" | jq -r '.assets[].name' | sed 's/^/  /' >&2
  exit 1
fi

ASSET_NAME="${ASSET_URL##*/}"
log "downloading $ASSET_NAME"
curl -sSfL "$ASSET_URL" -o "${TARGET_DIR}/${ASSET_NAME}"

# Optional: verify checksum file if present in the release
CHECKSUM_URL="$(echo "$RELEASES_JSON" | jq -r '.assets[] | select(.name | test("checksums.txt|SHA256SUMS"; "i")) | .browser_download_url' | head -1)"
if [[ -n "$CHECKSUM_URL" ]]; then
  log "verifying against release checksum file"
  curl -sSfL "$CHECKSUM_URL" -o "${TARGET_DIR}/checksums.txt"
  EXPECTED_SHA="$(grep -E " ?${ASSET_NAME}\$" "${TARGET_DIR}/checksums.txt" | awk '{print $1}' | head -1)"
  if [[ -n "$EXPECTED_SHA" ]]; then
    GOT_SHA="$(sha256sum "${TARGET_DIR}/${ASSET_NAME}" | cut -d' ' -f1)"
    [[ "$EXPECTED_SHA" == "$GOT_SHA" ]] || die "sha256 mismatch on ${ASSET_NAME}: expected $EXPECTED_SHA got $GOT_SHA"
    ok "sha256 verified"
  else
    warn "release has checksums file but no entry for ${ASSET_NAME}"
  fi
else
  warn "release has no checksums.txt — skipping sha256 verification (poktroll didn't ship one)"
fi

# Extract
log "extracting ${ASSET_NAME}"
case "$ASSET_NAME" in
  *.tar.gz|*.tgz) tar -xzf "${TARGET_DIR}/${ASSET_NAME}" -C "$TARGET_DIR" ;;
  *.zip)          unzip -q -o "${TARGET_DIR}/${ASSET_NAME}" -d "$TARGET_DIR" ;;
  *)              die "unsupported asset extension: $ASSET_NAME" ;;
esac

# Find the actual `pocketd` binary inside the extracted tree
ACTUAL="$(find "$TARGET_DIR" -type f -name "pocketd" -executable | head -1)"
if [[ -z "$ACTUAL" ]]; then
  # Some releases ship without exec bit; find by name and chmod
  ACTUAL="$(find "$TARGET_DIR" -type f -name "pocketd" | head -1)"
  [[ -n "$ACTUAL" ]] || die "no pocketd binary found in extracted archive"
  chmod +x "$ACTUAL"
fi

# Normalize: ensure binary is at $TARGET_DIR/pocketd
if [[ "$ACTUAL" != "$TARGET" ]]; then
  mv "$ACTUAL" "$TARGET"
fi

ok "binary $VERSION ready at $TARGET"
"$TARGET" version 2>&1 | head -3 || true
