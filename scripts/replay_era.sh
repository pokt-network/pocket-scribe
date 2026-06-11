#!/usr/bin/env bash
# replay_era.sh — extract one era tarball and feed it to the fileplugin sidecar.
#
# Usage:
#   ./scripts/replay_era.sh <version>        # e.g. v0.1.0
#   ./scripts/replay_era.sh v0.1.0 v0.1.5   # replay multiple eras in sequence
#
# Each era tarball lives at /tmp/<version>-h<H>-fileplugin.tar.xz.
# The script:
#   1. Locates the tarball for the requested version.
#   2. Extracts it to a temp workdir under /tmp/ps-replay/<version>/.
#   3. Runs `go run ./cmd/ps fileplugin --bootstrap` against that dir.
#   4. Waits until the NATS stream pending count reaches 0 (drain complete).
#   5. Removes the workdir (keeps /tmp clean for subsequent eras).
#
# Idempotent: re-running the same era re-publishes to NATS; PocketScribe
# consumers handle duplicate delivery via ON CONFLICT DO NOTHING.
#
# Requires: nats-cli (nats) in PATH for drain detection, or falls back to
#           a fixed sleep if nats-cli is absent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARBALL_DIR="${PS_TARBALL_DIR:-/tmp}"
WORKDIR_BASE="${PS_REPLAY_WORKDIR:-/tmp/ps-replay}"
NATS_URL="${PS_NATS_URL:-nats://localhost:4222}"
MAINNET_CONFIG="${REPO_ROOT}/configs/networks/mainnet.yaml"

# Drain wait config
DRAIN_POLL_INTERVAL=3   # seconds between pending checks
DRAIN_TIMEOUT=300       # seconds before giving up on drain
DRAIN_FALLBACK_SLEEP=15 # sleep when nats-cli absent

log() { echo "[replay_era] $*" >&2; }
die() { echo "[replay_era] ERROR: $*" >&2; exit 1; }

# ── locate tarball for a version ──────────────────────────────────────────────
find_tarball() {
    local version="$1"
    # Strip leading 'v' if present for glob match
    local ver="${version#v}"
    local pattern="${TARBALL_DIR}/v${ver}-h*-fileplugin.tar.xz"
    local match
    match="$(ls "${pattern}" 2>/dev/null | head -1)"
    if [[ -z "$match" ]]; then
        die "No tarball found matching: ${pattern}"
    fi
    echo "$match"
}

# ── wait for NATS stream to drain ─────────────────────────────────────────────
wait_drain() {
    local stream="$1"
    if ! command -v nats &>/dev/null; then
        log "nats-cli not found — sleeping ${DRAIN_FALLBACK_SLEEP}s as drain proxy"
        sleep "${DRAIN_FALLBACK_SLEEP}"
        return 0
    fi

    local elapsed=0
    log "Waiting for ${stream} stream to drain (timeout=${DRAIN_TIMEOUT}s)..."
    while true; do
        # `nats stream info` outputs: Messages: N — grab that line
        local pending
        pending=$(nats stream info "${stream}" --server "${NATS_URL}" 2>/dev/null \
            | awk '/Messages:/{print $2}' | head -1)
        pending="${pending:-unknown}"
        if [[ "$pending" == "0" ]]; then
            log "Stream ${stream} drained (0 pending)"
            return 0
        fi
        log "  pending=${pending} (${elapsed}s elapsed)"
        if (( elapsed >= DRAIN_TIMEOUT )); then
            log "WARN: drain timeout after ${DRAIN_TIMEOUT}s — continuing anyway"
            return 0
        fi
        sleep "${DRAIN_POLL_INTERVAL}"
        (( elapsed += DRAIN_POLL_INTERVAL )) || true
    done
}

# ── replay one era ─────────────────────────────────────────────────────────────
replay_one() {
    local version="$1"
    log "=== Replaying era: ${version} ==="

    local tarball
    tarball="$(find_tarball "${version}")"
    log "Tarball: ${tarball}"

    local workdir="${WORKDIR_BASE}/${version}"
    mkdir -p "${workdir}"

    log "Extracting to ${workdir}..."
    tar -xJf "${tarball}" -C "${workdir}"
    log "Extraction complete."

    log "Running fileplugin --bootstrap..."
    # Run from repo root so `go run` resolves the module correctly
    (
        cd "${REPO_ROOT}"
        go run ./cmd/ps fileplugin \
            --bootstrap \
            --config "${MAINNET_CONFIG}" \
            --input-dir "${workdir}" \
            --nats-url "${NATS_URL}"
    )
    log "fileplugin --bootstrap exited."

    # Wait for consumers to drain the stream before moving to the next era
    wait_drain "POCKETSCRIBE"

    log "Cleaning up ${workdir}..."
    rm -rf "${workdir}"
    log "=== Era ${version} complete ==="
}

# ── main ──────────────────────────────────────────────────────────────────────
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <version> [<version> ...]"
    echo "  Example: $0 v0.1.0"
    echo "  Example: $0 v0.1.0 v0.1.2 v0.1.3"
    echo ""
    echo "Available tarballs in ${TARBALL_DIR}:"
    ls "${TARBALL_DIR}"/v*-fileplugin.tar.xz 2>/dev/null \
        | sed 's|.*/||; s|-fileplugin\.tar\.xz||' || echo "  (none found)"
    exit 1
fi

mkdir -p "${WORKDIR_BASE}"

for version in "$@"; do
    replay_one "${version}"
done

log "All requested eras replayed."
