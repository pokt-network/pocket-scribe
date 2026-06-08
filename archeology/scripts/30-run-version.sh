#!/usr/bin/env bash
# Run a specific version of pocketd until it reaches a target height, then halt.
#
# Usage: scripts/30-run-version.sh <version> <target-height>
#   e.g. scripts/30-run-version.sh v0.1.0 78620
#
# The orchestrator (scripts/run-chapter.sh) is the typical caller. You can
# also run this directly to advance one version at a time.
#
# Logs go to ./run-<version>.log. PID written to ./node.pid.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

VERSION="${1:?usage: 30-run-version.sh <version> <target-height>}"
TARGET="${2:?usage: 30-run-version.sh <version> <target-height>}"

# Validate
[[ "$TARGET" =~ ^[0-9]+$ ]] || die "target height must be an integer, got: $TARGET"

# ─── Sanity check: are we initialized? ───────────────────────────────────
[[ "$(state_get initialized)" == "yes" ]] \
  || die "node home not bootstrapped. Run scripts/10-bootstrap.sh first."

# ─── Is the binary available? ────────────────────────────────────────────
# Prefer the patched -archeology build (has our in-process FilePlugin streamer).
# Fall back to the stock release if -archeology isn't built — but warn loudly,
# because the stock binary IGNORES [streaming.file] config silently.
if [[ -x "$BINARIES_DIR/$VERSION-archeology/pocketd" ]]; then
  BINARY="$BINARIES_DIR/$VERSION-archeology/pocketd"
  log "using ARCHEOLOGY binary for $VERSION ($BINARY)"
else
  BINARY="$BINARIES_DIR/$VERSION/pocketd"
  if [[ ! -x "$BINARY" ]]; then
    log "binary $VERSION not cached — fetching"
    scripts/20-fetch-binary.sh "$VERSION"
  fi
  warn "$VERSION-archeology not built — falling back to stock release."
  warn "Stock pocketd IGNORES [streaming.file] config; FilePlugin output will be empty."
  warn "Build the archeology binary first: see fork/ + scripts/build-archeology.sh"
fi

# ─── Shim check ──────────────────────────────────────────────────────────
SHIM_REQUIRED="$(version_field "$VERSION" shim_required)"
if [[ "$SHIM_REQUIRED" == "True" ]] || [[ "$SHIM_REQUIRED" == "true" ]]; then
  if [[ ! -x "$BINARIES_DIR/$VERSION-shim/pocketd" ]]; then
    die "version $VERSION requires shim binary (non-deterministic replay window). Run scripts/build-shim.sh $VERSION first."
  fi
  BINARY="$BINARIES_DIR/$VERSION-shim/pocketd"
  log "using SHIM binary for $VERSION ($BINARY)"
fi

# ─── Is anything already running? ────────────────────────────────────────
if node_running; then
  die "a node is already running (pid $(node_pid)). Use scripts/stop-node.sh to halt it first."
fi

# ─── Are we resuming or restarting? ──────────────────────────────────────
CURRENT_HEIGHT="$(local_height 2>/dev/null || echo 0)"
if [[ ! -d "$NODE_HOME/data" ]] || [[ ! -f "$NODE_HOME/data/priv_validator_state.json" ]]; then
  warn "no chain data found; this is a fresh sync from genesis"
fi

# ─── Start ───────────────────────────────────────────────────────────────
LOG_FILE="$EXP_ROOT/run-${VERSION}.log"
log "starting pocketd $VERSION, target height $TARGET, log: $LOG_FILE"

# NO --halt-height. Cosmos SDK v0.50.13 has a bug where halt-height fires
# during FinalizeBlock BEFORE app.finalizeBlockState is initialized, and
# the deferred streaming hook then SEGVs accessing nil.Context().
# Instead we rely on the natural UPGRADE NEEDED panic that fires when the
# OLD binary encounters the upgrade plan of the NEXT version at its
# applied_height. Last committed block = applied_height - 1 = runs_until.
# The orchestrator's death-detector treats "process died && last_committed
# >= target" as success.
#
# For the LAST version (v0.1.33, no next upgrade) we'll use a different
# mechanism — SIGINT from the watch loop when target is reached.
"$BINARY" start --home "$NODE_HOME" > "$LOG_FILE" 2>&1 &

NODE_PID=$!
echo "$NODE_PID" > "$PID_FILE"

trap 'warn "received interrupt; stopping node cleanly"; stop_node; exit 130' INT TERM

ok "node started pid=$NODE_PID. Watching for height $TARGET (poll every ${HEIGHT_POLL_INTERVAL}s)"

# ─── Watch loop ──────────────────────────────────────────────────────────
START_TS="$(date +%s)"
LAST_HEIGHT=0
STALL_TICKS=0

while true; do
  if ! kill -0 "$NODE_PID" 2>/dev/null; then
    # Node exited on its own. With --halt-height set, this is the expected
    # exit at TARGET. Verify the last committed height matches.
    rm -f "$PID_FILE"
    # Strip ANSI color codes before parsing — pocketd writes colored logs to file
    LAST_H="$(sed -E 's/\x1b\[[0-9;]*m//g' "$LOG_FILE" 2>/dev/null | grep "committed state" | tail -1 | sed -E 's/.*height=([0-9]+).*/\1/')"
    LAST_H="${LAST_H:-0}"
    if (( LAST_H >= TARGET )); then
      ok "version $VERSION halted gracefully at height $LAST_H (target $TARGET)"
      state_set "last_completed_version" "$VERSION"
      state_set "last_completed_height"  "$LAST_H"
      exit 0
    else
      err "node died unexpectedly at height $LAST_H (target $TARGET). Last log lines:"
      tail -50 "$LOG_FILE" >&2
      exit 1
    fi
  fi

  H="$(local_height)"
  if [[ "$H" =~ ^[0-9]+$ ]] && (( H > 0 )); then

    if (( H == LAST_HEIGHT )); then
      STALL_TICKS=$((STALL_TICKS + 1))
      if (( STALL_TICKS >= 60 )); then
        warn "no height progress for $((STALL_TICKS * HEIGHT_POLL_INTERVAL))s (stuck at $H). Check peers."
        STALL_TICKS=0
      fi
    else
      STALL_TICKS=0
    fi
    LAST_HEIGHT="$H"

    ELAPSED=$(( $(date +%s) - START_TS ))
    printf '  [+%5ds] height %d / target %d (remaining %d)\n' \
      "$ELAPSED" "$H" "$TARGET" "$((TARGET - H))" >&2
  fi

  # Optional max-hour ceiling
  if (( MAX_HOURS_PER_VERSION > 0 )); then
    ELAPSED=$(( $(date +%s) - START_TS ))
    if (( ELAPSED > MAX_HOURS_PER_VERSION * 3600 )); then
      err "exceeded MAX_HOURS_PER_VERSION=${MAX_HOURS_PER_VERSION} for $VERSION. Halting."
      stop_node
      exit 2
    fi
  fi

  sleep "$HEIGHT_POLL_INTERVAL"
done
