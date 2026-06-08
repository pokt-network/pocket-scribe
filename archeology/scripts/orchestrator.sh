#!/usr/bin/env bash
# Orchestrator v4 — full coverage v0.1.0..v0.1.33 from genesis.
#   - Reads versions.yaml dynamically.
#   - Bootstraps v0.1.0 datadir from genesis if missing.
#   - Filters pocketd stdout (keeps small log).
#   - Fixes h=0 cleanup bug (require positive height).
#   - Pre-snap local validate + post-upload spot-check (ADR-027).
#   - Idempotent (skip versions already in bucket).
set -u
export PATH="/usr/local/go/bin:$PATH"
cd /mnt/scribe/work
LOG=logs/orchestrator.log
mkdir -p logs tmp
exec >>"$LOG" 2>&1
echo "==================== V4 START $(date -u +%Y-%m-%dT%H:%M:%SZ) ===================="

RCLONE_OPTS="--checksum --s3-upload-concurrency 32 --s3-chunk-size 64M --transfers 4 --stats=15s"
REMOTE_BASE="pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet"
STALL_LIMIT_SEC=180
MAX_RETRIES=${MAX_RETRIES:-999999}       # effectively unlimited; slow versions just keep grinding

# ── tip-mode config ──────────────────────────────────────────────────────
TIP_RPC_URL=${TIP_RPC_URL:-https://sauron-rpc.infra.pocket.network}
TIP_PROXIMITY=${TIP_PROXIMITY:-3}        # local within N blocks of chain tip = caught up
TIP_STABLE_SEC=${TIP_STABLE_SEC:-300}    # hold caught-up state this long before SIGINT
VALIDATOR=/mnt/scribe/work/bin/validate-fileplugin
BINARIES_DIR=/mnt/scribe/work/rebuild/build-out
NODE_HOME=/mnt/scribe/work/datadir/.poktroll
FILEPLUGIN_OUTPUT=/mnt/scribe/work/fileplugin-output

sudo sysctl -p /etc/sysctl.d/99-zzz-pocketscribe-upload.conf >/dev/null 2>&1 || true

# ── helpers ───────────────────────────────────────────────────────────────

load_versions() {
  python3 - <<'PY'
import yaml
doc = yaml.safe_load(open("versions.yaml"))
for v in doc["versions"]:
    tag = v["tag"]
    ru = v.get("runs_until", 0)
    if ru in ("tip", None, ""): ru = 0
    print(f"{tag} {ru}")
PY
}

last_committed_height() {
  # Authoritative source: highest block-{H}-meta in fileplugin-output.
  # Survives log rotation across pocketd restarts.
  local highest
  highest=$(ls "$FILEPLUGIN_OUTPUT"/ 2>/dev/null | grep -oE "^block-[0-9]+-meta\$" | grep -oE "[0-9]+" | sort -n | tail -1)
  if [[ -n "$highest" ]]; then
    echo "$highest"
    return
  fi
  local v=$1
  tail -10000 "run-${v}.log" 2>/dev/null \
    | grep "committed state" | tail -1 | grep -oE 'height=[0-9]+' | cut -d= -f2
}

current_rpc_height() {
  curl -sf --max-time 5 http://127.0.0.1:26657/status 2>/dev/null \
    | jq -r .result.sync_info.latest_block_height 2>/dev/null
}

get_chain_tip() {
  curl -sf --max-time 10 "${TIP_RPC_URL}/status" 2>/dev/null \
    | jq -r .result.sync_info.latest_block_height 2>/dev/null
}


is_node_alive() {
  pgrep -u pnf -f "${BINARIES_DIR}/${1}/pocketd start" >/dev/null
}

start_node() {
  local v=$1 log="run-${1}.log"
  [[ -f "$log" ]] && rm -f "$log"   # filtered log; safe to discard prev
  # Filter pocketd stdout: keep only signal lines so the log stays small.
  ( "$BINARIES_DIR/${v}/pocketd" start --home "$NODE_HOME" 2>&1 \
    | stdbuf -oL grep -E "committed state|ERR |panic|UPGRADE NEEDED|fatal" \
    > "$log" ) &
  disown
  echo "[$(date -u +%H:%M:%S)] started $v pid=$!"
}

kill_node() {
  local v=$1
  pkill -INT -u pnf -f "${BINARIES_DIR}/${v}/pocketd start" 2>/dev/null || true
  sleep 15
  pkill -9 -u pnf -f "${BINARIES_DIR}/${v}/pocketd start" 2>/dev/null || true
  sleep 3
}

# Bug fix: only delete files when h > 0. h=0 was deleting EVERYTHING.
# Perf: fork-free per-file parsing (was ~460k forks for 230k files = 10-30min).
cleanup_partial_fileplugin() {
  local h=$1
  if (( h <= 0 )); then
    echo "[$(date -u +%H:%M:%S)] cleanup_partial_fileplugin: refusing h=$h (would delete all)"
    return 0
  fi
  local f name fh
  while IFS= read -r -d '' f; do
    name=${f##*/}        # strip dir; bash builtin (no fork)
    fh=${name#block-}    # strip "block-" prefix
    fh=${fh%%-*}         # keep only the height part
    [[ -z "$fh" || ! "$fh" =~ ^[0-9]+$ ]] && continue
    (( fh > h )) && rm -f "$f"
  done < <(find "$FILEPLUGIN_OUTPUT/" -mindepth 1 -name 'block-*' -print0 2>/dev/null)
}

bootstrap_if_needed() {
  if [[ -d "$NODE_HOME/data" ]] && [[ -f "$NODE_HOME/config/genesis.json" ]]; then
    echo "[$(date -u +%H:%M:%S)] datadir already initialized at $NODE_HOME"
    return 0
  fi
  echo "[$(date -u +%H:%M:%S)] bootstrap: datadir empty, initializing from genesis"
  # Symlink v0.1.0 binary to the path 10-bootstrap expects
  mkdir -p binaries/v0.1.0
  [[ -L binaries/v0.1.0/pocketd ]] || ln -sf "$BINARIES_DIR/v0.1.0/pocketd" binaries/v0.1.0/pocketd
  bash scripts/10-bootstrap.sh
  # Patch persistent_peers = seeds (HANDOFF lesson #1)
  source .env
  sed -i "s|^persistent_peers = .*|persistent_peers = \"$SEEDS\"|" "$NODE_HOME/config/config.toml"
  echo "[$(date -u +%H:%M:%S)] bootstrap complete"
}

# ── validate / spot-check (ADR-027) ───────────────────────────────────────

validate_local() {
  local v=$1
  echo "[$(date -u +%H:%M:%S)] validate-local $v"
  [[ -x "$VALIDATOR" ]] || { echo "FATAL validator missing"; return 1; }
  if ! "$VALIDATOR" "$FILEPLUGIN_OUTPUT/" > "logs/validate-${v}.log" 2>&1; then
    echo "[$(date -u +%H:%M:%S)] FATAL local validation failed for $v"
    tail -20 "logs/validate-${v}.log"
    return 1
  fi
  echo "[$(date -u +%H:%M:%S)] validate-local $v OK"
}

spot_check() {
  local v=$1 h=$2
  local REMOTE="${REMOTE_BASE}/${v}/" FP="${v}-h${h}-fileplugin.tar.xz"
  local td; td=$(mktemp -d -p /mnt/scribe/work/tmp)
  trap "rm -rf '$td'" RETURN
  rclone copy "${REMOTE}${FP}" "$td/" --checksum > /dev/null 2>&1 \
    || { echo "SUSPECT spot-check download $v"; return 1; }
  mkdir -p "$td/extract"
  tar -I "xz -T0" -xf "$td/$FP" -C "$td/extract" 2>/dev/null \
    || { echo "SUSPECT spot-check decompress $v"; return 1; }
  "$VALIDATOR" "$td/extract" > "logs/spot-${v}.log" 2>&1 \
    || { echo "SUSPECT spot-check validate $v"; tail -20 "logs/spot-${v}.log"; return 1; }
  echo "[$(date -u +%H:%M:%S)] spot-check $v OK"
}


run_tip_version() {
  local v=$1 retries=0
  local caught_up_at=""

  while (( retries < MAX_RETRIES )); do
    if ! is_node_alive "$v"; then
      rm -f "$NODE_HOME/config/addrbook.json"
      local h; h=$(last_committed_height "$v"); h=${h:-0}
      cleanup_partial_fileplugin "$h"
      start_node "$v"
      sleep 30
      retries=$((retries + 1))
    fi

    local last_h; last_h=$(current_rpc_height); last_h=${last_h:-0}
    local stall_start
    stall_start=$(date +%s)

    while is_node_alive "$v"; do
      sleep 30
      local h_now; h_now=$(current_rpc_height); h_now=${h_now:-0}
      local tip; tip=$(get_chain_tip)

      # Caught-up detection
      if [[ -n "$tip" ]] && (( h_now > 0 )) && (( h_now + TIP_PROXIMITY >= tip )); then
        if [[ -z "$caught_up_at" ]]; then
          caught_up_at=$(date +%s)
          echo "[$(date -u +%H:%M:%S)] $v reached tip h=$h_now chain_tip=$tip -- holding ${TIP_STABLE_SEC}s for stability"
        fi
        local held=$(( $(date +%s) - caught_up_at ))
        if (( held >= TIP_STABLE_SEC )); then
          echo "[$(date -u +%H:%M:%S)] $v stable at tip (held ${held}s, h=$h_now) -- SIGINT for snapshot"
          kill_node "$v"
          local fh; fh=$(last_committed_height "$v"); fh=${fh:-0}
          echo "[$(date -u +%H:%M:%S)] $v tip-snapshot at h=$fh"
          return 0
        fi
      else
        if [[ -n "$caught_up_at" ]]; then
          echo "[$(date -u +%H:%M:%S)] $v fell behind tip (h=$h_now tip=$tip) -- resuming sync"
          caught_up_at=""
        fi
      fi

      # Stall handling (same as run_version)
      if (( h_now > last_h )); then
        last_h=$h_now; stall_start=$(date +%s)
      else
        local dur=$(( $(date +%s) - stall_start ))
        if (( dur >= STALL_LIMIT_SEC )); then
          echo "[$(date -u +%H:%M:%S)] $v stalled at h=$h_now for ${dur}s -- kill+retry"
          kill_node "$v"
          break
        fi
      fi
    done
  done

  echo "[$(date -u +%H:%M:%S)] $v exceeded ${MAX_RETRIES} retries in tip-mode"
  return 1
}

# ── run/snap/upload per version ───────────────────────────────────────────

run_version() {
  local v=$1 target=$2 retries=0
  while (( retries < MAX_RETRIES )); do
    if ! is_node_alive "$v"; then
      rm -f "$NODE_HOME/config/addrbook.json"
      local h; h=$(last_committed_height "$v"); h=${h:-0}
      cleanup_partial_fileplugin "$h"
      start_node "$v"
      sleep 30
      retries=$((retries + 1))
    fi

    local last_h; last_h=$(current_rpc_height); last_h=${last_h:-0}
    local stall_start; stall_start=$(date +%s)

    while is_node_alive "$v"; do
      sleep 10
      local h_now; h_now=$(current_rpc_height); h_now=${h_now:-0}
      if (( h_now >= target )); then
        echo "[$(date -u +%H:%M:%S)] $v reached target h=$h_now (waiting for panic-exit)"
        while is_node_alive "$v"; do sleep 10; done
        local fh; fh=$(last_committed_height "$v"); fh=${fh:-0}
        (( fh >= target )) && { echo "[$(date -u +%H:%M:%S)] $v done at h=$fh"; return 0; }
        break
      fi
      if (( h_now > last_h )); then
        last_h=$h_now; stall_start=$(date +%s)
      else
        local dur=$(( $(date +%s) - stall_start ))
        if (( dur >= STALL_LIMIT_SEC )); then
          echo "[$(date -u +%H:%M:%S)] $v stalled at h=$h_now for ${dur}s -- kill+retry"
          kill_node "$v"
          break
        fi
      fi
    done

    local eh; eh=$(last_committed_height "$v"); eh=${eh:-0}
    (( eh >= target )) && { echo "[$(date -u +%H:%M:%S)] $v done at h=$eh"; return 0; }
    if grep -q "wrong Block.Header\|AppHash" "run-${v}.log" 2>/dev/null; then
      echo "[$(date -u +%H:%M:%S)] FATAL AppHash mismatch in $v"; return 1
    fi
  done
  echo "[$(date -u +%H:%M:%S)] $v exceeded $MAX_RETRIES retries"; return 1
}

snap_and_upload() {
  local v=$1 h=$2
  echo "[$(date -u +%H:%M:%S)] snapshot $v h=$h"
  # Use existing 40-snapshot-version.sh but env-override BINARIES_DIR
  BINARIES_DIR="$BINARIES_DIR" scripts/40-snapshot-version.sh "$v" "$h" > "logs/snap-${v}.log" 2>&1 \
    || { echo "FATAL snap $v"; tail -20 "logs/snap-${v}.log"; return 1; }
  echo "[$(date -u +%H:%M:%S)] upload $v"
  local REMOTE="${REMOTE_BASE}/${v}/" N="${v}-h${h}"
  rclone copy "snapshots/${N}-datadir.tar.xz" "$REMOTE" $RCLONE_OPTS > "logs/up-${v}-d.log" 2>&1 || { echo "FATAL up-d"; return 1; }
  rclone copy "snapshots/${N}-datadir.tar.xz.sha256" "$REMOTE" --checksum
  rclone copy "snapshots/${N}-fileplugin.tar.xz" "$REMOTE" $RCLONE_OPTS > "logs/up-${v}-fp.log" 2>&1 || { echo "FATAL up-fp"; return 1; }
  rclone copy "snapshots/${N}-fileplugin.tar.xz.sha256" "$REMOTE" --checksum
  local BX="snapshots/${v}-pocketd-archeology.xz"
  if [[ ! -f "$BX" ]]; then
    xz -9 -T0 -c "$BINARIES_DIR/${v}/pocketd" > "$BX"
    sha256sum "$BX" > "${BX}.sha256"
  fi
  rclone copy "$BX" "$REMOTE" $RCLONE_OPTS > "logs/up-${v}-b.log" 2>&1 || { echo "FATAL up-bin"; return 1; }
  rclone copy "${BX}.sha256" "$REMOTE" --checksum
  echo "[$(date -u +%H:%M:%S)] $v uploaded"
  rm -f "snapshots/${N}-datadir.tar.xz" "snapshots/${N}-datadir.tar.xz.sha256"
  rm -f "snapshots/${N}-fileplugin.tar.xz" "snapshots/${N}-fileplugin.tar.xz.sha256"
}

# ── main ──────────────────────────────────────────────────────────────────

bootstrap_if_needed

while read -r line; do
  set -- $line
  v=$1; target=$2
  echo "[$(date -u +%H:%M:%S)] === BEGIN $v (target=$target) ==="

  # Idempotent skip
  if rclone ls "${REMOTE_BASE}/${v}/" 2>/dev/null | grep -qE "${v}-h[0-9]+-datadir\.tar\.xz$"; then
    echo "[$(date -u +%H:%M:%S)] $v already in bucket — skip"
    continue
  fi

  if [[ "$target" == "0" ]]; then
    echo "[$(date -u +%H:%M:%S)] $v tip mode -- chain-following until stable at tip"
    if ! run_tip_version "$v"; then
      echo "[$(date -u +%H:%M:%S)] FATAL abort at $v (tip-mode)"
      exit 1
    fi
    H=$(last_committed_height "$v"); H=${H:-0}
    if ! validate_local "$v"; then
      echo "[$(date -u +%H:%M:%S)] FATAL pre-snap validation $v"
      exit 1
    fi
    if ! snap_and_upload "$v" "$H"; then
      echo "[$(date -u +%H:%M:%S)] FATAL snap/upload $v"
      exit 1
    fi
    if ! spot_check "$v" "$H"; then
      echo "[$(date -u +%H:%M:%S)] SUSPECT $v -- halting for human review"
      exit 2
    fi
    echo "$v" > .state/last_completed_version
    echo "$H" > .state/last_completed_height
    continue
  fi

  if ! run_version "$v" "$target"; then
    echo "[$(date -u +%H:%M:%S)] FATAL abort at $v"
    exit 1
  fi
  H=$(last_committed_height "$v"); H=${H:-0}

  if ! validate_local "$v"; then
    echo "[$(date -u +%H:%M:%S)] FATAL pre-snap validation $v"
    exit 1
  fi

  if ! snap_and_upload "$v" "$H"; then
    echo "[$(date -u +%H:%M:%S)] FATAL snap/upload $v"
    exit 1
  fi

  if ! spot_check "$v" "$H"; then
    echo "[$(date -u +%H:%M:%S)] SUSPECT $v — halting for human review"
    exit 2
  fi

  echo "$v" > .state/last_completed_version
  echo "$H" > .state/last_completed_height
done < <(load_versions)

echo "==================== END $(date -u +%Y-%m-%dT%H:%M:%SZ) — orchestrator done ===================="
