#!/usr/bin/env bash
# Common helpers sourced by every other script.
# Sourced, never executed directly.

set -euo pipefail

# Resolve experiment root regardless of where the script is invoked from
EXP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export EXP_ROOT

# ─── Logging ──────────────────────────────────────────────────────────────
log()   { printf '\033[1;34m[%s]\033[0m %s\n'   "$(date -u +%H:%M:%S)" "$*" >&2; }
warn()  { printf '\033[1;33m[%s] WARN:\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
err()   { printf '\033[1;31m[%s] ERROR:\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die()   { err "$@"; exit 1; }
ok()    { printf '\033[1;32m[%s] ✓\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# ─── Env loading ──────────────────────────────────────────────────────────
# Loads the experiment-local .env first, then overlays the project-root .env
# if it exists (project root holds secrets like Hetzner creds; experiment
# holds operational config). Project root values win on conflict.
load_env() {
  local exp_env="${EXP_ROOT}/.env"
  local root_env="${EXP_ROOT}/../../.env"
  if [[ ! -f "$exp_env" ]]; then
    die "missing ${exp_env} — copy .env.example to .env and edit."
  fi
  set -a
  # shellcheck disable=SC1090
  source "$exp_env"
  if [[ -f "$root_env" ]]; then
    # shellcheck disable=SC1090
    source "$root_env"
  fi
  set +a
}

# ─── Tool dependency check ────────────────────────────────────────────────
need_tool() {
  local tool="$1"
  command -v "$tool" >/dev/null 2>&1 \
    || die "missing required tool: $tool. Install it and retry."
}

# ─── Sauron query helpers (read-only, no auth) ────────────────────────────
sauron_status() {
  curl -sf --max-time 10 "${SAURON_RPC}/status" | jq -r .result
}

sauron_tip() {
  curl -sf --max-time 10 "${SAURON_RPC}/status" \
    | jq -r .result.sync_info.latest_block_height
}

sauron_block_hash() {
  local h="$1"
  curl -sf --max-time 10 "${SAURON_RPC}/block?height=${h}" \
    | jq -r .result.block_id.hash
}

# ─── Local node helpers ───────────────────────────────────────────────────
local_status() {
  curl -sf --max-time 5 "${LOCAL_RPC}/status" 2>/dev/null \
    | jq -r .result 2>/dev/null
}

local_height() {
  curl -sf --max-time 5 "${LOCAL_RPC}/status" 2>/dev/null \
    | jq -r .result.sync_info.latest_block_height 2>/dev/null \
    || echo 0
}

local_catching_up() {
  curl -sf --max-time 5 "${LOCAL_RPC}/status" 2>/dev/null \
    | jq -r .result.sync_info.catching_up 2>/dev/null \
    || echo true
}

# ─── versions.yaml lookup ─────────────────────────────────────────────────
# Print N lines for a given version: applied_height, runs_until, shim_required, upload_chapter, notes
version_field() {
  local tag="$1" field="$2"
  python3 - "$tag" "$field" <<'PY'
import sys, yaml
tag, field = sys.argv[1], sys.argv[2]
with open("versions.yaml") as f:
    doc = yaml.safe_load(f)
for v in doc["versions"]:
    if v["tag"] == tag:
        val = v.get(field, "")
        print(val)
        sys.exit(0)
print("", end="")
sys.exit(1)
PY
}

list_versions() {
  python3 - <<'PY'
import yaml
with open("versions.yaml") as f:
    doc = yaml.safe_load(f)
for v in doc["versions"]:
    print(v["tag"])
PY
}

# ─── Process management ───────────────────────────────────────────────────
PID_FILE="${EXP_ROOT}/node.pid"

node_pid() {
  [[ -f "$PID_FILE" ]] && cat "$PID_FILE" || echo ""
}

node_running() {
  local pid
  pid="$(node_pid)"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

stop_node() {
  local pid
  pid="$(node_pid)"
  if [[ -z "$pid" ]]; then return 0; fi
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    return 0
  fi
  log "sending SIGINT to node pid=$pid (grace ${HALT_GRACE_SECONDS:-15}s)"
  kill -INT "$pid"
  local deadline=$((SECONDS + ${HALT_GRACE_SECONDS:-15}))
  while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    warn "node did not exit on SIGINT, sending SIGTERM"
    kill -TERM "$pid"
    sleep 5
  fi
  if kill -0 "$pid" 2>/dev/null; then
    warn "node still alive, SIGKILL (data may be inconsistent)"
    kill -KILL "$pid"
  fi
  rm -f "$PID_FILE"
  ok "node stopped"
}

# ─── State tracking ───────────────────────────────────────────────────────
# .state/<key> holds small markers like "last_completed_version=v0.1.5".
STATE_DIR="${EXP_ROOT}/.state"
mkdir -p "$STATE_DIR"

state_get() { [[ -f "$STATE_DIR/$1" ]] && cat "$STATE_DIR/$1" || echo ""; }
state_set() { printf '%s' "$2" > "$STATE_DIR/$1"; }
