#!/usr/bin/env bash
# Preflight: verify the host has everything we need before any sync starts.
# Exit non-zero on any missing dependency; print a clean report.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh

log "preflight check starting"

# ─── Required CLI tools ───────────────────────────────────────────────────
for t in curl jq yq tar sha256sum python3 envsubst; do
  need_tool "$t"
done
# unzip is optional: needed only if a poktroll release ships as .zip (none observed so far)
if ! command -v unzip >/dev/null 2>&1; then
  warn "unzip not installed (optional). Required only if a poktroll release asset is .zip rather than .tar.gz. Install with: sudo apt install unzip"
fi
ok "all required tools present"

# ─── Python yaml module (we use it in lib.sh) ─────────────────────────────
python3 -c "import yaml" 2>/dev/null \
  || die "python3 yaml module missing. pip install pyyaml --break-system-packages OR apt install python3-yaml"
ok "python3 + pyyaml present"

# ─── .env present ────────────────────────────────────────────────────────
load_env
ok ".env loaded from ${EXP_ROOT}/.env"

# ─── Sauron reachable ────────────────────────────────────────────────────
TIP="$(sauron_tip)" || die "Sauron RPC unreachable at ${SAURON_RPC}"
[[ "$TIP" =~ ^[0-9]+$ ]] || die "Sauron returned bogus tip: $TIP"
ok "Sauron reachable — current tip: $TIP"

# ─── Sauron persistent peer reachable on P2P port ────────────────────────
PEER_HOST="$(echo "$PERSISTENT_PEERS" | sed -E 's/.*@([^:]+):.*/\1/')"
PEER_PORT="$(echo "$PERSISTENT_PEERS" | sed -E 's/.*:([0-9]+).*/\1/')"
if command -v nc >/dev/null 2>&1; then
  if nc -z -w 5 "$PEER_HOST" "$PEER_PORT" 2>/dev/null; then
    ok "P2P peer reachable: ${PEER_HOST}:${PEER_PORT}"
  else
    warn "P2P peer ${PEER_HOST}:${PEER_PORT} not reachable from this host. Sync may struggle to find peers. Continuing anyway."
  fi
else
  warn "nc not installed; skipping P2P reachability check"
fi

# ─── Disk space check ────────────────────────────────────────────────────
# We need ~500GB for full sync to tip. Less for early versions.
DISK_AVAIL_KB="$(df -kP "$EXP_ROOT" | awk 'NR==2 {print $4}')"
DISK_AVAIL_GB=$((DISK_AVAIL_KB / 1024 / 1024))
if (( DISK_AVAIL_GB < 50 )); then
  die "only ${DISK_AVAIL_GB}GB free at $EXP_ROOT — need >=50GB to start, >=500GB to reach tip"
elif (( DISK_AVAIL_GB < 500 )); then
  warn "${DISK_AVAIL_GB}GB free — enough to start but you'll need to clean up between chapters to reach tip"
else
  ok "${DISK_AVAIL_GB}GB free at $EXP_ROOT"
fi

# ─── Required directories exist ──────────────────────────────────────────
for d in datadir fileplugin-output binaries snapshots .state; do
  [[ -d "$EXP_ROOT/$d" ]] || mkdir -p "$EXP_ROOT/$d"
done
ok "scratch directories ready"

# ─── versions.yaml is sane ───────────────────────────────────────────────
[[ -f versions.yaml ]] || die "versions.yaml missing"
COUNT="$(list_versions | wc -l)"
[[ "$COUNT" -gt 0 ]] || die "versions.yaml has no versions entries"
ok "versions.yaml lists $COUNT versions"

# ─── GitHub API reachability (for binary downloads) ──────────────────────
HTTP="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 https://api.github.com/repos/pokt-network/poktroll)"
[[ "$HTTP" == "200" ]] || die "GitHub API unreachable (HTTP $HTTP) — cannot download release binaries"
ok "GitHub API reachable"

# ─── No node already running on our port ─────────────────────────────────
if node_running; then
  die "a node is already running (pid $(node_pid)). Run scripts/stop-node.sh first."
fi
if (curl -sf --max-time 2 "${LOCAL_RPC}/status" >/dev/null 2>&1); then
  warn "something is listening on ${LOCAL_RPC} but no node.pid found. Investigate before starting."
fi

ok "preflight passed — ready to run scripts/10-bootstrap.sh"
