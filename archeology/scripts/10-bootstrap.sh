#!/usr/bin/env bash
# Bootstrap the node home: download canonical genesis, init datadir, write
# config files. Idempotent — safe to run twice; will refuse to clobber an
# existing initialized datadir.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

GENESIS_URL="$(python3 -c "import yaml; print(yaml.safe_load(open('versions.yaml'))['genesis_url'])")"
GENESIS_SHA="$(python3 -c "import yaml; print(yaml.safe_load(open('versions.yaml'))['genesis_sha256'])")"
GENESIS_LOCAL="$EXP_ROOT/genesis.json"

# ─── 1. Genesis ──────────────────────────────────────────────────────────
if [[ ! -f "$GENESIS_LOCAL" ]]; then
  log "downloading mainnet genesis.json from pocket-network-genesis repo"
  curl -sSfL "$GENESIS_URL" -o "$GENESIS_LOCAL"
fi

LOCAL_SHA="$(sha256sum "$GENESIS_LOCAL" | cut -d' ' -f1)"
if [[ "$LOCAL_SHA" != "$GENESIS_SHA" ]]; then
  err "genesis sha256 mismatch."
  err "  expected: $GENESIS_SHA"
  err "  got:      $LOCAL_SHA"
  err "delete $GENESIS_LOCAL and retry, or update versions.yaml if PNF rotated genesis"
  exit 1
fi
ok "genesis.json verified ($GENESIS_SHA)"

# ─── 1b. Cross-check against live chain ──────────────────────────────────
log "cross-checking genesis against Sauron /genesis_chunked"
TMP="$(mktemp)"
CHUNK=0
TOTAL=1
while (( CHUNK < TOTAL )); do
  RES="$(curl -sf "${SAURON_RPC}/genesis_chunked?chunk=$CHUNK")"
  TOTAL="$(echo "$RES" | jq -r '.result.total // "0"')"
  echo "$RES" | jq -r '.result.data // empty' | base64 -d >> "$TMP"
  CHUNK=$((CHUNK + 1))
done
# Compare semantically with jq -S (sorted keys). Note: the repo genesis uses
# the older flat `consensus_params` layout with `app_name`+`app_version`, while
# Sauron's /genesis_chunked returns the newer nested `consensus.params` layout
# (Cosmos SDK serializes the running form, not the original). Both describe
# the same chain — only the SERIALIZED REPRESENTATION differs. We warn rather
# than die because the canonical chain identity is the repo file's sha256.
if diff -q <(jq -S -c . "$GENESIS_LOCAL") <(jq -S -c . "$TMP") >/dev/null 2>&1; then
  ok "Sauron and repo genesis semantically identical (jq -S)"
else
  warn "Sauron and repo genesis differ in serialization (expected):"
  ( diff <(jq -S . "$GENESIS_LOCAL") <(jq -S . "$TMP") 2>/dev/null \
      | head -20 | sed 's/^/    /' >&2 ) || true
  warn "Trusting the repo genesis (canonical sha256 ${GENESIS_SHA:0:12}...)"
fi
rm "$TMP"

# ─── 2. Pick a binary to init with ───────────────────────────────────────
# We use v0.1.0 (genesis binary) for init. The init only touches config/
# files, not state — so any version's binary would produce equivalent
# scaffolding. Using v0.1.0 keeps the chain matching from the start.
INIT_VERSION="v0.1.0"
BINARY="$BINARIES_DIR/$INIT_VERSION/pocketd"
if [[ ! -x "$BINARY" ]]; then
  log "binary $INIT_VERSION not yet present — running scripts/20-fetch-binary.sh first"
  scripts/20-fetch-binary.sh "$INIT_VERSION"
fi

# ─── 3. Init node home ───────────────────────────────────────────────────
if [[ -d "$NODE_HOME" ]] && [[ -f "$NODE_HOME/config/genesis.json" ]]; then
  warn "$NODE_HOME already initialized — skipping init step"
else
  log "initializing $NODE_HOME with $INIT_VERSION"
  mkdir -p "$NODE_HOME"
  "$BINARY" init "$MONIKER" --home "$NODE_HOME" --chain-id "$CHAIN_ID" -o
  ok "node home initialized"
fi

# Place canonical genesis (overwriting the one init created — that one is empty)
cp "$GENESIS_LOCAL" "$NODE_HOME/config/genesis.json"
ok "canonical genesis placed at $NODE_HOME/config/genesis.json"

# ─── 4. Overlay our config templates ─────────────────────────────────────
# We use a simple Python TOML merger so we override only the keys we care
# about, leaving the init-generated defaults intact for everything else.
log "rendering + overlaying configs onto pocketd-init defaults"
mkdir -p "$FILEPLUGIN_OUTPUT"

# Render BOTH templates with envsubst, then merge each onto the init baseline.
for tpl in app.toml config.toml; do
  TMP_RENDERED="$(mktemp)"
  envsubst < "$EXP_ROOT/configs/${tpl}.template" > "$TMP_RENDERED"
  python3 scripts/merge-toml.py \
    "$NODE_HOME/config/${tpl}" \
    "$TMP_RENDERED" \
    "$NODE_HOME/config/${tpl}"
  rm "$TMP_RENDERED"
  ok "rendered ${tpl}"
done

# Sanity: confirm the rendered FilePlugin write_dir is what we expect.
# yq's merge may emit inline-table format, so use yq itself to extract the path.
ACTUAL_WD="$(yq -p toml -o json '.streaming.file.write_dir // ""' "$NODE_HOME/config/app.toml")"
if [[ "$ACTUAL_WD" != "\"$FILEPLUGIN_OUTPUT\"" ]] && [[ "$ACTUAL_WD" != "$FILEPLUGIN_OUTPUT" ]]; then
  die "rendered app.toml has write_dir=$ACTUAL_WD but expected $FILEPLUGIN_OUTPUT"
fi
ok "FilePlugin write_dir → $FILEPLUGIN_OUTPUT"

state_set initialized "yes"
state_set node_home "$NODE_HOME"
ok "bootstrap complete. Next: scripts/run-chapter.sh <from-version> <to-version>"
