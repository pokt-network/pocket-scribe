#!/usr/bin/env bash
# Verify the local genesis.json matches Sauron's authoritative version.
# Run this BEFORE starting the sync to catch genesis-state hash mismatch early.
set -euo pipefail

LOCAL="${1:-./genesis.json}"
SAURON="https://sauron-rpc.infra.pocket.network"

if [ ! -f "$LOCAL" ]; then
  echo "❌ Local genesis not found: $LOCAL"
  exit 1
fi

LOCAL_SHA=$(sha256sum "$LOCAL" | cut -d' ' -f1)
echo "Local genesis sha256:  $LOCAL_SHA"

# Sauron returns base64-encoded chunks; reconstruct
echo "→ Fetching authoritative genesis from Sauron (may take ~30s for large genesis)..."
TMP=$(mktemp)
CHUNK=0
while :; do
  RES=$(curl -sf "$SAURON/genesis_chunked?chunk=$CHUNK")
  TOTAL=$(echo "$RES" | jq -r '.result.total // "0"')
  DATA=$(echo "$RES" | jq -r '.result.data // empty')
  if [ -z "$DATA" ]; then break; fi
  echo "$DATA" | base64 -d >> "$TMP"
  CHUNK=$((CHUNK + 1))
  if [ "$CHUNK" -ge "$TOTAL" ]; then break; fi
done

SAURON_SHA=$(sha256sum "$TMP" | cut -d' ' -f1)
echo "Sauron genesis sha256: $SAURON_SHA"

if [ "$LOCAL_SHA" = "$SAURON_SHA" ]; then
  echo "✓ Genesis matches"
  rm "$TMP"
  exit 0
else
  echo "❌ Genesis MISMATCH — local will not produce same app_hash at height 1"
  echo "   Diff (jq-normalized):"
  diff <(jq -S . "$LOCAL") <(jq -S . "$TMP") | head -50
  rm "$TMP"
  exit 1
fi
