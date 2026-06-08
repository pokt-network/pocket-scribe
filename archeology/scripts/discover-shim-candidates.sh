#!/usr/bin/env bash
# Discover blocks in [94370, 102141] that exercise the non-deterministic
# code path (MorseClaimableAccount.GetEstimatedUnbondingEndHeight via
# MsgClaimMorseSupplier and any other paths that called time.Until before
# PR #1436). Output: shim-map/candidates.json with the canonical values
# extracted from Sauron's /block_results.
#
# DO NOT RUN this script blindly yet — we'll execute it once we have
# v0.1.14 snapshotted at height 94369 and need to build the shim.
#
# Strategy (to be implemented):
#   1. For each height in [94370, 102141]:
#        curl /block_results?height=H from Sauron
#   2. Parse events; for any event of type EventSupplierUnbondingEnd (or
#      whatever the canonical name is in v0.1.15/16), extract:
#        - tx_index (which tx in the block triggered it)
#        - the estimated_unbonding_end_height field value
#   3. Aggregate into shim-map/candidates.json:
#        {
#          "94370": null,            // no event this height
#          ...
#          "96610": { "tx_index": 0, "value": <canonical_int> },
#          ...
#        }
#   4. Cross-check: this should contain at least h=96610 (per issue #1481).
#      If fewer entries than expected, audit the PR #1436 diff for other
#      code paths we might be missing.
#
# Cost: ~7700 RPC calls to Sauron (one per height). At ~5/s rate this is
# ~25 minutes. We cache results per height under .state/block_results/<H>.json
# so re-runs are cheap.

set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=lib.sh
source scripts/lib.sh
load_env

WINDOW_LOW=94370
WINDOW_HIGH=102141
CACHE_DIR="$EXP_ROOT/.state/block_results"
mkdir -p "$CACHE_DIR" "$EXP_ROOT/shim-map"

# Sentinel implementation. Once Jorge says "go", we flip on the discovery
# loop. Default is "show me the plan, don't burn 7700 requests".
if [[ "${1:-}" != "--yes-discover" ]]; then
  cat <<EOF
Discovery script for the v0.1.15/v0.1.16 shim map.

Window: heights $WINDOW_LOW..$WINDOW_HIGH (${WINDOW_HIGH} - ${WINDOW_LOW} + 1 = $((WINDOW_HIGH - WINDOW_LOW + 1)) blocks)
Target: shim-map/candidates.json

This will issue ~7700 RPC calls to ${SAURON_RPC}. Estimated runtime ~25 min.
Results are cached per-height under .state/block_results/.

Run with --yes-discover when ready.
EOF
  exit 0
fi

log "scanning [$WINDOW_LOW, $WINDOW_HIGH] for shim candidates"
> "$EXP_ROOT/shim-map/candidates.json.tmp"
echo "{" >> "$EXP_ROOT/shim-map/candidates.json.tmp"

FIRST=true
for ((H=WINDOW_LOW; H<=WINDOW_HIGH; H++)); do
  CACHE="$CACHE_DIR/$H.json"
  if [[ ! -f "$CACHE" ]]; then
    curl -sf "${SAURON_RPC}/block_results?height=$H" -o "$CACHE" \
      || { warn "RPC failed for h=$H, retrying once"; sleep 1; curl -sf "${SAURON_RPC}/block_results?height=$H" -o "$CACHE" || { rm -f "$CACHE"; continue; }; }
  fi

  # Look for events that contain "unbonding_end" or similar.
  # NOTE: exact event name TBD. We capture ANY event whose type/attribute
  # references unbonding-end-height for further inspection.
  HITS="$(jq '
    [
      (.result.end_block_events // []),
      (.result.begin_block_events // []),
      ((.result.txs_results // []) | map(.events // []) | add // [])
    ]
    | flatten
    | map(select(.type | test("UnbondingEnd|unbonding_end|MorseClaimable"; "i")))
  ' "$CACHE" 2>/dev/null || echo "[]")"

  if [[ "$HITS" != "[]" && -n "$HITS" ]]; then
    [[ "$FIRST" == "true" ]] || echo "," >> "$EXP_ROOT/shim-map/candidates.json.tmp"
    FIRST=false
    printf '  "%d": %s' "$H" "$HITS" >> "$EXP_ROOT/shim-map/candidates.json.tmp"
    log "h=$H — hit ($(echo "$HITS" | jq 'length') events)"
  fi

  # Progress every 500 blocks
  if (( (H - WINDOW_LOW) % 500 == 0 )); then
    log "progress: h=$H ($((H - WINDOW_LOW)) / $((WINDOW_HIGH - WINDOW_LOW + 1)))"
  fi
done

echo "" >> "$EXP_ROOT/shim-map/candidates.json.tmp"
echo "}" >> "$EXP_ROOT/shim-map/candidates.json.tmp"
mv "$EXP_ROOT/shim-map/candidates.json.tmp" "$EXP_ROOT/shim-map/candidates.json"

ok "discovery complete. Review shim-map/candidates.json before building the shim."
