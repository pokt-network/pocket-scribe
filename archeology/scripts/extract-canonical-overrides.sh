#!/usr/bin/env bash
# Extract canonical unstake_session_end_height for MorseClaimableAccounts
# from block_results cache. Output: canonical_overrides.json keyed by
# "morse_node_address|height" → unstake_session_end_height (int64).
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=.state/canonical-overrides/overrides.json
LO=${1:-99293}
HI=${2:-102141}

# Process all blocks in range. For each tx event of type
#   pocket.migration.EventMorseSupplierClaimed
#   pocket.migration.EventMorseApplicationClaimed
# extract:
#   - morse_node_address (or morse_src_address for application)
#   - supplier.unstake_session_end_height (or application.unstake_session_end_height)
# Write as object.

ls .state/block_results/*.json \
  | awk -v lo=$LO -v hi=$HI -F'[/.]' '{h=$(NF-1); if (h>=lo && h<=hi) print $0}' \
  | xargs -P 16 -I@@ jq -r '
      .result.height as $height
      | (.result.txs_results // [])
      | to_entries[]
      | .key as $msg_idx
      | .value.events // []
      | .[]
      | select(
          .type == "pocket.migration.EventMorseSupplierClaimed"
          or .type == "pocket.migration.EventMorseApplicationClaimed"
        )
      | . as $ev
      | (reduce .attributes[] as $a ({}; .[$a.key] = $a.value)) as $kv
      | {
          height: ($height|tonumber),
          msg_idx: $msg_idx,
          ev_type: $ev.type,
          morse_addr: (
            ($kv.morse_node_address // $kv.morse_src_address // "")
            | fromjson? // .
          ),
          unstake_height: (
            (($kv.supplier // $kv.application // "{}") | fromjson?
             | .unstake_session_end_height // "0")
            | tonumber
          )
        }
      | select(.unstake_height > 0)
    ' @@ 2>/dev/null \
  | jq -s 'reduce .[] as $r ({}; .["\($r.morse_addr)|\($r.height)"] = $r.unstake_height)' \
  > "$OUT"

echo "wrote $OUT"
jq 'length as $n | "overrides: \($n)"' "$OUT"
jq 'to_entries | .[0:5]' "$OUT"
