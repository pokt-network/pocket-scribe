#!/usr/bin/env bash
# /generate-decoder vX.Y.Z — vendor + snapshot + diff. See SKILL.md.
#
# Now vendors BOTH poktroll AND its dependency cosmos-sdk (deduplicated by
# cosmos-sdk version across poktroll releases).
set -euo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
fi
if ! [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid tag: $TAG (expected vX.Y.Z)" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROOT"

VDIR="$(echo "$TAG" | tr '.' '_')"
DEST="third_party/proto/poktroll/$VDIR"
TMPDIR="/tmp/poktroll-clone-$TAG-$$"
SHAPES="docs/research/.shapes"
SCRIPTS="$SCRIPT_DIR/scripts"
TRACKED="$SCRIPT_DIR/tracked-entities.txt"
EVO="docs/research/spine-shape-evolution.md"
DIFF_TMP="/tmp/proto-diff-$TAG-$$.json"

mkdir -p "$SHAPES" "third_party/proto/poktroll" "third_party/proto/cosmos-sdk"

# Step 1: vendor poktroll
echo "[1/5] cloning poktroll $TAG..."
rm -rf "$TMPDIR" "$DEST"
mkdir -p "$DEST"
if ! git clone --quiet --depth=1 --branch "$TAG" \
    https://github.com/pokt-network/poktroll.git "$TMPDIR" 2>&1; then
  echo "FAIL: clone $TAG" >&2
  rm -rf "$TMPDIR"
  exit 3
fi
if [ ! -d "$TMPDIR/proto" ]; then
  echo "FAIL: no proto/ in $TAG" >&2
  rm -rf "$TMPDIR"
  exit 3
fi
cp -r "$TMPDIR/proto/"* "$DEST/"

# Step 2: detect cosmos-sdk version from go.mod
CSDK_VERSION=""
if [ -f "$TMPDIR/go.mod" ]; then
  CSDK_VERSION=$(grep -E "^\s*github.com/cosmos/cosmos-sdk\s+v" "$TMPDIR/go.mod" | awk '{print $2}' | head -1)
fi
if [ -z "$CSDK_VERSION" ]; then
  echo "FAIL: could not detect cosmos-sdk version in $TAG go.mod" >&2
  rm -rf "$TMPDIR"
  exit 3
fi
echo "      cosmos-sdk dependency: $CSDK_VERSION"
rm -rf "$TMPDIR"

# Step 3: vendor cosmos-sdk if not already (dedup)
CSDK_DIR="third_party/proto/cosmos-sdk/$(echo "$CSDK_VERSION" | tr '.' '_')"
if [ ! -d "$CSDK_DIR" ] || [ -z "$(ls -A "$CSDK_DIR" 2>/dev/null)" ]; then
  echo "[2/5] cloning cosmos-sdk $CSDK_VERSION (new)..."
  CSDK_TMP="/tmp/cosmos-sdk-clone-$CSDK_VERSION-$$"
  rm -rf "$CSDK_TMP" "$CSDK_DIR"
  mkdir -p "$CSDK_DIR"
  if ! git clone --quiet --depth=1 --branch "$CSDK_VERSION" \
      https://github.com/cosmos/cosmos-sdk.git "$CSDK_TMP" 2>&1; then
    echo "FAIL: clone cosmos-sdk $CSDK_VERSION" >&2
    rm -rf "$CSDK_TMP"
    exit 3
  fi
  if [ ! -d "$CSDK_TMP/proto" ]; then
    echo "FAIL: no proto/ in cosmos-sdk $CSDK_VERSION" >&2
    rm -rf "$CSDK_TMP"
    exit 3
  fi
  cp -r "$CSDK_TMP/proto/"* "$CSDK_DIR/"
  rm -rf "$CSDK_TMP"
else
  echo "[2/5] cosmos-sdk $CSDK_VERSION already vendored (reuse)."
fi

# Step 4: extract snapshot — both poktroll AND cosmos-sdk
echo "[3/5] extracting shape snapshot..."
VENDORED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
VERSION_TAG="$TAG" \
COSMOS_SDK_VERSION="$CSDK_VERSION" \
COSMOS_SDK_DIR="$CSDK_DIR" \
  python3 "$SCRIPTS/extract.py" "$DEST" > "$SHAPES/$VDIR.json"

# Step 5: compute diff vs previous + update evolution doc
echo "[4/5] diffing vs previous..."
python3 "$SCRIPTS/diff.py" "$SHAPES/$VDIR.json" > "$DIFF_TMP"

echo "[5/5] updating evolution doc..."
python3 "$SCRIPTS/update_evolution.py" \
  --diff "$DIFF_TMP" \
  --snapshot "$SHAPES/$VDIR.json" \
  --tracked "$TRACKED" \
  --evo "$EVO" \
  --tag "$TAG"

# Summary
python3 - <<PYEOF
import json
diff = json.load(open("$DIFF_TMP"))
snap = json.load(open("$SHAPES/$VDIR.json"))
prev = diff.get("previous_version") or "none"
added = len(diff.get("added_messages", []))
removed = len(diff.get("removed_messages", []))
changed = len(diff.get("changed_messages", {}))
total = len(snap["messages"])
print()
print(f"OK $TAG vendored.")
print(f"   Poktroll: $TAG  |  cosmos-sdk: $CSDK_VERSION")
print(f"   Messages: {total} total (+{added}, -{removed}, ~{changed} changed vs {prev}).")
print(f"   Snapshot: $SHAPES/$VDIR.json")
print(f"   Evolution doc: $EVO")
PYEOF

rm -f "$DIFF_TMP"
