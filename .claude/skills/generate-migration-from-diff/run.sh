#!/usr/bin/env bash
# /generate-migration-from-diff vX.Y.Z — emit goose SQL from shape snapshot.
set -euo pipefail

TAG="${1:-}"
if [ -z "$TAG" ]; then
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROOT"
python3 "$SCRIPT_DIR/scripts/generate.py" "$TAG"
