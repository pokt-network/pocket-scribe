#!/usr/bin/env bash
set -euo pipefail
V="${1:?usage: gen_decoder_protos.sh <version_dir e.g. v0_1_8>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$(go env GOPATH)/bin:$PATH"
SRC="$ROOT/third_party/proto/poktroll/$V"
TEMPLATE="$ROOT/buf.gen.poktroll-v0_1_30.yaml"
OUT="$ROOT/internal/decoders/$V/gen"
[ -d "$SRC/pocket" ] || { echo "FATAL: $SRC/pocket not found"; exit 1; }
WS="$(mktemp -d /tmp/bufws-$V-XXXXXX)"; trap 'rm -rf "$WS"' EXIT
# Symlink ONLY pocket/: old vendored trees (v0_1_0/v0_1_10) still contain
# upstream buf.yaml v1 + buf.lock (BSR deps) which must not leak into the ws.
mkdir -p "$WS/poktroll"
ln -s "$SRC/pocket"                                "$WS/poktroll/pocket"
ln -s "$ROOT/third_party/proto/cosmos-sdk/v0_53_0" "$WS/cosmos-sdk"
ln -s "$ROOT/third_party/proto/wkt"                "$WS/wkt"
cat > "$WS/buf.yaml" <<'EOF'
version: v2
modules:
  - path: poktroll
  - path: cosmos-sdk
  - path: wkt
EOF
sed -e "s/v0_1_30/$V/g" \
    -e "s#^    out: internal/decoders/$V/gen\$#    out: $OUT#" \
    "$TEMPLATE" > "$WS/buf.gen.yaml"
grep -q "out: $OUT" "$WS/buf.gen.yaml" || { echo "FATAL: out: rewrite failed"; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT"
(cd "$WS" && buf generate --template buf.gen.yaml poktroll)
N="$(find "$OUT" -name '*.pb.go' | wc -l)"
[ "$N" -gt 0 ] || { echo "FATAL: buf exited 0 but produced no .pb.go"; exit 1; }
echo "OK: generated $N .pb.go files under internal/decoders/$V/gen/"
