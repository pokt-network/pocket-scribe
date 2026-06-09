#!/usr/bin/env bash
# scaffold_decoder.sh v0_1_31 — emit a new versioned decoder adapter skeleton.
#
# Non-destructive: prints the decoder.go skeleton to stdout. The operator pipes
# it to internal/decoders/<vdir>/decoder.go ONLY if that file does not yet exist
# (ADR-008: a committed decoder version is never overwritten). The skeleton
# implements the current minimal Decoder interface (Version + DecodeBlockHeader,
# which delegates to the shared version-invariant decoder). As later phases add
# interface methods, extend this template alongside them.
set -euo pipefail

VDIR="${1:-}"
if [ -z "$VDIR" ]; then
  echo "usage: $0 v{X}_{Y}_{Z}   (e.g. v0_1_31)" >&2
  exit 2
fi
if ! [[ "$VDIR" =~ ^v[0-9]+_[0-9]+_[0-9]+$ ]]; then
  echo "invalid version dir: $VDIR (expected v{X}_{Y}_{Z}, e.g. v0_1_31)" >&2
  exit 2
fi

cat <<EOF
// Package ${VDIR} is the decoder for poktroll release ${VDIR}. The buf-generated
// proto bindings live in the gen/ subpackage (read-only; regenerate via
// \`make gen-proto\`); this file is the hand-written adapter binding them to the
// canonical types in internal/types. New versions are NEW packages — this one is
// never repurposed (ADR-008).
package ${VDIR}

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll ${VDIR}.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "${VDIR}" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
EOF
