// Package v0_1_30 is the decoder for poktroll release v0.1.30. The buf-generated
// proto bindings live in the gen/ subpackage (read-only; regenerate via
// `make gen-proto`); this file is the hand-written adapter binding them to the
// canonical types in internal/types. Phase C implements only the version-invariant
// block header (delegated to the shared decoders helper); tx/state/event
// categories arrive in later phases. New versions are NEW packages — this one is
// never repurposed (ADR-008).
package v0_1_30

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.30.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_30" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder. The cometbft ABCI block header is identical across all poktroll
// versions, so there is nothing version-specific to do here.
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
