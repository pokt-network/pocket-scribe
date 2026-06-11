// Package v0_1_34 is the decoder for poktroll v0.1.34 — the start of the
// [v0_1_34..] supplier shape range. The shape break is additive:
// pocket.tokenomics.EventSupplierSlashed gains tag=9 supplier_stake_after_slash
// (string). Seven new message types were added to the closure (not yet in
// PocketScribe decode scope). The buf-generated bindings live in gen/ (read-only;
// regenerate via `make gen-proto`). Registered so the lenient router never falls
// back across the v0.1.34 shape boundary (mainnet height 788945).
package v0_1_34

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.34.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_34" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder. The cometbft ABCI block header is identical across all poktroll
// versions, so there is nothing version-specific to do here.
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
