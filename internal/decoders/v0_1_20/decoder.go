// Package v0_1_20 is the Phase-D delegating adapter for poktroll v0.1.20.
// The block header is version-invariant (cometbft ABCI RequestFinalizeBlock);
// this adapter delegates DecodeBlockHeader to the shared decoder.
// No gen/ subpackage — codegen is deferred to Phase E (tx/state/event categories).
package v0_1_20

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0_1_20.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_20" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
