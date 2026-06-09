package decoders

import "github.com/pokt-network/pocketscribe/internal/types"

// Decoder is implemented by every internal/decoders/v{X}_{Y}_{Z} package and is
// the contract the router dispatches on per block height (ADR-008). The interface
// grows ADDITIVELY: each phase adds a method alongside its implementation. Slice 1
// Phase C commits only the two version-agnostic essentials; Phase D+ add
// DecodeTx / DecodeStateEntity / DecodeEvent when those categories are built.
type Decoder interface {
	// Version returns the canonical decoder version tag, e.g. "v0_1_30".
	Version() string
	// DecodeBlockHeader parses a FilePlugin `block-{H}-meta` payload into the
	// canonical BlockHeader. The header is version-invariant, so every version
	// delegates to the shared DecodeBlockHeader function in this package.
	DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)
}
