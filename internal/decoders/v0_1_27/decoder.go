// Package v0_1_27 is the decoder for poktroll v0.1.27 — the start of the
// [v0_1_27..v0_1_33] supplier shape range (EventSupplierStaked /
// EventSupplierServiceConfigActivated / EventSupplierSlashed restructured:
// supplier embed removed, operator_address added). The buf-generated bindings
// live in gen/ (read-only; regenerate via `make gen-proto`). Registered so the
// lenient router never falls back across the v0.1.27 shape boundary.
package v0_1_27

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.27.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_27" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
