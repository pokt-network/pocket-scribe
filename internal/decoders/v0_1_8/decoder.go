// Package v0_1_8 is the decoder for poktroll v0.1.8 — the start of the
// [v0_1_8..v0_1_26] supplier shape range (pocket.shared.ServiceConfigUpdate
// tag reuse; the chain stores Supplier DEHYDRATED from this version on — see
// docs/research/supplier-shape-breaks.md). The buf-generated bindings live in
// gen/ (read-only; regenerate via `make gen-proto`). Registered so the lenient
// router never falls back across the v0.1.8 shape boundary.
package v0_1_8

import (
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder implements decoders.Decoder for poktroll v0.1.8.
type Decoder struct{}

// Version returns the canonical version tag for this decoder package.
func (Decoder) Version() string { return "v0_1_8" }

// DecodeBlockHeader delegates to the shared, version-invariant block-header
// decoder (the cometbft ABCI header is identical across all poktroll versions).
func (Decoder) DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error) {
	return decoders.DecodeBlockHeader(metaBytes)
}
