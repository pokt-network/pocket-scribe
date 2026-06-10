package decoders

import "github.com/pokt-network/pocketscribe/internal/types"

// Decoder is implemented by every internal/decoders/v{X}_{Y}_{Z} package and is
// the contract the router dispatches on per block height (ADR-008). The interface
// grows ADDITIVELY: each phase adds a method alongside its implementation. Slice 1
// Phase C commits only the two version-agnostic essentials; Phase E adds the
// supplier decode methods (tx msgs, events, KV state).
type Decoder interface {
	// Version returns the canonical decoder version tag, e.g. "v0_1_30".
	Version() string
	// DecodeBlockHeader parses a FilePlugin `block-{H}-meta` payload into the
	// canonical BlockHeader. The header is version-invariant, so every version
	// delegates to the shared DecodeBlockHeader function in this package.
	DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)
	// DecodeSupplierMsg decodes one tx-body message given its Any type_url and
	// value bytes. Returns (nil, nil) when typeURL is not a supplier-module
	// message this indexer persists. An error means a real decode failure —
	// the consumer must NOT ack (spec §10).
	DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error)
	// DecodeSupplierEvent decodes one typed supplier event from its ABCI
	// attributes. Returns (nil, nil) for event types not persisted in Phase E.
	DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error)
	// DecodeSupplierKV decodes one StoreKVPair captured from the "supplier"
	// store. Returns (nil, nil) for index/params/non-persisted records
	// (key discrimination table: docs/research/phase-e-spike-findings.md §4d).
	DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error)
}
