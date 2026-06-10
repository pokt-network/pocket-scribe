package v0_1_30

import (
	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Supplier decode delegates to the v0_1_27 range owner: the supplier closure is
// shape-identical across [v0_1_27..v0_1_33] (docs/research/supplier-shape-breaks.md).

// DecodeSupplierMsg implements decoders.Decoder; delegates to v0_1_27.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	return v0_1_27.Decoder{}.DecodeSupplierMsg(typeURL, value)
}

// DecodeSupplierEvent implements decoders.Decoder; delegates to v0_1_27.
func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	return v0_1_27.Decoder{}.DecodeSupplierEvent(eventType, attrs)
}

// DecodeSupplierKV implements decoders.Decoder; delegates to v0_1_27.
func (Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	return v0_1_27.Decoder{}.DecodeSupplierKV(key, value, deleted)
}
