package v0_1_34

import (
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	"github.com/pokt-network/pocketscribe/internal/types"

	tokenomics "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_34/gen/pocket/tokenomics"
)

// Supplier decode for the [v0_1_34..] shape range
// (docs/research/supplier-shape-breaks.md §4). The only supplier-closure break
// from the v0_1_27 era is the addition of supplier_stake_after_slash (tag=9) to
// pocket.tokenomics.EventSupplierSlashed. All other supplier msg/KV paths are
// shape-identical and delegate to the v0_1_27 range owner.

// DecodeSupplierMsg implements decoders.Decoder; delegates to v0_1_27.
// MsgStakeSupplier / MsgUnstakeSupplier shapes are unchanged in v0_1_34.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	return v0_1_27.Decoder{}.DecodeSupplierMsg(typeURL, value)
}

// DecodeSupplierEvent implements decoders.Decoder. Handles
// pocket.tokenomics.EventSupplierSlashed natively (v0_1_34 adds
// supplier_stake_after_slash tag=9). All other supplier event types delegate
// to the v0_1_27 range owner whose shapes are identical.
func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	if eventType != "pocket.tokenomics.EventSupplierSlashed" {
		return v0_1_27.Decoder{}.DecodeSupplierEvent(eventType, attrs)
	}
	doc := decoders.EventAttrsJSON(attrs)
	var ev tokenomics.EventSupplierSlashed
	if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
		return nil, fmt.Errorf("v0_1_34 %s: %w", eventType, err)
	}
	return &types.SupplierEvent{Slashed: &types.EventSupplierSlashed{
		ProofMissingPenalty:     ev.ProofMissingPenalty,
		ServiceID:               ev.ServiceId,
		ApplicationAddress:      ev.ApplicationAddress,
		SessionEndBlockHeight:   ev.SessionEndBlockHeight,
		ClaimProofStatusInt:     ev.ClaimProofStatusInt,
		SupplierOperatorAddress: ev.SupplierOperatorAddress,
		SupplierStakeAfterSlash: ev.SupplierStakeAfterSlash,
	}}, nil
}

// DecodeSupplierKV implements decoders.Decoder; delegates to v0_1_27.
// Supplier and ServiceConfigUpdate KV shapes are unchanged in v0_1_34.
func (Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	return v0_1_27.Decoder{}.DecodeSupplierKV(key, value, deleted)
}
