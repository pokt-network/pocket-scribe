package v0_1_0

import (
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/supplier"
)

// Supplier-module decode for the [v0_1_0..v0_1_7] shape range
// (docs/research/supplier-shape-breaks.md §3). Mainnet has ZERO supplier
// activity in the v0.1.0..v0.1.7 eras (verified on-chain; decision 4) — this
// implementation exists for registry completeness and decodes the v0_1_0-era
// shapes (hydrated Supplier; pre-refactor ServiceConfigUpdate with Services +
// EffectiveBlockHeight fields instead of the v0_1_8+ OperatorAddress/Service/
// ActivationHeight/DeactivationHeight layout).

// DecodeSupplierMsg implements decoders.Decoder. Only Code==0 txs reach this
// point (handler filters); (nil, nil) = not a supplier msg we persist.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	switch typeURL {
	case "/pocket.supplier.MsgStakeSupplier":
		var m supplier.MsgStakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_0 MsgStakeSupplier: %w", err)
		}
		servicesJSON, err := decoders.MarshalJSONPBSlice(m.Services)
		if err != nil {
			return nil, err
		}
		out := &types.MsgStakeSupplier{
			Signer:          m.Signer,
			OwnerAddress:    m.OwnerAddress,
			OperatorAddress: m.OperatorAddress,
			ServicesJSON:    servicesJSON,
		}
		if m.Stake != nil {
			if !m.Stake.Amount.IsInt64() {
				return nil, fmt.Errorf("v0_1_0 MsgStakeSupplier stake overflows int64: %s", m.Stake.Amount)
			}
			out.StakeAmount = m.Stake.Amount.Int64()
			out.StakeDenom = m.Stake.Denom
		}
		return &types.SupplierMsg{Stake: out}, nil
	case "/pocket.supplier.MsgUnstakeSupplier":
		var m supplier.MsgUnstakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_0 MsgUnstakeSupplier: %w", err)
		}
		return &types.SupplierMsg{Unstake: &types.MsgUnstakeSupplier{
			Signer:          m.Signer,
			OperatorAddress: m.OperatorAddress,
		}}, nil
	default:
		return nil, nil
	}
}

// DecodeSupplierEvent implements decoders.Decoder. The jsonpb decode VALIDATES
// the payload against this range's schema and extracts scalars; JSONB columns
// store the raw attribute JSON verbatim (fidelity).
func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	doc := decoders.EventAttrsJSON(attrs)
	switch eventType {
	case "pocket.supplier.EventSupplierStaked":
		var ev supplier.EventSupplierStaked
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_0 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{Staked: &types.EventSupplierStaked{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingBegin":
		var ev supplier.EventSupplierUnbondingBegin
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_0 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingBegin: &types.EventSupplierUnbondingBegin{
			SupplierJSON:       decoders.EventAttrRaw(attrs, "supplier"),
			ReasonJSON:         decoders.EventAttrRaw(attrs, "reason"),
			SessionEndHeight:   ev.SessionEndHeight,
			UnbondingEndHeight: ev.UnbondingEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingEnd":
		var ev supplier.EventSupplierUnbondingEnd
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_0 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingEnd: &types.EventSupplierUnbondingEnd{
			SupplierJSON:       decoders.EventAttrRaw(attrs, "supplier"),
			ReasonJSON:         decoders.EventAttrRaw(attrs, "reason"),
			SessionEndHeight:   ev.SessionEndHeight,
			UnbondingEndHeight: ev.UnbondingEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingCanceled":
		var ev supplier.EventSupplierUnbondingCanceled
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_0 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingCanceled: &types.EventSupplierUnbondingCanceled{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			AtHeight:         ev.Height,
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierServiceConfigActivated":
		var ev supplier.EventSupplierServiceConfigActivated
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_0 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{ServiceConfigActivated: &types.EventSupplierServiceConfigActivated{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			ActivationHeight: ev.ActivationHeight,
		}}, nil
	default:
		return nil, nil
	}
}

// DecodeSupplierKV implements decoders.Decoder. Only the two proto-carrying
// key layouts are decoded; index pointers and params are skipped (nil, nil).
// Note: v0_1_0's ServiceConfigUpdate has Services+EffectiveBlockHeight fields
// (no OperatorAddress/Service/ActivationHeight/DeactivationHeight). For
// SupplierKeySCURecord, we parse the operator/service/activation from the KEY
// via ParseSCUPrimaryKey and store the whole SCU as JSON.
// ParseSCUPrimaryKey assumes the key layout introduced at v0_1_8; the v0_1_0
// keeper may have used another layout, but this era has zero supplier KV
// activity on mainnet (decision 4) — if this path ever fires, the parse error
// surfaces as a Nak (loud), never as garbage rows.
func (Decoder) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	switch decoders.ClassifySupplierKey(key) {
	case decoders.SupplierKeyRecord:
		if deleted {
			// Supplier record deletion (unbond completion). Phase E decision 6:
			// skip — captured via EventSupplierUnbondingEnd; revisit in Phase F.
			return nil, nil
		}
		var s shared.Supplier
		if err := s.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_0 Supplier KV: %w", err)
		}
		servicesJSON, err := decoders.MarshalJSONPBSlice(s.Services)
		if err != nil {
			return nil, err
		}
		schJSON, err := decoders.MarshalJSONPBSlice(s.ServiceConfigHistory)
		if err != nil {
			return nil, err
		}
		out := &types.SupplierSnapshot{
			OperatorAddress:          s.OperatorAddress,
			OwnerAddress:             s.OwnerAddress,
			ServicesJSON:             servicesJSON,
			UnstakeSessionEndHeight:  int64(s.UnstakeSessionEndHeight),
			ServiceConfigHistoryJSON: schJSON,
		}
		if s.Stake != nil {
			if !s.Stake.Amount.IsInt64() {
				return nil, fmt.Errorf("v0_1_0 Supplier stake overflows int64: %s", s.Stake.Amount)
			}
			out.StakeAmount = s.Stake.Amount.Int64()
			out.StakeDenom = s.Stake.Denom
		}
		return &types.SupplierKVRecord{Supplier: out}, nil
	case decoders.SupplierKeySCURecord:
		if deleted {
			// ParseSCUPrimaryKey assumes the v0_1_8 key layout; the v0_1_0 keeper
			// may differ, but zero mainnet activity means this branch is defensive.
			svc, act, op, err := decoders.ParseSCUPrimaryKey(key)
			if err != nil {
				return nil, fmt.Errorf("v0_1_0 deleted SCU key: %w", err)
			}
			return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
				OperatorAddress: op, ServiceID: svc, ActivationHeight: act, Deleted: true,
			}}, nil
		}
		var scu shared.ServiceConfigUpdate
		if err := scu.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_0 ServiceConfigUpdate KV: %w", err)
		}
		// v0_1_0 SCU has Services+EffectiveBlockHeight (no OperatorAddress/Service).
		// Parse op/svc/act from the key; store the whole SCU object as JSON for fidelity.
		svc, act, op, err := decoders.ParseSCUPrimaryKey(key)
		if err != nil {
			return nil, fmt.Errorf("v0_1_0 SCU key parse: %w", err)
		}
		scuJSON, err := marshalSCU(&scu)
		if err != nil {
			return nil, err
		}
		return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
			OperatorAddress:   op,
			ServiceID:         svc,
			ActivationHeight:  act,
			ServiceConfigJSON: scuJSON,
		}}, nil
	default:
		return nil, nil
	}
}

// marshalSCU serializes the v0_1_0 ServiceConfigUpdate to JSON using jsonpb.
// This is the version-local helper (the v0_1_0 SCU type differs from v0_1_8+).
func marshalSCU(scu *shared.ServiceConfigUpdate) ([]byte, error) {
	// Use MarshalJSONPBSlice as a single-element slice then unwrap.
	arr, err := decoders.MarshalJSONPBSlice([]*shared.ServiceConfigUpdate{scu})
	if err != nil {
		return nil, fmt.Errorf("v0_1_0 ServiceConfigUpdate JSON: %w", err)
	}
	if len(arr) < 2 {
		return nil, fmt.Errorf("v0_1_0 ServiceConfigUpdate JSON unexpectedly short")
	}
	// strip array brackets: [{"..."}] → {"..."}
	return arr[1 : len(arr)-1], nil
}
