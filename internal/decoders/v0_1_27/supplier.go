package v0_1_27

import (
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// Supplier-module decode for the [v0_1_27..v0_1_33] shape range
// (docs/research/supplier-shape-breaks.md §3). In-range versions
// (v0_1_28, v0_1_29, v0_1_30) delegate here. Era differences from v0_1_8:
// EventSupplierStaked and EventSupplierServiceConfigActivated have
// operator_address instead of a supplier embed; unbonding events retain
// the supplier embed unchanged.

// DecodeSupplierMsg implements decoders.Decoder. Only Code==0 txs reach this
// point (handler filters); (nil, nil) = not a supplier msg we persist.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	switch typeURL {
	case "/pocket.supplier.MsgStakeSupplier":
		var m supplier.MsgStakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_27 MsgStakeSupplier: %w", err)
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
				return nil, fmt.Errorf("v0_1_27 MsgStakeSupplier stake overflows int64: %s", m.Stake.Amount)
			}
			out.StakeAmount = m.Stake.Amount.Int64()
			out.StakeDenom = m.Stake.Denom
		}
		return &types.SupplierMsg{Stake: out}, nil
	case "/pocket.supplier.MsgUnstakeSupplier":
		var m supplier.MsgUnstakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_27 MsgUnstakeSupplier: %w", err)
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
// v0.1.27 era differences: EventSupplierStaked has OperatorAddress (no supplier embed);
// EventSupplierServiceConfigActivated has OperatorAddress + ServiceId (no supplier embed).
// Unbonding events retain the supplier embed (no break at v0.1.27 for those).
func (Decoder) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	doc := decoders.EventAttrsJSON(attrs)
	switch eventType {
	case "pocket.supplier.EventSupplierStaked":
		var ev supplier.EventSupplierStaked
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_27 %s: %w", eventType, err)
		}
		// v0.1.27+: OperatorAddress replaces the supplier embed
		return &types.SupplierEvent{Staked: &types.EventSupplierStaked{
			OperatorAddress:  ev.OperatorAddress,
			SessionEndHeight: ev.SessionEndHeight,
			SupplierJSON:     nil,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingBegin":
		var ev supplier.EventSupplierUnbondingBegin
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_27 %s: %w", eventType, err)
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
			return nil, fmt.Errorf("v0_1_27 %s: %w", eventType, err)
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
			return nil, fmt.Errorf("v0_1_27 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingCanceled: &types.EventSupplierUnbondingCanceled{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			AtHeight:         ev.Height,
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierServiceConfigActivated":
		var ev supplier.EventSupplierServiceConfigActivated
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_27 %s: %w", eventType, err)
		}
		// v0.1.27+: OperatorAddress + ServiceId replace the supplier embed
		return &types.SupplierEvent{ServiceConfigActivated: &types.EventSupplierServiceConfigActivated{
			OperatorAddress:  ev.OperatorAddress,
			ServiceID:        ev.ServiceId,
			ActivationHeight: ev.ActivationHeight,
			SupplierJSON:     nil,
		}}, nil
	default:
		return nil, nil
	}
}

// DecodeSupplierKV implements decoders.Decoder. Only the two proto-carrying
// key layouts are decoded; index pointers and params are skipped (nil, nil).
// KV shapes for Supplier and ServiceConfigUpdate are identical to v0_1_8 range.
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
			return nil, fmt.Errorf("v0_1_27 Supplier KV: %w", err)
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
				return nil, fmt.Errorf("v0_1_27 Supplier stake overflows int64: %s", s.Stake.Amount)
			}
			out.StakeAmount = s.Stake.Amount.Int64()
			out.StakeDenom = s.Stake.Denom
		}
		return &types.SupplierKVRecord{Supplier: out}, nil
	case decoders.SupplierKeySCURecord:
		if deleted {
			svc, act, op, err := decoders.ParseSCUPrimaryKey(key)
			if err != nil {
				return nil, fmt.Errorf("v0_1_27 deleted SCU key: %w", err)
			}
			return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
				OperatorAddress: op, ServiceID: svc, ActivationHeight: act, Deleted: true,
			}}, nil
		}
		var scu shared.ServiceConfigUpdate
		if err := scu.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_27 ServiceConfigUpdate KV: %w", err)
		}
		// A non-deleted SCU KV record must have a non-nil Service field; a nil
		// Service would write an empty string to the NOT NULL service_id column
		// (migration 0040) and signals a malformed or truncated record.
		if scu.Service == nil {
			return nil, fmt.Errorf("v0_1_27 ServiceConfigUpdate KV: nil Service field on non-deleted record (key %q)", key)
		}
		var svcJSON []byte
		j, err := decoders.MarshalJSONPBSlice([]*shared.SupplierServiceConfig{scu.Service})
		if err != nil {
			return nil, err
		}
		// single-element array → unwrap to the object
		svcJSON = j[1 : len(j)-1]
		return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
			OperatorAddress:    scu.OperatorAddress,
			ServiceID:          scu.Service.GetServiceId(),
			ActivationHeight:   scu.ActivationHeight,
			DeactivationHeight: scu.DeactivationHeight,
			ServiceConfigJSON:  svcJSON,
		}}, nil
	default:
		return nil, nil
	}
}
