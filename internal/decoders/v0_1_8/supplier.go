package v0_1_8

import (
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// Supplier-module decode for the [v0_1_8..v0_1_26] shape range
// (docs/research/supplier-shape-breaks.md §3). In-range versions
// (v0_1_10, v0_1_20) delegate here.

// marshalServiceConfigsJSONPB / marshalSCUsJSONPB are test seams over
// decoders.MarshalJSONPBSlice (same pattern as flushFn/processFn in
// internal/consumer/batch.go): jsonpb cannot fail for these concrete types,
// but the defensive guards stay testable. If a future shape adds a field that
// CAN fail jsonpb (e.g. Any), the guards are already proven to propagate.
var (
	marshalServiceConfigsJSONPB = decoders.MarshalJSONPBSlice[*shared.SupplierServiceConfig]
	marshalSCUsJSONPB           = decoders.MarshalJSONPBSlice[*shared.ServiceConfigUpdate]
)

// DecodeSupplierMsg implements decoders.Decoder. Only Code==0 txs reach this
// point (handler filters); (nil, nil) = not a supplier msg we persist.
func (Decoder) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	switch typeURL {
	case "/pocket.supplier.MsgStakeSupplier":
		var m supplier.MsgStakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 MsgStakeSupplier: %w", err)
		}
		servicesJSON, err := marshalServiceConfigsJSONPB(m.Services)
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
				return nil, fmt.Errorf("v0_1_8 MsgStakeSupplier stake overflows int64: %s", m.Stake.Amount)
			}
			out.StakeAmount = m.Stake.Amount.Int64()
			out.StakeDenom = m.Stake.Denom
		}
		return &types.SupplierMsg{Stake: out}, nil
	case "/pocket.supplier.MsgUnstakeSupplier":
		var m supplier.MsgUnstakeSupplier
		if err := m.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 MsgUnstakeSupplier: %w", err)
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
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{Staked: &types.EventSupplierStaked{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierUnbondingBegin":
		var ev supplier.EventSupplierUnbondingBegin
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
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
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
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
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
		}
		return &types.SupplierEvent{UnbondingCanceled: &types.EventSupplierUnbondingCanceled{
			SupplierJSON:     decoders.EventAttrRaw(attrs, "supplier"),
			AtHeight:         ev.Height,
			SessionEndHeight: ev.SessionEndHeight,
		}}, nil
	case "pocket.supplier.EventSupplierServiceConfigActivated":
		var ev supplier.EventSupplierServiceConfigActivated
		if err := decoders.UnmarshalEventJSON(doc, &ev); err != nil {
			return nil, fmt.Errorf("v0_1_8 %s: %w", eventType, err)
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
			return nil, fmt.Errorf("v0_1_8 Supplier KV: %w", err)
		}
		servicesJSON, err := marshalServiceConfigsJSONPB(s.Services)
		if err != nil {
			return nil, err
		}
		schJSON, err := marshalSCUsJSONPB(s.ServiceConfigHistory)
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
				return nil, fmt.Errorf("v0_1_8 Supplier stake overflows int64: %s", s.Stake.Amount)
			}
			out.StakeAmount = s.Stake.Amount.Int64()
			out.StakeDenom = s.Stake.Denom
		}
		return &types.SupplierKVRecord{Supplier: out}, nil
	case decoders.SupplierKeySCURecord:
		if deleted {
			svc, act, op, err := decoders.ParseSCUPrimaryKey(key)
			if err != nil {
				return nil, fmt.Errorf("v0_1_8 deleted SCU key: %w", err)
			}
			return &types.SupplierKVRecord{ServiceConfigUpdate: &types.ServiceConfigUpdateSnapshot{
				OperatorAddress: op, ServiceID: svc, ActivationHeight: act, Deleted: true,
			}}, nil
		}
		var scu shared.ServiceConfigUpdate
		if err := scu.Unmarshal(value); err != nil {
			return nil, fmt.Errorf("v0_1_8 ServiceConfigUpdate KV: %w", err)
		}
		// A non-deleted SCU KV record must have a non-nil Service field; a nil
		// Service would write an empty string to the NOT NULL service_id column
		// (migration 0040) and signals a malformed or truncated record.
		if scu.Service == nil {
			return nil, fmt.Errorf("v0_1_8 ServiceConfigUpdate KV: nil Service field on non-deleted record (key %q)", key)
		}
		var svcJSON []byte
		j, err := marshalServiceConfigsJSONPB([]*shared.SupplierServiceConfig{scu.Service})
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
