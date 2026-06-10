package types

import "time"

// Position is stamped by the consumer handler (from the BlockEnvelope and the
// fan-out message metadata), NOT by decoders — decoders see only chain bytes.
// Height/Time are the invariant-#1 axis; TxIndex/EventIndex complete the
// deterministic PK (block_time, block_height, tx_index, event_index).
type Position struct {
	Height     int64
	Time       time.Time
	TxIndex    int32 // -1 → stored as 0 with EventIndex disambiguating? NO: block-level events keep -1 sentinel only on the bus; rows store the table default 0 for block-level. See handler.
	EventIndex int32 // msg tables: the msg index within its tx
}

// MsgStakeSupplier → msg_stake_supplier (hypertable). Field-stable across
// v0_1_0..v0_1_33 (docs/research/supplier-shape-breaks.md §3a).
type MsgStakeSupplier struct {
	Position
	Signer          string
	OwnerAddress    string
	OperatorAddress string
	StakeAmount     int64
	StakeDenom      string
	ServicesJSON    []byte // JSON array of SupplierServiceConfig (jsonpb, OrigName)
}

// MsgUnstakeSupplier → msg_unstake_supplier (hypertable).
type MsgUnstakeSupplier struct {
	Position
	Signer          string
	OperatorAddress string
}

// SupplierMsg is a tagged union: exactly one field is non-nil.
type SupplierMsg struct {
	Stake   *MsgStakeSupplier
	Unstake *MsgUnstakeSupplier
}

// EventSupplierStaked → event_supplier_staked. SupplierJSON is the raw
// "supplier" attribute (pre-v0.1.27 eras only); OperatorAddress is set from
// v0.1.27 on (column added by migration 0032). Exactly one of the two is set.
type EventSupplierStaked struct {
	Position
	SupplierJSON     []byte
	SessionEndHeight int64
	OperatorAddress  string
}

// EventSupplierUnbondingBegin → event_supplier_unbonding_begin. The unbonding
// events retain the supplier embed across ALL versions (no v0.1.27 break).
type EventSupplierUnbondingBegin struct {
	Position
	SupplierJSON       []byte
	ReasonJSON         []byte // raw "reason" attribute (enum name as JSON string)
	SessionEndHeight   int64
	UnbondingEndHeight int64
}

// EventSupplierUnbondingEnd → event_supplier_unbonding_end.
type EventSupplierUnbondingEnd struct {
	Position
	SupplierJSON       []byte
	ReasonJSON         []byte
	SessionEndHeight   int64
	UnbondingEndHeight int64
}

// EventSupplierUnbondingCanceled → event_supplier_unbonding_canceled.
type EventSupplierUnbondingCanceled struct {
	Position
	SupplierJSON     []byte
	AtHeight         int64 // event field "height"
	SessionEndHeight int64
}

// EventSupplierServiceConfigActivated → event_supplier_service_config_activated.
// Pre-v0.1.27: SupplierJSON + ActivationHeight. v0.1.27+: OperatorAddress +
// ServiceID + ActivationHeight (0032 columns).
type EventSupplierServiceConfigActivated struct {
	Position
	SupplierJSON     []byte
	ActivationHeight int64
	OperatorAddress  string
	ServiceID        string
}

// SupplierEvent is a tagged union: exactly one field is non-nil.
type SupplierEvent struct {
	Staked                 *EventSupplierStaked
	UnbondingBegin         *EventSupplierUnbondingBegin
	UnbondingEnd           *EventSupplierUnbondingEnd
	UnbondingCanceled      *EventSupplierUnbondingCanceled
	ServiceConfigActivated *EventSupplierServiceConfigActivated
}

// SupplierSnapshot → supplier_history (append-only, PK (operator_address,
// block_height)). From v0.1.8 the chain stores Supplier DEHYDRATED (no
// services / service_config_history) — those fields stay nil and the hydrated
// truth lives in ServiceConfigUpdateSnapshot rows (decision 5).
type SupplierSnapshot struct {
	Position
	OperatorAddress          string
	OwnerAddress             string
	StakeAmount              int64
	StakeDenom               string
	ServicesJSON             []byte
	UnstakeSessionEndHeight  int64
	ServiceConfigHistoryJSON []byte
}

// ServiceConfigUpdateSnapshot → supplier_service_config_update_history
// (append-only). One row per chain KV write of a ServiceConfigUpdate primary
// (`ServiceConfigUpdate/service_id/...` keys; index layouts are skipped).
type ServiceConfigUpdateSnapshot struct {
	Position
	OperatorAddress    string
	ServiceID          string
	ActivationHeight   int64
	DeactivationHeight int64
	ServiceConfigJSON  []byte
	Deleted            bool
}

// SupplierKVRecord is a tagged union: exactly one field is non-nil.
type SupplierKVRecord struct {
	Supplier            *SupplierSnapshot
	ServiceConfigUpdate *ServiceConfigUpdateSnapshot
}
