package v0_1_27

import (
	"bytes"
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// ---------------------------------------------------------------------------
// DecodeSupplierMsg
// ---------------------------------------------------------------------------

func TestDecodeSupplierMsgStakeRoundtrip(t *testing.T) {
	in := &supplier.MsgStakeSupplier{
		Signer:          "pokt1signer",
		OwnerAddress:    "pokt1owner",
		OperatorAddress: "pokt1operator",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(60000000000)},
		Services: []*shared.SupplierServiceConfig{{
			ServiceId: "eth",
			Endpoints: []*shared.SupplierEndpoint{{Url: "https://example.net", RpcType: shared.RPCType_JSON_RPC}},
		}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", raw)
	if err != nil {
		t.Fatalf("DecodeSupplierMsg: %v", err)
	}
	s := got.Stake
	if s == nil || s.OperatorAddress != "pokt1operator" || s.StakeAmount != 60000000000 || s.StakeDenom != "upokt" {
		t.Fatalf("decoded = %+v", got)
	}
	if !bytes.Contains(s.ServicesJSON, []byte(`"service_id":"eth"`)) {
		t.Fatalf("ServicesJSON = %s", s.ServicesJSON)
	}
}

func TestDecodeSupplierMsgUnstakeRoundtrip(t *testing.T) {
	in := &supplier.MsgUnstakeSupplier{
		Signer:          "pokt1signer27",
		OperatorAddress: "pokt1operator27",
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgUnstakeSupplier", raw)
	if err != nil {
		t.Fatalf("DecodeSupplierMsg: %v", err)
	}
	if got.Unstake == nil {
		t.Fatalf("want Unstake, got %+v", got)
	}
	if got.Unstake.Signer != "pokt1signer27" || got.Unstake.OperatorAddress != "pokt1operator27" {
		t.Fatalf("decoded Unstake = %+v", got.Unstake)
	}
}

func TestDecodeSupplierMsgSkipsForeignTypeURL(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierEvent
// ---------------------------------------------------------------------------

// TestDecodeSupplierEventStakedV027UsesOperatorAddress verifies the v0.1.27+
// era: EventSupplierStaked has operator_address (no supplier embed).
func TestDecodeSupplierEventStakedV027UsesOperatorAddress(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"290590"`},
		{Key: "operator_address", Value: `"pokt1operator27"`},
		{Key: "msg_index", Value: "0"},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	ev := got.Staked
	if ev == nil || ev.OperatorAddress != "pokt1operator27" || ev.SessionEndHeight != 290590 {
		t.Fatalf("decoded = %+v", got)
	}
	if ev.SupplierJSON != nil {
		t.Fatalf("v0.1.27+ staked event must have nil SupplierJSON, got %s", ev.SupplierJSON)
	}
}

func TestDecodeSupplierEventUnbondingBegin(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner27","operator_address":"pokt1op27","stake":{"denom":"upokt","amount":"2000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"210"`},
		{Key: "unbonding_end_height", Value: `"310"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingBegin", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingBegin: %v", err)
	}
	ev := got.UnbondingBegin
	if ev == nil {
		t.Fatalf("want UnbondingBegin, got %+v", got)
	}
	if ev.SessionEndHeight != 210 || ev.UnbondingEndHeight != 310 {
		t.Fatalf("heights wrong: session=%d unbonding=%d", ev.SessionEndHeight, ev.UnbondingEndHeight)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op27"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
	if !bytes.Contains(ev.ReasonJSON, []byte("VOLUNTARY")) {
		t.Fatalf("ReasonJSON = %s", ev.ReasonJSON)
	}
}

func TestDecodeSupplierEventUnbondingEnd(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner27","operator_address":"pokt1op27","stake":{"denom":"upokt","amount":"2000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"410"`},
		{Key: "unbonding_end_height", Value: `"510"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingEnd", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingEnd: %v", err)
	}
	ev := got.UnbondingEnd
	if ev == nil {
		t.Fatalf("want UnbondingEnd, got %+v", got)
	}
	if ev.SessionEndHeight != 410 || ev.UnbondingEndHeight != 510 {
		t.Fatalf("heights wrong: session=%d unbonding=%d", ev.SessionEndHeight, ev.UnbondingEndHeight)
	}
}

func TestDecodeSupplierEventUnbondingCanceled(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner27","operator_address":"pokt1op27","stake":{"denom":"upokt","amount":"2000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "height", Value: `"610"`},
		{Key: "session_end_height", Value: `"710"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingCanceled", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingCanceled: %v", err)
	}
	ev := got.UnbondingCanceled
	if ev == nil {
		t.Fatalf("want UnbondingCanceled, got %+v", got)
	}
	if ev.AtHeight != 610 || ev.SessionEndHeight != 710 {
		t.Fatalf("heights wrong: at=%d session=%d", ev.AtHeight, ev.SessionEndHeight)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op27"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
}

// TestDecodeSupplierEventServiceConfigActivatedV027 verifies the v0.1.27+ era:
// EventSupplierServiceConfigActivated has OperatorAddress + ServiceId (no supplier embed).
func TestDecodeSupplierEventServiceConfigActivatedV027(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "operator_address", Value: `"pokt1operator27"`},
		{Key: "service_id", Value: `"anvil"`},
		{Key: "activation_height", Value: `"820"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierServiceConfigActivated", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent ServiceConfigActivated: %v", err)
	}
	ev := got.ServiceConfigActivated
	if ev == nil {
		t.Fatalf("want ServiceConfigActivated, got %+v", got)
	}
	if ev.ActivationHeight != 820 {
		t.Fatalf("ActivationHeight = %d, want 820", ev.ActivationHeight)
	}
	if ev.OperatorAddress != "pokt1operator27" {
		t.Fatalf("OperatorAddress = %q, want \"pokt1operator27\"", ev.OperatorAddress)
	}
	if ev.ServiceID != "anvil" {
		t.Fatalf("ServiceID = %q, want \"anvil\"", ev.ServiceID)
	}
	if ev.SupplierJSON != nil {
		t.Fatalf("v0.1.27+ ServiceConfigActivated must have nil SupplierJSON, got %s", ev.SupplierJSON)
	}
}

func TestDecodeSupplierEventSkipsUnknownType(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", nil)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierKV
// ---------------------------------------------------------------------------

func TestDecodeSupplierKVRecordRoundtrip(t *testing.T) {
	in := &shared.Supplier{
		OwnerAddress:    "pokt1owner",
		OperatorAddress: "pokt1op27",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(77)},
	}
	raw, _ := in.Marshal()
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op27/"), raw, false)
	if err != nil || got.Supplier == nil {
		t.Fatalf("DecodeSupplierKV: %+v, %v", got, err)
	}
	if got.Supplier.StakeAmount != 77 || got.Supplier.OperatorAddress != "pokt1op27" {
		t.Fatalf("snapshot = %+v", got.Supplier)
	}
}

// TestDecodeSupplierKVRecordDeleted verifies the deleted Supplier record is skipped
// (Phase E decision 6: capture via EventSupplierUnbondingEnd instead).
func TestDecodeSupplierKVRecordDeleted(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op27/"), nil, true)
	if got != nil || err != nil {
		t.Fatalf("deleted Supplier KV: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDecodeSupplierKVSCURoundtrip verifies the SCU non-deleted path decodes
// a properly-constructed ServiceConfigUpdate with a non-nil Service field.
func TestDecodeSupplierKVSCURoundtrip(t *testing.T) {
	scu := &shared.ServiceConfigUpdate{
		OperatorAddress:    "pokt1op27",
		ActivationHeight:   2000,
		DeactivationHeight: 0,
		Service: &shared.SupplierServiceConfig{
			ServiceId: "polygon",
			Endpoints: []*shared.SupplierEndpoint{{
				Url:     "https://rpc27.example.com",
				RpcType: shared.RPCType_JSON_RPC,
			}},
		},
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}

	// activation height 2000 big-endian 8 bytes: 0x7D0
	actBytes := []byte{0, 0, 0, 0, 0, 0, 7, 208}
	key := append([]byte("ServiceConfigUpdate/service_id/polygon/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op27/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, raw, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV SCU: %v", err)
	}
	if got.ServiceConfigUpdate == nil {
		t.Fatalf("want ServiceConfigUpdate, got %+v", got)
	}
	scu2 := got.ServiceConfigUpdate
	if scu2.ServiceID != "polygon" {
		t.Fatalf("ServiceID = %q, want \"polygon\"", scu2.ServiceID)
	}
	if scu2.OperatorAddress != "pokt1op27" {
		t.Fatalf("OperatorAddress = %q, want \"pokt1op27\"", scu2.OperatorAddress)
	}
	if scu2.ActivationHeight != 2000 {
		t.Fatalf("ActivationHeight = %d, want 2000", scu2.ActivationHeight)
	}
	if !bytes.Contains(scu2.ServiceConfigJSON, []byte("polygon")) {
		t.Fatalf("ServiceConfigJSON = %s", scu2.ServiceConfigJSON)
	}
	if scu2.Deleted {
		t.Fatalf("Deleted must be false for non-deleted record")
	}
}

// TestDecodeSupplierKVSCUNilServiceReturnsError verifies that a non-deleted SCU
// KV with a nil Service field surfaces an error rather than writing empty service_id.
func TestDecodeSupplierKVSCUNilServiceReturnsError(t *testing.T) {
	scu := &shared.ServiceConfigUpdate{
		OperatorAddress:  "pokt1op27",
		ActivationHeight: 2000,
		// Service intentionally nil — malformed record
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}

	actBytes := []byte{0, 0, 0, 0, 0, 0, 7, 208}
	key := append([]byte("ServiceConfigUpdate/service_id/polygon/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op27/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, raw, false)
	if err == nil {
		t.Fatalf("expected error for nil Service, got result %+v", got)
	}
	if !strings.Contains(err.Error(), "nil Service field") {
		t.Fatalf("error message unexpected: %v", err)
	}
}

// TestDecodeSupplierKVSCUDeleted verifies the deleted SCU path extracts key fields.
func TestDecodeSupplierKVSCUDeleted(t *testing.T) {
	actBytes := []byte{0, 0, 0, 0, 0, 0, 7, 208} // activation height 2000
	key := append([]byte("ServiceConfigUpdate/service_id/polygon/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op27/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, nil, true)
	if err != nil {
		t.Fatalf("DecodeSupplierKV deleted SCU: %v", err)
	}
	if got.ServiceConfigUpdate == nil {
		t.Fatalf("want ServiceConfigUpdate, got %+v", got)
	}
	scu := got.ServiceConfigUpdate
	if !scu.Deleted {
		t.Fatalf("Deleted must be true")
	}
	if scu.ServiceID != "polygon" {
		t.Fatalf("ServiceID = %q, want \"polygon\"", scu.ServiceID)
	}
	if scu.OperatorAddress != "pokt1op27" {
		t.Fatalf("OperatorAddress = %q, want \"pokt1op27\"", scu.OperatorAddress)
	}
	if scu.ActivationHeight != 2000 {
		t.Fatalf("ActivationHeight = %d, want 2000", scu.ActivationHeight)
	}
}

func TestDecodeSupplierKVIgnoresIndexLayouts(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
