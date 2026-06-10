package v0_1_8

import (
	"bytes"
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
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
		Signer:          "pokt1signer",
		OperatorAddress: "pokt1operator",
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
	if got.Unstake.Signer != "pokt1signer" || got.Unstake.OperatorAddress != "pokt1operator" {
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

func TestDecodeSupplierEventStakedExtractsAndValidates(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		{Key: "supplier", Value: `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"60000000000"},"services":[{"service_id":"eth","endpoints":[{"url":"https://x","rpc_type":"JSON_RPC","configs":[]}]}]}`},
		{Key: "msg_index", Value: "0"},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	ev := got.Staked
	if ev == nil || ev.SessionEndHeight != 135840 {
		t.Fatalf("decoded = %+v", got)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
}

func TestDecodeSupplierEventUnbondingBegin(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"200"`},
		{Key: "unbonding_end_height", Value: `"300"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingBegin", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingBegin: %v", err)
	}
	ev := got.UnbondingBegin
	if ev == nil {
		t.Fatalf("want UnbondingBegin, got %+v", got)
	}
	if ev.SessionEndHeight != 200 || ev.UnbondingEndHeight != 300 {
		t.Fatalf("heights wrong: session=%d unbonding=%d", ev.SessionEndHeight, ev.UnbondingEndHeight)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
	if !bytes.Contains(ev.ReasonJSON, []byte("VOLUNTARY")) {
		t.Fatalf("ReasonJSON = %s", ev.ReasonJSON)
	}
}

func TestDecodeSupplierEventUnbondingEnd(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"400"`},
		{Key: "unbonding_end_height", Value: `"500"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingEnd", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingEnd: %v", err)
	}
	ev := got.UnbondingEnd
	if ev == nil {
		t.Fatalf("want UnbondingEnd, got %+v", got)
	}
	if ev.SessionEndHeight != 400 || ev.UnbondingEndHeight != 500 {
		t.Fatalf("heights wrong: session=%d unbonding=%d", ev.SessionEndHeight, ev.UnbondingEndHeight)
	}
}

func TestDecodeSupplierEventUnbondingCanceled(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "height", Value: `"600"`},
		{Key: "session_end_height", Value: `"700"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingCanceled", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent UnbondingCanceled: %v", err)
	}
	ev := got.UnbondingCanceled
	if ev == nil {
		t.Fatalf("want UnbondingCanceled, got %+v", got)
	}
	if ev.AtHeight != 600 || ev.SessionEndHeight != 700 {
		t.Fatalf("heights wrong: at=%d session=%d", ev.AtHeight, ev.SessionEndHeight)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
}

func TestDecodeSupplierEventServiceConfigActivated(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "activation_height", Value: `"800"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierServiceConfigActivated", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent ServiceConfigActivated: %v", err)
	}
	ev := got.ServiceConfigActivated
	if ev == nil {
		t.Fatalf("want ServiceConfigActivated, got %+v", got)
	}
	if ev.ActivationHeight != 800 {
		t.Fatalf("ActivationHeight = %d, want 800", ev.ActivationHeight)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
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
		OperatorAddress: "pokt1op",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(42)},
	}
	raw, _ := in.Marshal()
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op/"), raw, false)
	if err != nil || got.Supplier == nil {
		t.Fatalf("DecodeSupplierKV: %+v, %v", got, err)
	}
	if got.Supplier.StakeAmount != 42 || got.Supplier.ServicesJSON != nil {
		t.Fatalf("snapshot = %+v (dehydrated supplier must have nil ServicesJSON)", got.Supplier)
	}
}

// TestDecodeSupplierKVRecordDeleted verifies the deleted Supplier record is skipped
// (Phase E decision 6: capture via EventSupplierUnbondingEnd instead).
func TestDecodeSupplierKVRecordDeleted(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op/"), nil, true)
	if got != nil || err != nil {
		t.Fatalf("deleted Supplier KV: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDecodeSupplierKVSCURoundtrip verifies the SCU non-deleted path decodes
// a properly-constructed ServiceConfigUpdate with a non-nil Service field.
func TestDecodeSupplierKVSCURoundtrip(t *testing.T) {
	scu := &shared.ServiceConfigUpdate{
		OperatorAddress:    "pokt1op",
		ActivationHeight:   1000,
		DeactivationHeight: 0,
		Service: &shared.SupplierServiceConfig{
			ServiceId: "anvil",
			Endpoints: []*shared.SupplierEndpoint{{
				Url:     "https://rpc.example.com",
				RpcType: shared.RPCType_JSON_RPC,
			}},
		},
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}

	// Build a valid primary-layout key: ServiceConfigUpdate/service_id/<svc>/<act:BE8>/<op>/
	// activation height 1000 big-endian 8 bytes:
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // 1000 = 0x3E8
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, raw, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV SCU: %v", err)
	}
	if got.ServiceConfigUpdate == nil {
		t.Fatalf("want ServiceConfigUpdate, got %+v", got)
	}
	scu2 := got.ServiceConfigUpdate
	if scu2.ServiceID != "anvil" {
		t.Fatalf("ServiceID = %q, want \"anvil\"", scu2.ServiceID)
	}
	if scu2.OperatorAddress != "pokt1op" {
		t.Fatalf("OperatorAddress = %q, want \"pokt1op\"", scu2.OperatorAddress)
	}
	if scu2.ActivationHeight != 1000 {
		t.Fatalf("ActivationHeight = %d, want 1000", scu2.ActivationHeight)
	}
	if !bytes.Contains(scu2.ServiceConfigJSON, []byte("anvil")) {
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
		OperatorAddress:  "pokt1op",
		ActivationHeight: 1000,
		// Service intentionally nil — malformed record
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}

	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232}
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op/")...)

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
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // activation height 1000
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op/")...)

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
	if scu.ServiceID != "anvil" {
		t.Fatalf("ServiceID = %q, want \"anvil\"", scu.ServiceID)
	}
	if scu.OperatorAddress != "pokt1op" {
		t.Fatalf("OperatorAddress = %q, want \"pokt1op\"", scu.OperatorAddress)
	}
	if scu.ActivationHeight != 1000 {
		t.Fatalf("ActivationHeight = %d, want 1000", scu.ActivationHeight)
	}
}

func TestDecodeSupplierKVIgnoresIndexLayouts(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
