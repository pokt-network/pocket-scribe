package v0_1_27

import (
	"bytes"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

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

func TestDecodeSupplierMsgSkipsForeignTypeURL(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

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

func TestDecodeSupplierEventSkipsUnknownType(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", nil)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

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

func TestDecodeSupplierKVIgnoresIndexLayouts(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
