package v0_1_8

import (
	"bytes"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
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

func TestDecodeSupplierEventSkipsUnknownType(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", nil)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

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

func TestDecodeSupplierKVIgnoresIndexLayouts(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
