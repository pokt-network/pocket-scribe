package v0_1_0

import (
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
)

// TestDecodeSupplierKVRecordSmokeTest verifies that the v0_1_0 decoder can
// round-trip a Supplier KV record. Mainnet has zero supplier activity in the
// v0.1.0..v0.1.7 eras (decision 4 — negative fixture) so this is a
// constructed-bytes smoke test only; golden tests use real bytes from later eras.
func TestDecodeSupplierKVRecordSmokeTest(t *testing.T) {
	in := &shared.Supplier{
		OwnerAddress:    "pokt1owner0",
		OperatorAddress: "pokt1op0",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(100)},
		Services: []*shared.SupplierServiceConfig{{
			ServiceId: "svc0",
		}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op0/"), raw, false)
	if err != nil || got.Supplier == nil {
		t.Fatalf("DecodeSupplierKV: %+v, %v", got, err)
	}
	if got.Supplier.StakeAmount != 100 || got.Supplier.OperatorAddress != "pokt1op0" {
		t.Fatalf("snapshot = %+v", got.Supplier)
	}
	// v0_1_0 Supplier stores services (hydrated era)
	if got.Supplier.ServicesJSON == nil {
		t.Fatalf("v0_1_0 Supplier with services must have non-nil ServicesJSON")
	}
}

func TestDecodeSupplierMsgSkipsForeignTypeURL(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}

func TestDecodeSupplierEventSkipsUnknownType(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", nil)
	if got != nil || err != nil {
		t.Fatalf("want (nil,nil), got %+v, %v", got, err)
	}
}
