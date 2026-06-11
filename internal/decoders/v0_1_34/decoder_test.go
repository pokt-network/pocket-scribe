package v0_1_34

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Decoder must satisfy the shared decoders.Decoder interface.
var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_34" {
		t.Fatalf("Version() = %q, want v0_1_34", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	// Truncated meta bytes must surface the shared decoder's error through the
	// delegation (proves the method is wired without duplicating the fixture test).
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}

// TestDecodeSupplierMsgDelegates confirms the v0_1_34 delegation path for
// DecodeSupplierMsg is wired (foreign typeURL → (nil, nil) from v0_1_27 owner).
func TestDecodeSupplierMsgDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDecodeSupplierEventNonSlashedDelegates confirms the v0_1_34 delegation
// path for non-slashed events is wired (unknown type → (nil, nil) from v0_1_27).
func TestDecodeSupplierEventNonSlashedDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", []types.EventAttr{})
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDecodeSupplierKVDelegates confirms the v0_1_34 delegation path for
// DecodeSupplierKV is wired (index key → (nil, nil) from v0_1_27 owner).
func TestDecodeSupplierKVDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}
