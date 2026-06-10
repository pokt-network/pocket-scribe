package v0_1_28

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

var _ decoders.Decoder = Decoder{}

func TestVersion(t *testing.T) {
	if got := (Decoder{}).Version(); got != "v0_1_28" {
		t.Fatalf("Version() = %q, want v0_1_28", got)
	}
}

func TestDecodeBlockHeaderDelegates(t *testing.T) {
	if _, err := (Decoder{}).DecodeBlockHeader([]byte{0x0a, 0x01}); err == nil {
		t.Fatal("expected error decoding truncated meta bytes")
	}
}

// TestDelegateSupplierMsgDelegates confirms the v0_1_28 delegation path for
// DecodeSupplierMsg is wired (foreign typeURL → (nil, nil) from v0_1_27 owner).
func TestDelegateSupplierMsgDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierMsg("/cosmos.bank.v1beta1.MsgSend", []byte{0x01})
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDelegateSupplierEventDelegates confirms the v0_1_28 delegation path for
// DecodeSupplierEvent is wired (unknown type → (nil, nil) from v0_1_27 owner).
func TestDelegateSupplierEventDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierEvent("pocket.proof.EventProofSubmitted", []types.EventAttr{})
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDelegateSupplierKVDelegates confirms the v0_1_28 delegation path for
// DecodeSupplierKV is wired (index key → (nil, nil) from v0_1_27 owner).
func TestDelegateSupplierKVDelegates(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("ServiceConfigUpdate/operator_address/pokt1op/eth/"), []byte("ptr"), false)
	if got != nil || err != nil {
		t.Fatalf("delegation: want (nil,nil), got %+v, %v", got, err)
	}
}
