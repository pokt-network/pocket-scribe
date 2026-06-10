package v0_1_27

// Error-path and boundary coverage tests for the v0_1_27 supplier decoder.
// These complement the round-trip tests in supplier_test.go.

import (
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// ---------------------------------------------------------------------------
// DecodeSupplierMsg — error paths
// ---------------------------------------------------------------------------

// TestDecodeSupplierMsgStakeCorruptBytes verifies that corrupt MsgStakeSupplier
// bytes return an error with the v0_1_27 prefix.
func TestDecodeSupplierMsgStakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgStakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_27 MsgStakeSupplier") {
		t.Fatalf("error should mention v0_1_27 MsgStakeSupplier: %v", err)
	}
}

// TestDecodeSupplierMsgStakeOverflowInt64 verifies stake amount overflow detection.
func TestDecodeSupplierMsgStakeOverflowInt64(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &supplier.MsgStakeSupplier{
		OperatorAddress: "pokt1op27",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: bigAmt},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", raw)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("error should mention overflow: %v", err)
	}
}

// TestDecodeSupplierMsgUnstakeCorruptBytes verifies error on corrupt unstake bytes.
func TestDecodeSupplierMsgUnstakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgUnstakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgUnstakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_27 MsgUnstakeSupplier") {
		t.Fatalf("error should mention v0_1_27 MsgUnstakeSupplier: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierEvent — error paths
// ---------------------------------------------------------------------------

// TestDecodeSupplierEventStakedCorrupt verifies malformed JSON returns an error.
func TestDecodeSupplierEventStakedCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierStaked JSON")
	}
	if !strings.Contains(err.Error(), "v0_1_27") {
		t.Fatalf("error should mention v0_1_27: %v", err)
	}
}

// TestDecodeSupplierEventUnbondingBeginCorrupt verifies malformed JSON.
func TestDecodeSupplierEventUnbondingBeginCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingBegin", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierUnbondingBegin JSON")
	}
}

// TestDecodeSupplierEventUnbondingEndCorrupt verifies malformed JSON.
func TestDecodeSupplierEventUnbondingEndCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingEnd", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierUnbondingEnd JSON")
	}
}

// TestDecodeSupplierEventUnbondingCanceledCorrupt verifies malformed JSON.
func TestDecodeSupplierEventUnbondingCanceledCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingCanceled", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierUnbondingCanceled JSON")
	}
}

// TestDecodeSupplierEventServiceConfigActivatedCorrupt verifies malformed JSON.
func TestDecodeSupplierEventServiceConfigActivatedCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "activation_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierServiceConfigActivated", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierServiceConfigActivated JSON")
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierKV — error paths
// ---------------------------------------------------------------------------

// TestDecodeSupplierKVRecordCorruptBytes verifies error on malformed Supplier proto.
func TestDecodeSupplierKVRecordCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op27/"),
		[]byte{0xff, 0xfe, 0x00},
		false,
	)
	if err == nil {
		t.Fatal("expected error for corrupt Supplier KV bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_27 Supplier KV") {
		t.Fatalf("error should mention v0_1_27 Supplier KV: %v", err)
	}
}

// TestDecodeSupplierKVRecordStakeOverflow verifies stake overflow error.
func TestDecodeSupplierKVRecordStakeOverflow(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &shared.Supplier{
		OperatorAddress: "pokt1op27",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: bigAmt},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op27/"),
		raw,
		false,
	)
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if !strings.Contains(err.Error(), "overflows int64") {
		t.Fatalf("error should mention overflow: %v", err)
	}
}

// TestDecodeSupplierKVSCUCorruptBytes verifies error on malformed SCU proto.
func TestDecodeSupplierKVSCUCorruptBytes(t *testing.T) {
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232}
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op27/")...)
	_, err := Decoder{}.DecodeSupplierKV(key, []byte{0xff, 0xfe, 0x00}, false)
	if err == nil {
		t.Fatal("expected error for corrupt SCU KV bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_27 ServiceConfigUpdate KV") {
		t.Fatalf("error should mention v0_1_27 ServiceConfigUpdate KV: %v", err)
	}
}

// TestDecodeSupplierKVDeletedSCUBadKey verifies that a malformed deleted SCU key
// surfaces an error instead of writing garbage rows (Nak path).
func TestDecodeSupplierKVDeletedSCUBadKey(t *testing.T) {
	badKey := []byte("ServiceConfigUpdate/service_id/x")
	_, err := Decoder{}.DecodeSupplierKV(badKey, nil, true)
	if err == nil {
		t.Fatal("expected error: malformed deleted SCU key")
	}
	if !strings.Contains(err.Error(), "v0_1_27 deleted SCU key") {
		t.Fatalf("error should mention v0_1_27 deleted SCU key: %v", err)
	}
}
