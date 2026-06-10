package v0_1_8

// Error-path and boundary coverage tests for the v0_1_8 supplier decoder.
// These complement the round-trip tests in supplier_test.go.

import (
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// ---------------------------------------------------------------------------
// DecodeSupplierMsg — error paths
// ---------------------------------------------------------------------------

// TestDecodeSupplierMsgStakeCorruptBytes verifies that corrupt MsgStakeSupplier
// bytes return an error wrapping the unmarshal failure.
func TestDecodeSupplierMsgStakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgStakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_8 MsgStakeSupplier") {
		t.Fatalf("error should mention v0_1_8 MsgStakeSupplier: %v", err)
	}
}

// TestDecodeSupplierMsgStakeOverflowInt64 verifies that a stake whose Amount
// does not fit in int64 returns an overflow error (math.Int > MaxInt64).
func TestDecodeSupplierMsgStakeOverflowInt64(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &supplier.MsgStakeSupplier{
		OperatorAddress: "pokt1op8",
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

// TestDecodeSupplierMsgUnstakeCorruptBytes verifies that corrupt bytes produce
// a descriptive error for the MsgUnstakeSupplier case.
func TestDecodeSupplierMsgUnstakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgUnstakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgUnstakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_8 MsgUnstakeSupplier") {
		t.Fatalf("error should mention v0_1_8 MsgUnstakeSupplier: %v", err)
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
	if !strings.Contains(err.Error(), "v0_1_8") {
		t.Fatalf("error should mention v0_1_8: %v", err)
	}
}

// TestDecodeSupplierEventUnbondingBeginCorrupt verifies malformed JSON produces an error.
func TestDecodeSupplierEventUnbondingBeginCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingBegin", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierUnbondingBegin JSON")
	}
	if !strings.Contains(err.Error(), "v0_1_8") {
		t.Fatalf("error should mention v0_1_8: %v", err)
	}
}

// TestDecodeSupplierEventUnbondingEndCorrupt verifies malformed JSON produces an error.
func TestDecodeSupplierEventUnbondingEndCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingEnd", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt EventSupplierUnbondingEnd JSON")
	}
}

// TestDecodeSupplierEventUnbondingCanceledCorrupt verifies malformed JSON produces an error.
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
		[]byte("Supplier/operator_address/pokt1op8/"),
		[]byte{0xff, 0xfe, 0x00},
		false,
	)
	if err == nil {
		t.Fatal("expected error for corrupt Supplier KV bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_8 Supplier KV") {
		t.Fatalf("error should mention v0_1_8 Supplier KV: %v", err)
	}
}

// TestDecodeSupplierKVRecordStakeOverflow verifies stake overflow detection.
func TestDecodeSupplierKVRecordStakeOverflow(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &shared.Supplier{
		OperatorAddress: "pokt1op8",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: bigAmt},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op8/"),
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
	key = append(key, []byte("pokt1op8/")...)
	_, err := Decoder{}.DecodeSupplierKV(key, []byte{0xff, 0xfe, 0x00}, false)
	if err == nil {
		t.Fatal("expected error for corrupt SCU KV bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_8 ServiceConfigUpdate KV") {
		t.Fatalf("error should mention v0_1_8 ServiceConfigUpdate KV: %v", err)
	}
}

// TestDecodeSupplierKVDeletedSCUBadKey verifies that a deleted SCU with a
// malformed key surfaces an error (Nak path) instead of writing garbage rows.
func TestDecodeSupplierKVDeletedSCUBadKey(t *testing.T) {
	badKey := []byte("ServiceConfigUpdate/service_id/x")
	_, err := Decoder{}.DecodeSupplierKV(badKey, nil, true)
	if err == nil {
		t.Fatal("expected error: malformed deleted SCU key")
	}
	if !strings.Contains(err.Error(), "v0_1_8 deleted SCU key") {
		t.Fatalf("error should mention v0_1_8 deleted SCU key: %v", err)
	}
}
