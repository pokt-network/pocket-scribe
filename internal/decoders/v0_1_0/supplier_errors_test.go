package v0_1_0

// Error-path and branch coverage tests for the v0_1_0 supplier decoder.
// These tests complement the smoke tests in supplier_test.go.

import (
	"bytes"
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/pokt-network/pocketscribe/internal/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/supplier"
)

// ---------------------------------------------------------------------------
// DecodeSupplierMsg — error paths
// ---------------------------------------------------------------------------

// TestDecodeSupplierMsgStakeRoundtrip verifies the full stake decode path
// including ServicesJSON and stake amount.
func TestDecodeSupplierMsgStakeRoundtrip(t *testing.T) {
	in := &supplier.MsgStakeSupplier{
		Signer:          "pokt1signer0",
		OwnerAddress:    "pokt1owner0",
		OperatorAddress: "pokt1op0",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(60000000000)},
		Services: []*shared.SupplierServiceConfig{{
			ServiceId: "eth",
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
	if s == nil || s.OperatorAddress != "pokt1op0" || s.StakeAmount != 60000000000 {
		t.Fatalf("decoded = %+v", s)
	}
	if !bytes.Contains(s.ServicesJSON, []byte(`"service_id":"eth"`)) {
		t.Fatalf("ServicesJSON = %s", s.ServicesJSON)
	}
}

// TestDecodeSupplierMsgStakeCorruptBytes verifies that a corrupted MsgStakeSupplier
// value returns an error wrapping the unmarshal failure.
func TestDecodeSupplierMsgStakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgStakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_0 MsgStakeSupplier") {
		t.Fatalf("error should mention v0_1_0 MsgStakeSupplier: %v", err)
	}
}

// TestDecodeSupplierMsgStakeOverflowInt64 verifies that a stake whose math.Int
// does not fit in int64 returns a descriptive overflow error.
func TestDecodeSupplierMsgStakeOverflowInt64(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &supplier.MsgStakeSupplier{
		OperatorAddress: "pokt1op0",
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

// TestDecodeSupplierMsgUnstakeRoundtrip verifies the MsgUnstakeSupplier path.
func TestDecodeSupplierMsgUnstakeRoundtrip(t *testing.T) {
	in := &supplier.MsgUnstakeSupplier{
		Signer:          "pokt1signer0",
		OperatorAddress: "pokt1op0",
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgUnstakeSupplier", raw)
	if err != nil {
		t.Fatalf("DecodeSupplierMsg Unstake: %v", err)
	}
	if got.Unstake == nil {
		t.Fatalf("want Unstake, got nil")
	}
	if got.Unstake.Signer != "pokt1signer0" || got.Unstake.OperatorAddress != "pokt1op0" {
		t.Fatalf("decoded Unstake = %+v", got.Unstake)
	}
}

// TestDecodeSupplierMsgUnstakeCorruptBytes verifies error on corrupt bytes.
func TestDecodeSupplierMsgUnstakeCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgUnstakeSupplier", []byte{0xff, 0xfe, 0x00})
	if err == nil {
		t.Fatal("expected error for corrupt MsgUnstakeSupplier bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_0 MsgUnstakeSupplier") {
		t.Fatalf("error should mention v0_1_0 MsgUnstakeSupplier: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierEvent — all cases
// ---------------------------------------------------------------------------

// TestDecodeSupplierEventStakedRoundtrip verifies the staked event decode.
func TestDecodeSupplierEventStakedRoundtrip(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner0","operator_address":"pokt1op0","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		{Key: "supplier", Value: supplierJSON},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	ev := got.Staked
	if ev == nil || ev.SessionEndHeight != 135840 {
		t.Fatalf("decoded = %+v", got)
	}
	if !bytes.Contains(ev.SupplierJSON, []byte(`"operator_address":"pokt1op0"`)) {
		t.Fatalf("SupplierJSON = %s", ev.SupplierJSON)
	}
}

// TestDecodeSupplierEventStakedCorruptJSON verifies error on malformed JSON.
func TestDecodeSupplierEventStakedCorruptJSON(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"broken`},
	}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt event JSON")
	}
}

// TestDecodeSupplierEventUnbondingBeginRoundtrip verifies the unbonding-begin event.
func TestDecodeSupplierEventUnbondingBeginRoundtrip(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner0","operator_address":"pokt1op0","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
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
	if ev == nil || ev.SessionEndHeight != 200 || ev.UnbondingEndHeight != 300 {
		t.Fatalf("decoded = %+v", got)
	}
}

// TestDecodeSupplierEventUnbondingBeginCorrupt verifies error on corrupt attrs.
func TestDecodeSupplierEventUnbondingBeginCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingBegin", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt UnbondingBegin event JSON")
	}
}

// TestDecodeSupplierEventUnbondingEndRoundtrip verifies the unbonding-end event.
func TestDecodeSupplierEventUnbondingEndRoundtrip(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner0","operator_address":"pokt1op0","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
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
	if ev == nil || ev.SessionEndHeight != 400 || ev.UnbondingEndHeight != 500 {
		t.Fatalf("decoded = %+v", got)
	}
}

// TestDecodeSupplierEventUnbondingEndCorrupt verifies error on corrupt attrs.
func TestDecodeSupplierEventUnbondingEndCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "session_end_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingEnd", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt UnbondingEnd event JSON")
	}
}

// TestDecodeSupplierEventUnbondingCanceledRoundtrip verifies the canceled event.
func TestDecodeSupplierEventUnbondingCanceledRoundtrip(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner0","operator_address":"pokt1op0","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
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
	if ev == nil || ev.AtHeight != 600 || ev.SessionEndHeight != 700 {
		t.Fatalf("decoded = %+v", got)
	}
}

// TestDecodeSupplierEventUnbondingCanceledCorrupt verifies error on corrupt attrs.
func TestDecodeSupplierEventUnbondingCanceledCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierUnbondingCanceled", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt UnbondingCanceled event JSON")
	}
}

// TestDecodeSupplierEventServiceConfigActivatedRoundtrip verifies the SCU-activated event.
func TestDecodeSupplierEventServiceConfigActivatedRoundtrip(t *testing.T) {
	supplierJSON := `{"owner_address":"pokt1owner0","operator_address":"pokt1op0","stake":{"denom":"upokt","amount":"1000"},"services":[]}`
	attrs := []types.EventAttr{
		{Key: "supplier", Value: supplierJSON},
		{Key: "activation_height", Value: `"800"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierServiceConfigActivated", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent ServiceConfigActivated: %v", err)
	}
	ev := got.ServiceConfigActivated
	if ev == nil || ev.ActivationHeight != 800 {
		t.Fatalf("decoded = %+v", got)
	}
}

// TestDecodeSupplierEventServiceConfigActivatedCorrupt verifies error.
func TestDecodeSupplierEventServiceConfigActivatedCorrupt(t *testing.T) {
	attrs := []types.EventAttr{{Key: "activation_height", Value: `"broken`}}
	_, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierServiceConfigActivated", attrs)
	if err == nil {
		t.Fatal("expected error for corrupt ServiceConfigActivated event JSON")
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierKV — error paths and remaining branches
// ---------------------------------------------------------------------------

// TestDecodeSupplierKVRecordCorruptBytes verifies error on malformed Supplier proto.
func TestDecodeSupplierKVRecordCorruptBytes(t *testing.T) {
	_, err := Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op0/"),
		[]byte{0xff, 0xfe, 0x00},
		false,
	)
	if err == nil {
		t.Fatal("expected error for corrupt Supplier KV bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_0 Supplier KV") {
		t.Fatalf("error should mention v0_1_0 Supplier KV: %v", err)
	}
}

// TestDecodeSupplierKVRecordStakeOverflow verifies that a Supplier with stake
// exceeding int64 returns an overflow error.
func TestDecodeSupplierKVRecordStakeOverflow(t *testing.T) {
	bigAmt, _ := math.NewIntFromString("99999999999999999999999")
	in := &shared.Supplier{
		OperatorAddress: "pokt1op0",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: bigAmt},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op0/"),
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
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // height 1000
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op0/")...)

	_, err := Decoder{}.DecodeSupplierKV(key, []byte{0xff, 0xfe, 0x00}, false)
	if err == nil {
		t.Fatal("expected error for corrupt SCU bytes")
	}
	if !strings.Contains(err.Error(), "v0_1_0 ServiceConfigUpdate KV") {
		t.Fatalf("error should mention v0_1_0 ServiceConfigUpdate KV: %v", err)
	}
}

// TestDecodeSupplierKVDeletedSCUBadKey verifies the Nak path described in the
// comment: if ParseSCUPrimaryKey fails on a deleted SCU key, the error surfaces.
// This is the "loud failure" path instead of writing garbage rows.
func TestDecodeSupplierKVDeletedSCUBadKey(t *testing.T) {
	// A key with the SCU prefix but no height segment → ParseSCUPrimaryKey fails.
	badKey := []byte("ServiceConfigUpdate/service_id/x")
	_, err := Decoder{}.DecodeSupplierKV(badKey, nil, true)
	if err == nil {
		t.Fatal("expected error: malformed deleted SCU key (Nak path)")
	}
	if !strings.Contains(err.Error(), "v0_1_0 deleted SCU key") {
		t.Fatalf("error should mention v0_1_0 deleted SCU key: %v", err)
	}
}

// TestDecodeSupplierKVRecordDeletedSkipped verifies that a deleted Supplier record
// (unbond completion) is silently skipped — Phase E decision 6.
func TestDecodeSupplierKVRecordDeletedSkipped(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV(
		[]byte("Supplier/operator_address/pokt1op0/"),
		nil,
		true,
	)
	if got != nil || err != nil {
		t.Fatalf("deleted Supplier KV: want (nil,nil), got %+v, %v", got, err)
	}
}

// TestDecodeSupplierKVSCURoundtrip verifies the v0_1_0 SCU round-trip.
// v0_1_0 SCU uses Services+EffectiveBlockHeight (no OperatorAddress/Service).
// ParseSCUPrimaryKey extracts op/svc/act from the key.
func TestDecodeSupplierKVSCURoundtrip(t *testing.T) {
	scu := &shared.ServiceConfigUpdate{
		EffectiveBlockHeight: 100,
		Services: []*shared.SupplierServiceConfig{{
			ServiceId: "anvil",
		}},
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // 1000 big-endian
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op0/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, raw, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV SCU: %v", err)
	}
	if got.ServiceConfigUpdate == nil {
		t.Fatalf("want ServiceConfigUpdate, got %+v", got)
	}
	scu2 := got.ServiceConfigUpdate
	if scu2.ServiceID != "anvil" || scu2.ActivationHeight != 1000 || scu2.OperatorAddress != "pokt1op0" {
		t.Fatalf("decoded SCU = %+v", scu2)
	}
	if !bytes.Contains(scu2.ServiceConfigJSON, []byte("anvil")) {
		t.Fatalf("ServiceConfigJSON = %s", scu2.ServiceConfigJSON)
	}
}

// TestDecodeSupplierKVDeletedSCUValidKey verifies that a deleted SCU with a
// well-formed key returns a Deleted:true snapshot (not an error).  This covers
// the success branch of the deleted-SCU path (line 176–178 in supplier.go).
func TestDecodeSupplierKVDeletedSCUValidKey(t *testing.T) {
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // height 1000 big-endian
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op0/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, nil, true)
	if err != nil {
		t.Fatalf("unexpected error for valid deleted SCU key: %v", err)
	}
	if got == nil || got.ServiceConfigUpdate == nil {
		t.Fatalf("want ServiceConfigUpdate snapshot, got %+v", got)
	}
	scu := got.ServiceConfigUpdate
	if !scu.Deleted {
		t.Fatalf("expected Deleted=true, got %+v", scu)
	}
	if scu.OperatorAddress != "pokt1op0" || scu.ServiceID != "anvil" || scu.ActivationHeight != 1000 {
		t.Fatalf("decoded snapshot = %+v", scu)
	}
}

// TestDecodeSupplierKVSCUBadKeyNonDeleted verifies that a non-deleted SCU with
// a malformed key (valid prefix but truncated height segment) returns an error.
// This covers the ParseSCUPrimaryKey error branch for the live-record path
// (line 187–189 in supplier.go).
func TestDecodeSupplierKVSCUBadKeyNonDeleted(t *testing.T) {
	// Key has the SCU prefix and a service segment but no height bytes.
	badKey := []byte("ServiceConfigUpdate/service_id/anvil")
	_, err := Decoder{}.DecodeSupplierKV(badKey, []byte{}, false)
	if err == nil {
		t.Fatal("expected error for malformed non-deleted SCU key")
	}
	if !strings.Contains(err.Error(), "v0_1_0 SCU key parse") {
		t.Fatalf("error should mention v0_1_0 SCU key parse: %v", err)
	}
}

// TestDecodeSupplierKVUnknownKeySkipped verifies that a key that does not match
// any known supplier prefix (e.g., a params or index-pointer key) is silently
// skipped — returns (nil, nil).  This covers the default case (line 200–201).
func TestDecodeSupplierKVUnknownKeySkipped(t *testing.T) {
	got, err := Decoder{}.DecodeSupplierKV([]byte("p_supplier"), nil, false)
	if got != nil || err != nil {
		t.Fatalf("expected (nil,nil) for unknown key, got %+v, %v", got, err)
	}
}
