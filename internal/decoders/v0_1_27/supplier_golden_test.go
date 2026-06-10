package v0_1_27

// Golden tests for the v0_1_27 shape range [v0_1_27..v0_1_33].
// Blobs are extracted from mainnet block 290584 (v0.1.28 era, which delegates
// here) via tools/fixtureextract golden. They verify real-bytes decode
// correctness for the post-v0.1.27 shape change: EventSupplierStaked carries
// operator_address (not an embedded Supplier), and MsgStakeSupplier has
// expanded service config fields.
//
// To regenerate blobs (e.g. after a decoder bug fix):
//
//	go run ./tools/fixtureextract golden 290584 \
//	    test/fixtures/v0_1_28 \
//	    internal/decoders/testdata/supplier/v0_1_27

import (
	"bytes"
	"os"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"
)

const blobDir = "../testdata/supplier/v0_1_27"

// TestGoldenMsgStake_v0_1_27 decodes the real MsgStakeSupplier Any.value bytes
// from block 290584 and asserts operator address and stake amount.
func TestGoldenMsgStake_v0_1_27(t *testing.T) {
	raw, err := os.ReadFile(blobDir + "/msg_stake.bin")
	if err != nil {
		t.Fatalf("read msg_stake.bin: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", raw)
	if err != nil {
		t.Fatalf("DecodeSupplierMsg: %v", err)
	}
	if got == nil || got.Stake == nil {
		t.Fatalf("expected Stake result, got %v", got)
	}
	s := got.Stake
	const wantOp = "pokt1sszu7p0mgjpsu363wpfhssgxsf00k0rezwy02h"
	if s.OperatorAddress != wantOp {
		t.Fatalf("OperatorAddress = %q, want %q", s.OperatorAddress, wantOp)
	}
	if s.StakeAmount != 60099999988 {
		t.Fatalf("StakeAmount = %d, want 60099999988", s.StakeAmount)
	}
	if s.StakeDenom != "upokt" {
		t.Fatalf("StakeDenom = %q, want upokt", s.StakeDenom)
	}
	if !bytes.Contains(s.ServicesJSON, []byte(`"service_id"`)) {
		t.Fatalf("ServicesJSON missing service_id: %s", s.ServicesJSON)
	}
}

// TestGoldenEventStaked_v0_1_27 decodes the real EventSupplierStaked
// attributes from block 290584 and asserts session_end_height and
// operator_address. In the v0_1_27 shape the event carries operator_address
// directly (not an embedded Supplier); SupplierJSON must be nil.
func TestGoldenEventStaked_v0_1_27(t *testing.T) {
	// event_staked.json for v0_1_27 shape is small: operator_address + session_end_height.
	const (
		wantOp                = "pokt1sszu7p0mgjpsu363wpfhssgxsf00k0rezwy02h"
		wantSessionEndH int64 = 290640
	)
	attrs := []types.EventAttr{
		{Key: "operator_address", Value: `"` + wantOp + `"`},
		{Key: "session_end_height", Value: `"290640"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	if got == nil || got.Staked == nil {
		t.Fatalf("expected Staked result, got %v", got)
	}
	if got.Staked.SessionEndHeight != wantSessionEndH {
		t.Fatalf("SessionEndHeight = %d, want %d", got.Staked.SessionEndHeight, wantSessionEndH)
	}
	if got.Staked.OperatorAddress != wantOp {
		t.Fatalf("OperatorAddress = %q, want %q", got.Staked.OperatorAddress, wantOp)
	}
	// v0_1_27 shape: no embedded Supplier in event.
	if got.Staked.SupplierJSON != nil {
		t.Fatalf("SupplierJSON must be nil for v0_1_27 era; got %s", got.Staked.SupplierJSON)
	}
}

// TestGoldenEventStaked_RealBytes_v0_1_27 reads the saved event_staked.json
// blob (the EventAttrsJSON output) and verifies it contains the expected fields
// for the v0_1_27 shape.
func TestGoldenEventStaked_RealBytes_v0_1_27(t *testing.T) {
	doc, err := os.ReadFile(blobDir + "/event_staked.json")
	if err != nil {
		t.Fatalf("read event_staked.json: %v", err)
	}
	if !bytes.Contains(doc, []byte(`"operator_address"`)) {
		t.Fatalf("event_staked.json missing operator_address field; got: %s", doc)
	}
	if !bytes.Contains(doc, []byte(`"session_end_height"`)) {
		t.Fatalf("event_staked.json missing session_end_height field; got: %s", doc)
	}
	// v0_1_27 shape: no embedded supplier object in the event.
	if bytes.Contains(doc, []byte(`"supplier":`)) {
		t.Fatalf("event_staked.json must NOT contain embedded supplier for v0_1_27 era; got: %s", doc)
	}
}

// TestGoldenSupplierKV_v0_1_27 decodes the real Supplier/operator_address/
// proto value from block 290584 and asserts the operator address.
func TestGoldenSupplierKV_v0_1_27(t *testing.T) {
	value, err := os.ReadFile(blobDir + "/supplier_kv.bin")
	if err != nil {
		t.Fatalf("read supplier_kv.bin: %v", err)
	}
	// Synthetic key — ClassifySupplierKey only needs the correct prefix.
	key := []byte("Supplier/operator_address/pokt109whtw8nltx7l5vm5a8806mw2dplqhpg8rlntz/")
	got, err := Decoder{}.DecodeSupplierKV(key, value, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV: %v", err)
	}
	if got == nil || got.Supplier == nil {
		t.Fatalf("expected Supplier result, got %v", got)
	}
	s := got.Supplier
	const wantOp = "pokt109whtw8nltx7l5vm5a8806mw2dplqhpg8rlntz"
	if s.OperatorAddress != wantOp {
		t.Fatalf("OperatorAddress = %q, want %q", s.OperatorAddress, wantOp)
	}
	// v0_1_27 era: stored Supplier is still DEHYDRATED (spike §4d finding).
	if s.StakeAmount != 60004999991 {
		t.Fatalf("StakeAmount = %d, want 60004999991", s.StakeAmount)
	}
}

// TestGoldenSCUKV_v0_1_27 decodes the real ServiceConfigUpdate/service_id/
// proto value from block 290584 and asserts key fields.
func TestGoldenSCUKV_v0_1_27(t *testing.T) {
	value, err := os.ReadFile(blobDir + "/scu_kv.bin")
	if err != nil {
		t.Fatalf("read scu_kv.bin: %v", err)
	}
	// Key from inspect: service_id=bera, act=242341, op=pokt109whtw8nltx7l5vm5a8806mw2dplqhpg8rlntz
	const (
		actHeight int64 = 242341
		svcID           = "bera"
		op              = "pokt109whtw8nltx7l5vm5a8806mw2dplqhpg8rlntz"
	)
	actEnc := [8]byte{}
	putBE8(actEnc[:], uint64(actHeight))
	key := []byte("ServiceConfigUpdate/service_id/" + svcID + "/")
	key = append(key, actEnc[:]...)
	key = append(key, '/')
	key = append(key, []byte(op+"/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, value, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV: %v", err)
	}
	if got == nil || got.ServiceConfigUpdate == nil {
		t.Fatalf("expected ServiceConfigUpdate result, got %v", got)
	}
	scu := got.ServiceConfigUpdate
	if scu.ServiceID != svcID {
		t.Fatalf("ServiceID = %q, want %q", scu.ServiceID, svcID)
	}
	if scu.ActivationHeight != actHeight {
		t.Fatalf("ActivationHeight = %d, want %d", scu.ActivationHeight, actHeight)
	}
	if scu.OperatorAddress != op {
		t.Fatalf("OperatorAddress = %q, want %q", scu.OperatorAddress, op)
	}
	if len(scu.ServiceConfigJSON) == 0 {
		t.Fatal("ServiceConfigJSON must be non-empty")
	}
}

// putBE8 writes v as 8-byte big-endian into dst.
func putBE8(dst []byte, v uint64) {
	dst[0] = byte(v >> 56)
	dst[1] = byte(v >> 48)
	dst[2] = byte(v >> 40)
	dst[3] = byte(v >> 32)
	dst[4] = byte(v >> 24)
	dst[5] = byte(v >> 16)
	dst[6] = byte(v >> 8)
	dst[7] = byte(v)
}
