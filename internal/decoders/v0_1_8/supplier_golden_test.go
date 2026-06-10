package v0_1_8

// Golden tests for the v0_1_8 shape range [v0_1_8..v0_1_26].
// Blobs are extracted from mainnet block 135836 (v0.1.20 era, which delegates
// here) via tools/fixtureextract golden. They verify real-bytes decode
// correctness that constructed-byte tests cannot catch (wire layout, enum
// encoding, actual field population).
//
// To regenerate blobs (e.g. after a decoder bug fix):
//
//	go run ./tools/fixtureextract golden 135836 \
//	    test/fixtures/v0_1_20 \
//	    internal/decoders/testdata/supplier/v0_1_8

import (
	"bytes"
	"os"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// blobDir is the path relative to the repo root. Tests run with cwd = package
// dir, so we walk up two levels from internal/decoders/v0_1_8/ to root.
const blobDir = "../testdata/supplier/v0_1_8"

// TestGoldenMsgStake_v0_1_8 decodes the real MsgStakeSupplier Any.value bytes
// from block 135836 and asserts operator address and stake amount.
func TestGoldenMsgStake_v0_1_8(t *testing.T) {
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
	const wantOp = "pokt16ar6g3wd9ppat0rtm390wdhnt06kf3z4u2mxm8"
	if s.OperatorAddress != wantOp {
		t.Fatalf("OperatorAddress = %q, want %q", s.OperatorAddress, wantOp)
	}
	if s.StakeAmount != 60000000000 {
		t.Fatalf("StakeAmount = %d, want 60000000000", s.StakeAmount)
	}
	if s.StakeDenom != "upokt" {
		t.Fatalf("StakeDenom = %q, want upokt", s.StakeDenom)
	}
	if !bytes.Contains(s.ServicesJSON, []byte(`"service_id"`)) {
		t.Fatalf("ServicesJSON missing service_id: %s", s.ServicesJSON)
	}
}

// TestGoldenEventStaked_v0_1_8 decodes the real EventSupplierStaked attributes
// JSON from block 135836 and asserts session_end_height. The v0_1_8 shape
// embeds the full Supplier in the "supplier" attribute (~19 KiB); we assert
// SupplierJSON is non-empty to verify that path, and check session_end_height
// equals 135840 (session block at that height).
func TestGoldenEventStaked_v0_1_8(t *testing.T) {
	// Reconstruct attrs that produce the saved event_staked.json. The key attrs
	// for EventSupplierStaked are "session_end_height" and "supplier". We use
	// the pre-extracted JSON doc directly via a synthetic single-attr set to
	// exercise the jsonpb unmarshal path on real bytes.
	eventJSON, err := os.ReadFile(blobDir + "/event_staked.json")
	if err != nil {
		t.Fatalf("read event_staked.json: %v", err)
	}
	// The eventJSON IS the EventAttrsJSON output; reconstruct minimal attrs for
	// the test by embedding it as a synthetic "session_end_height" + "supplier".
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"135840"`},
		// splice the saved JSON supplier value (omit outer braces — the key is
		// the attr key; attr value is the JSON object itself).
		{Key: "supplier", Value: string(extractAttrValue(eventJSON, "supplier"))},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	if got == nil || got.Staked == nil {
		t.Fatalf("expected Staked result, got %v", got)
	}
	if got.Staked.SessionEndHeight != 135840 {
		t.Fatalf("SessionEndHeight = %d, want 135840", got.Staked.SessionEndHeight)
	}
	if len(got.Staked.SupplierJSON) == 0 {
		t.Fatal("SupplierJSON must be non-empty for v0_1_8 era (embedded hydrated supplier)")
	}
}

// TestGoldenSupplierKV_v0_1_8 decodes the real Supplier/operator_address/
// proto value from block 135836 and asserts the operator address.
func TestGoldenSupplierKV_v0_1_8(t *testing.T) {
	value, err := os.ReadFile(blobDir + "/supplier_kv.bin")
	if err != nil {
		t.Fatalf("read supplier_kv.bin: %v", err)
	}
	// Synthetic key — ClassifySupplierKey only needs the correct prefix.
	key := []byte("Supplier/operator_address/pokt12qse7tla856d4xqru2eg9w4hta6cy2npzuzlku/")
	got, err := Decoder{}.DecodeSupplierKV(key, value, false)
	if err != nil {
		t.Fatalf("DecodeSupplierKV: %v", err)
	}
	if got == nil || got.Supplier == nil {
		t.Fatalf("expected Supplier result, got %v", got)
	}
	s := got.Supplier
	const wantOp = "pokt12qse7tla856d4xqru2eg9w4hta6cy2npzuzlku"
	if s.OperatorAddress != wantOp {
		t.Fatalf("OperatorAddress = %q, want %q", s.OperatorAddress, wantOp)
	}
	// v0_1_8 era: stored Supplier is DEHYDRATED (spike §4d finding).
	// Services field is absent (nil slice JSON).
	if s.StakeAmount != 60000000000 {
		t.Fatalf("StakeAmount = %d, want 60000000000", s.StakeAmount)
	}
}

// TestGoldenSCUKV_v0_1_8 decodes the real ServiceConfigUpdate/service_id/
// proto value from block 135836 and asserts key fields.
func TestGoldenSCUKV_v0_1_8(t *testing.T) {
	value, err := os.ReadFile(blobDir + "/scu_kv.bin")
	if err != nil {
		t.Fatalf("read scu_kv.bin: %v", err)
	}
	// Synthetic SCU primary key: service_id=arb_one, act=96801 (0x000000000001794_1 BE8),
	// operator=pokt12qse7tla856d4xqru2eg9w4hta6cy2npzuzlku — actual values from inspect.
	const (
		actHeight int64 = 96801
		svcID           = "arb_one"
		op              = "pokt12qse7tla856d4xqru2eg9w4hta6cy2npzuzlku"
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

// extractAttrValue returns the raw JSON value of a field named key from an
// EventAttrsJSON document ({"key":value,...}). Used only in tests.
func extractAttrValue(doc []byte, key string) []byte {
	// Simple scan: find `"key":` and extract the value until the next top-level
	// comma or closing brace. Works for simple object values.
	search := `"` + key + `":`
	idx := bytes.Index(doc, []byte(search))
	if idx < 0 {
		return nil
	}
	rest := doc[idx+len(search):]
	// Find end of this value: track brace depth.
	depth := 0
	for i, b := range rest {
		switch b {
		case '{':
			depth++
		case '}':
			depth--
			if depth < 0 {
				return rest[:i]
			}
		case ',':
			if depth == 0 {
				return rest[:i]
			}
		}
	}
	return rest
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
