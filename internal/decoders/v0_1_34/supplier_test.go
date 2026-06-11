package v0_1_34

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/types"

	tokenomics "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_34/gen/pocket/tokenomics"
)

// ---------------------------------------------------------------------------
// DecodeSupplierEvent — EventSupplierSlashed (v0_1_34 native path)
// ---------------------------------------------------------------------------

// TestDecodeSupplierEventSlashedV034WithStakeAfterSlash verifies that the
// v0_1_34-era EventSupplierSlashed decoder reads the new supplier_stake_after_slash
// field (tag=9) correctly.
func TestDecodeSupplierEventSlashedV034WithStakeAfterSlash(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "proof_missing_penalty", Value: `"1000upokt"`},
		{Key: "service_id", Value: `"anvil"`},
		{Key: "application_address", Value: `"pokt1app34"`},
		{Key: "session_end_block_height", Value: `"800000"`},
		{Key: "claim_proof_status_int", Value: `1`},
		{Key: "supplier_operator_address", Value: `"pokt1op34"`},
		{Key: "supplier_stake_after_slash", Value: `"999000upokt"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.tokenomics.EventSupplierSlashed", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent: %v", err)
	}
	if got == nil || got.Slashed == nil {
		t.Fatalf("want Slashed non-nil, got %+v", got)
	}
	ev := got.Slashed
	if ev.ProofMissingPenalty != "1000upokt" {
		t.Errorf("ProofMissingPenalty = %q, want \"1000upokt\"", ev.ProofMissingPenalty)
	}
	if ev.ServiceID != "anvil" {
		t.Errorf("ServiceID = %q, want \"anvil\"", ev.ServiceID)
	}
	if ev.ApplicationAddress != "pokt1app34" {
		t.Errorf("ApplicationAddress = %q, want \"pokt1app34\"", ev.ApplicationAddress)
	}
	if ev.SessionEndBlockHeight != 800000 {
		t.Errorf("SessionEndBlockHeight = %d, want 800000", ev.SessionEndBlockHeight)
	}
	if ev.ClaimProofStatusInt != 1 {
		t.Errorf("ClaimProofStatusInt = %d, want 1", ev.ClaimProofStatusInt)
	}
	if ev.SupplierOperatorAddress != "pokt1op34" {
		t.Errorf("SupplierOperatorAddress = %q, want \"pokt1op34\"", ev.SupplierOperatorAddress)
	}
	if ev.SupplierStakeAfterSlash != "999000upokt" {
		t.Errorf("SupplierStakeAfterSlash = %q, want \"999000upokt\"", ev.SupplierStakeAfterSlash)
	}
}

// TestDecodeSupplierEventSlashedV034WithoutStakeAfterSlash verifies graceful handling
// when supplier_stake_after_slash is absent (e.g. replaying pre-v0_1_34 blocks
// through the v0_1_34 decoder under a misconfigured router — field defaults to "").
func TestDecodeSupplierEventSlashedV034WithoutStakeAfterSlash(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "proof_missing_penalty", Value: `"500upokt"`},
		{Key: "service_id", Value: `"eth"`},
		{Key: "application_address", Value: `"pokt1appold"`},
		{Key: "session_end_block_height", Value: `"750000"`},
		{Key: "claim_proof_status_int", Value: `0`},
		{Key: "supplier_operator_address", Value: `"pokt1opold"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.tokenomics.EventSupplierSlashed", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent without stake_after_slash: %v", err)
	}
	if got == nil || got.Slashed == nil {
		t.Fatalf("want Slashed non-nil, got %+v", got)
	}
	ev := got.Slashed
	if ev.SupplierStakeAfterSlash != "" {
		t.Errorf("SupplierStakeAfterSlash = %q, want \"\" when absent", ev.SupplierStakeAfterSlash)
	}
	if ev.SupplierOperatorAddress != "pokt1opold" {
		t.Errorf("SupplierOperatorAddress = %q, want \"pokt1opold\"", ev.SupplierOperatorAddress)
	}
}

// TestDecodeSupplierEventSlashedRoundtripProto verifies the v0_1_34 decoder
// against a proto-marshaled EventSupplierSlashed (not just ABCI attrs), using
// the generated type directly to catch tag-mapping regressions.
func TestDecodeSupplierEventSlashedRoundtripProto(t *testing.T) {
	in := &tokenomics.EventSupplierSlashed{
		ProofMissingPenalty:     "2500upokt",
		ServiceId:               "polygon",
		ApplicationAddress:      "pokt1apptest",
		SessionEndBlockHeight:   790000,
		ClaimProofStatusInt:     2,
		SupplierOperatorAddress: "pokt1optest",
		SupplierStakeAfterSlash: "97500upokt",
	}
	// Encode as ABCI attrs (the real ingestion path for typed events).
	attrs := []types.EventAttr{
		{Key: "proof_missing_penalty", Value: `"` + in.ProofMissingPenalty + `"`},
		{Key: "service_id", Value: `"` + in.ServiceId + `"`},
		{Key: "application_address", Value: `"` + in.ApplicationAddress + `"`},
		{Key: "session_end_block_height", Value: `"790000"`},
		{Key: "claim_proof_status_int", Value: `2`},
		{Key: "supplier_operator_address", Value: `"` + in.SupplierOperatorAddress + `"`},
		{Key: "supplier_stake_after_slash", Value: `"` + in.SupplierStakeAfterSlash + `"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.tokenomics.EventSupplierSlashed", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent roundtrip: %v", err)
	}
	ev := got.Slashed
	if ev.SupplierStakeAfterSlash != in.SupplierStakeAfterSlash {
		t.Errorf("SupplierStakeAfterSlash = %q, want %q", ev.SupplierStakeAfterSlash, in.SupplierStakeAfterSlash)
	}
	if ev.SessionEndBlockHeight != in.SessionEndBlockHeight {
		t.Errorf("SessionEndBlockHeight = %d, want %d", ev.SessionEndBlockHeight, in.SessionEndBlockHeight)
	}
	if ev.ClaimProofStatusInt != in.ClaimProofStatusInt {
		t.Errorf("ClaimProofStatusInt = %d, want %d", ev.ClaimProofStatusInt, in.ClaimProofStatusInt)
	}
}

// TestDecodeSupplierEventSlashedMalformedJSON verifies that malformed JSON in attrs
// surfaces an error rather than returning a partial result.
func TestDecodeSupplierEventSlashedMalformedJSON(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_block_height", Value: `"not-a-number"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.tokenomics.EventSupplierSlashed", attrs)
	if err == nil {
		t.Fatalf("expected error for malformed session_end_block_height, got %+v", got)
	}
}

// ---------------------------------------------------------------------------
// DecodeSupplierEvent — delegation paths (non-slashed events)
// ---------------------------------------------------------------------------

// TestDecodeSupplierEventStakedDelegatesV034 verifies that v0_1_34 correctly
// delegates EventSupplierStaked to the v0_1_27 range owner.
func TestDecodeSupplierEventStakedDelegatesV034(t *testing.T) {
	attrs := []types.EventAttr{
		{Key: "session_end_height", Value: `"800100"`},
		{Key: "operator_address", Value: `"pokt1operator34"`},
	}
	got, err := Decoder{}.DecodeSupplierEvent("pocket.supplier.EventSupplierStaked", attrs)
	if err != nil {
		t.Fatalf("DecodeSupplierEvent delegation Staked: %v", err)
	}
	if got == nil || got.Staked == nil {
		t.Fatalf("want Staked non-nil, got %+v", got)
	}
	if got.Staked.OperatorAddress != "pokt1operator34" {
		t.Errorf("OperatorAddress = %q, want \"pokt1operator34\"", got.Staked.OperatorAddress)
	}
}
