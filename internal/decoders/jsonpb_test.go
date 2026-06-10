package decoders

import (
	"bytes"
	"strings"
	"testing"

	gogotypes "github.com/cosmos/gogoproto/types"

	sh8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/shared"
	sup8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8/gen/pocket/supplier"
)

// ---------------------------------------------------------------------------
// MarshalJSONPBSlice
// ---------------------------------------------------------------------------

// TestMarshalJSONPBSliceNilOnEmpty verifies the contract: an empty slice
// returns nil (not an empty-array []byte), so dehydrated Supplier rows with no
// services receive a SQL NULL rather than "[]".
func TestMarshalJSONPBSliceNilOnEmpty(t *testing.T) {
	got, err := MarshalJSONPBSlice([]*sh8.SupplierServiceConfig{})
	if err != nil {
		t.Fatalf("MarshalJSONPBSlice empty: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for empty slice, got %s", got)
	}
}

// TestMarshalJSONPBSliceSingleElement verifies that a one-item slice is
// rendered as a JSON array containing the single object.
func TestMarshalJSONPBSliceSingleElement(t *testing.T) {
	cfg := &sh8.SupplierServiceConfig{
		ServiceId: "eth",
		Endpoints: []*sh8.SupplierEndpoint{{
			Url:     "https://example.com",
			RpcType: sh8.RPCType_JSON_RPC,
		}},
	}
	got, err := MarshalJSONPBSlice([]*sh8.SupplierServiceConfig{cfg})
	if err != nil {
		t.Fatalf("MarshalJSONPBSlice: %v", err)
	}
	if !bytes.HasPrefix(got, []byte("[")) || !bytes.HasSuffix(got, []byte("]")) {
		t.Fatalf("expected JSON array, got: %s", got)
	}
	if !bytes.Contains(got, []byte(`"service_id":"eth"`)) {
		t.Fatalf("service_id missing from output: %s", got)
	}
	if !bytes.Contains(got, []byte("JSON_RPC")) {
		t.Fatalf("RPC type missing from output: %s", got)
	}
}

// TestMarshalJSONPBSliceMultipleElements verifies that multiple items produce a
// comma-separated JSON array (the "[" + join + "]" assembly path).
func TestMarshalJSONPBSliceMultipleElements(t *testing.T) {
	cfgs := []*sh8.SupplierServiceConfig{
		{ServiceId: "eth"},
		{ServiceId: "sol"},
	}
	got, err := MarshalJSONPBSlice(cfgs)
	if err != nil {
		t.Fatalf("MarshalJSONPBSlice: %v", err)
	}
	if !bytes.Contains(got, []byte(`"eth"`)) || !bytes.Contains(got, []byte(`"sol"`)) {
		t.Fatalf("missing service ids in output: %s", got)
	}
}

// TestMarshalJSONPBSliceUsesOrigName verifies OrigName=true behaviour: the JSON
// keys must use snake_case proto field names, not camelCase Go names.
// service_id → "service_id" (not "serviceId").
func TestMarshalJSONPBSliceUsesOrigName(t *testing.T) {
	cfg := &sh8.SupplierServiceConfig{ServiceId: "anvil"}
	got, err := MarshalJSONPBSlice([]*sh8.SupplierServiceConfig{cfg})
	if err != nil {
		t.Fatalf("MarshalJSONPBSlice: %v", err)
	}
	if bytes.Contains(got, []byte("serviceId")) {
		t.Fatalf("camelCase key found — OrigName=true not respected: %s", got)
	}
	if !bytes.Contains(got, []byte("service_id")) {
		t.Fatalf("snake_case key not found: %s", got)
	}
}

// TestMarshalJSONPBSliceErrorPropagation verifies that a marshaling failure
// inside MarshalJSONPBSlice (and marshalJSONPB) is surfaced as a non-nil error.
// A gogo types.Any with an unresolvable type_url triggers the jsonpb error path
// because the jsonpb Marshaler resolves Any values via the proto registry —
// the stripped gen init() leaves no registration for "/nonexistent.Type".
func TestMarshalJSONPBSliceErrorPropagation(t *testing.T) {
	bad := []*gogotypes.Any{{TypeUrl: "/nonexistent.Type", Value: []byte{0x01}}}
	_, err := MarshalJSONPBSlice(bad)
	if err == nil {
		t.Fatal("expected error for Any with unresolvable type_url")
	}
	if !strings.Contains(err.Error(), "jsonpb") {
		t.Fatalf("error should mention jsonpb: %v", err)
	}
}

// ---------------------------------------------------------------------------
// UnmarshalEventJSON
// ---------------------------------------------------------------------------

// TestUnmarshalEventJSONHappyPath verifies that a valid JSON document round-trips
// into the typed event struct.
func TestUnmarshalEventJSONHappyPath(t *testing.T) {
	doc := []byte(`{"session_end_height":"135840","supplier":{"owner_address":"pokt1owner","operator_address":"pokt1op","stake":{"denom":"upokt","amount":"60000000000"},"services":[]}}`)
	var ev sup8.EventSupplierStaked
	if err := UnmarshalEventJSON(doc, &ev); err != nil {
		t.Fatalf("UnmarshalEventJSON: %v", err)
	}
	if ev.SessionEndHeight != 135840 {
		t.Fatalf("SessionEndHeight = %d, want 135840", ev.SessionEndHeight)
	}
}

// TestUnmarshalEventJSONAllowsUnknownFields verifies AllowUnknownFields=true:
// a JSON key not in the proto schema should NOT cause an error (future-proofing
// for new chain fields that arrive before a decoder update).
func TestUnmarshalEventJSONAllowsUnknownFields(t *testing.T) {
	doc := []byte(`{"session_end_height":"100","future_field":"ignored","another_new_field":42}`)
	var ev sup8.EventSupplierStaked
	if err := UnmarshalEventJSON(doc, &ev); err != nil {
		t.Fatalf("UnmarshalEventJSON rejected unknown field: %v", err)
	}
	if ev.SessionEndHeight != 100 {
		t.Fatalf("SessionEndHeight = %d, want 100", ev.SessionEndHeight)
	}
}

// TestUnmarshalEventJSONMalformedJSON verifies that syntactically invalid JSON
// returns a non-nil error (the marshal error is wrapped and surfaced).
func TestUnmarshalEventJSONMalformedJSON(t *testing.T) {
	if err := UnmarshalEventJSON([]byte(`{broken json`), &sup8.EventSupplierStaked{}); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

// TestUnmarshalEventJSONBadEnumValue verifies that a JSON string value that
// does not map to any known enum name produces an error (enum validation path).
func TestUnmarshalEventJSONBadEnumValue(t *testing.T) {
	// SupplierUnbondingReason has known values; "INVALID_REASON_XYZ" is not one.
	doc := []byte(`{"reason":"INVALID_REASON_XYZ","session_end_height":"100","unbonding_end_height":"200"}`)
	var ev sup8.EventSupplierUnbondingBegin
	err := UnmarshalEventJSON(doc, &ev)
	if err == nil {
		t.Fatal("expected error for unrecognised enum name")
	}
	if !strings.Contains(err.Error(), "jsonpb") {
		t.Fatalf("error should mention jsonpb: %v", err)
	}
}
