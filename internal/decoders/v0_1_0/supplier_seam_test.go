package v0_1_0

// Seam-override tests for the defensive MarshalJSONPBSlice guards.
// jsonpb cannot fail for the concrete types this package marshals, so the
// guards are unreachable with real inputs; the package-level seams let these
// tests prove that IF a future shape introduces a failing marshal (e.g. an
// Any field), every decode path propagates the error to the caller instead
// of panicking or writing partial rows.

import (
	"errors"
	"strings"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/supplier"
)

var errSeam = errors.New("seam: jsonpb marshal failed")

// failServiceConfigsSeam overrides the SupplierServiceConfig marshal seam to
// always fail; restored via t.Cleanup.
func failServiceConfigsSeam(t *testing.T) {
	t.Helper()
	orig := marshalServiceConfigsJSONPB
	marshalServiceConfigsJSONPB = func([]*shared.SupplierServiceConfig) ([]byte, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { marshalServiceConfigsJSONPB = orig })
}

// failSCUsSeam overrides the ServiceConfigUpdate marshal seam to always fail;
// restored via t.Cleanup.
func failSCUsSeam(t *testing.T) {
	t.Helper()
	orig := marshalSCUsJSONPB
	marshalSCUsJSONPB = func([]*shared.ServiceConfigUpdate) ([]byte, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { marshalSCUsJSONPB = orig })
}

// scuKey builds a valid SCU primary key (v0_1_8 layout, which
// ParseSCUPrimaryKey assumes): service anvil, activation 1000, op pokt1op0.
func scuKey() []byte {
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // height 1000 big-endian
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op0/")...)
	return key
}

// TestSeamMsgStakeServicesMarshalError verifies DecodeSupplierMsg propagates a
// services-JSON marshal failure to the caller (no partial result).
func TestSeamMsgStakeServicesMarshalError(t *testing.T) {
	failServiceConfigsSeam(t)
	in := &supplier.MsgStakeSupplier{
		OperatorAddress: "pokt1op0",
		Stake:           &cosmostypes.Coin{Denom: "upokt", Amount: math.NewInt(1)},
		Services:        []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierMsg("/pocket.supplier.MsgStakeSupplier", raw)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
}

// TestSeamKVSupplierServicesMarshalError verifies DecodeSupplierKV propagates
// a Services marshal failure on the Supplier record path.
func TestSeamKVSupplierServicesMarshalError(t *testing.T) {
	failServiceConfigsSeam(t)
	in := &shared.Supplier{
		OperatorAddress: "pokt1op0",
		Services:        []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op0/"), raw, false)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
}

// TestSeamKVSupplierHistoryMarshalError verifies DecodeSupplierKV propagates a
// ServiceConfigHistory marshal failure (Services marshal runs the real path).
func TestSeamKVSupplierHistoryMarshalError(t *testing.T) {
	failSCUsSeam(t)
	in := &shared.Supplier{
		OperatorAddress: "pokt1op0",
		Services:        []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
		ServiceConfigHistory: []*shared.ServiceConfigUpdate{{
			EffectiveBlockHeight: 5, // v0_1_0 SCU shape: Services + EffectiveBlockHeight
			Services:             []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
		}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op0/"), raw, false)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
}

// TestSeamKVSCUMarshalError verifies the SCU record path: marshalSCU wraps the
// seam failure with context and DecodeSupplierKV propagates it to the caller.
func TestSeamKVSCUMarshalError(t *testing.T) {
	failSCUsSeam(t)
	scu := &shared.ServiceConfigUpdate{
		EffectiveBlockHeight: 100,
		Services:             []*shared.SupplierServiceConfig{{ServiceId: "anvil"}},
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV(scuKey(), raw, false)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
	if !strings.Contains(err.Error(), "v0_1_0 ServiceConfigUpdate JSON") {
		t.Fatalf("marshalSCU must wrap with context, got %v", err)
	}
}

// TestSeamKVSCUShortMarshalOutput verifies the marshalSCU length guard: a
// degenerate (sub-2-byte) marshal output must surface a descriptive error to
// the caller, never a panic from the bracket-strip slice.
func TestSeamKVSCUShortMarshalOutput(t *testing.T) {
	orig := marshalSCUsJSONPB
	marshalSCUsJSONPB = func([]*shared.ServiceConfigUpdate) ([]byte, error) {
		return []byte("x"), nil // shorter than "[]" — impossible from the real marshaler
	}
	t.Cleanup(func() { marshalSCUsJSONPB = orig })

	scu := &shared.ServiceConfigUpdate{EffectiveBlockHeight: 100}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV(scuKey(), raw, false)
	if got != nil {
		t.Fatalf("want nil result on short marshal output, got %+v", got)
	}
	if err == nil || !strings.Contains(err.Error(), "unexpectedly short") {
		t.Fatalf("want 'unexpectedly short' error, got %v", err)
	}
}
