package v0_1_27

// Seam-override tests for the defensive MarshalJSONPBSlice guards.
// jsonpb cannot fail for the concrete types this package marshals, so the
// guards are unreachable with real inputs; the package-level seams let these
// tests prove that IF a future shape introduces a failing marshal (e.g. an
// Any field), every decode path propagates the error to the caller instead
// of panicking or writing partial rows.

import (
	"errors"
	"testing"

	"cosmossdk.io/math"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"

	shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/shared"
	supplier "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
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

// TestSeamMsgStakeServicesMarshalError verifies DecodeSupplierMsg propagates a
// services-JSON marshal failure to the caller (no partial result).
func TestSeamMsgStakeServicesMarshalError(t *testing.T) {
	failServiceConfigsSeam(t)
	in := &supplier.MsgStakeSupplier{
		OperatorAddress: "pokt1op27",
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
		OperatorAddress: "pokt1op27",
		Services:        []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op27/"), raw, false)
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
		OperatorAddress: "pokt1op27",
		Services:        []*shared.SupplierServiceConfig{{ServiceId: "eth"}},
		ServiceConfigHistory: []*shared.ServiceConfigUpdate{{
			OperatorAddress:  "pokt1op27",
			ActivationHeight: 5,
			Service:          &shared.SupplierServiceConfig{ServiceId: "eth"},
		}},
	}
	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Decoder{}.DecodeSupplierKV([]byte("Supplier/operator_address/pokt1op27/"), raw, false)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
}

// TestSeamKVSCUServiceMarshalError verifies DecodeSupplierKV propagates a
// Service marshal failure on the non-deleted SCU record path.
func TestSeamKVSCUServiceMarshalError(t *testing.T) {
	failServiceConfigsSeam(t)
	scu := &shared.ServiceConfigUpdate{
		OperatorAddress:  "pokt1op27",
		ActivationHeight: 1000,
		Service:          &shared.SupplierServiceConfig{ServiceId: "anvil"},
	}
	raw, err := scu.Marshal()
	if err != nil {
		t.Fatalf("marshal SCU: %v", err)
	}
	actBytes := []byte{0, 0, 0, 0, 0, 0, 3, 232} // height 1000 big-endian
	key := append([]byte("ServiceConfigUpdate/service_id/anvil/"), actBytes...)
	key = append(key, '/')
	key = append(key, []byte("pokt1op27/")...)

	got, err := Decoder{}.DecodeSupplierKV(key, raw, false)
	if got != nil {
		t.Fatalf("want nil result on marshal failure, got %+v", got)
	}
	if !errors.Is(err, errSeam) {
		t.Fatalf("caller must receive the marshal error, got %v", err)
	}
}
