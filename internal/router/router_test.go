package router

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_10 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10"
	v0_1_20 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_20"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	v0_1_29 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_29"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
)

func TestDecoderForBoundaries(t *testing.T) {
	reg := map[string]decoders.Decoder{
		"v0_1_0": v0_1_0.Decoder{}, "v0_1_10": v0_1_10.Decoder{},
		"v0_1_20": v0_1_20.Decoder{}, "v0_1_28": v0_1_28.Decoder{}, "v0_1_29": v0_1_29.Decoder{},
	}
	r, err := NewStaticRouter([]Upgrade{
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.20", AppliedAtHeight: 135297, DecoderVersion: "v0_1_20"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
		{Name: "v0.1.29", AppliedAtHeight: 382250, DecoderVersion: "v0_1_29"},
	}, reg, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		h    int64
		want string
	}{
		{1, "v0_1_0"}, {78682, "v0_1_0"}, {78683, "v0_1_10"},
		{135296, "v0_1_10"}, {135297, "v0_1_20"},
		{287932, "v0_1_28"}, {382250, "v0_1_29"}, {999999, "v0_1_29"},
	}
	for _, tc := range cases {
		d, err := r.DecoderFor(tc.h)
		if err != nil {
			t.Fatalf("DecoderFor(%d): %v", tc.h, err)
		}
		if d.Version() != tc.want {
			t.Fatalf("DecoderFor(%d) = %s, want %s", tc.h, d.Version(), tc.want)
		}
	}
}

// TestDecoderForFallsBackToEarlierRegistered verifies the LENIENT router:
// an upgrade entry whose decoder_version is not in the registry is silently
// skipped and the nearest earlier registered version is returned.
// Here v0_1_31 is in the upgrades list at height 635506 but NOT in the registry;
// the router must return the v0_1_30 decoder (the nearest earlier registered).
func TestDecoderForFallsBackToEarlierRegistered(t *testing.T) {
	reg := map[string]decoders.Decoder{
		"v0_1_30": v0_1_30.Decoder{},
	}
	r, err := NewStaticRouter([]Upgrade{
		{Name: "v0.1.31", AppliedAtHeight: 635506, DecoderVersion: "v0_1_31"}, // NOT in registry
	}, reg, "v0_1_30")
	if err != nil {
		t.Fatal(err)
	}
	d, err := r.DecoderFor(635506)
	if err != nil {
		t.Fatalf("DecoderFor(635506): %v", err)
	}
	if d.Version() != "v0_1_30" {
		t.Fatalf("expected fallback to v0_1_30, got %s", d.Version())
	}
}

// TestNewStaticRouterRejectsEmptyRegistry verifies the only construction-time
// hard failure: a completely empty registry (nothing to fall back to).
func TestNewStaticRouterRejectsEmptyRegistry(t *testing.T) {
	_, err := NewStaticRouter(nil, map[string]decoders.Decoder{}, "v0_1_0")
	if err == nil {
		t.Fatal("expected error: empty decoder registry")
	}
}
