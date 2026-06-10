package router

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_10 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_10"
	v0_1_20 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_20"
	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	v0_1_29 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_29"
	v0_1_30 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_30"
	v0_1_8 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_8"
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

// TestDecoderForShapeBreakEras pins the Phase E shape-complete registry: the
// v0.1.8/v0.1.9 and v0.1.27 eras must resolve to their OWN range decoders, not
// fall back across a supplier shape boundary (mainnet applied heights from
// docs/research/poktroll-versions.md).
func TestDecoderForShapeBreakEras(t *testing.T) {
	ups := []Upgrade{
		{Name: "v0.1.8", AppliedAtHeight: 78671, DecoderVersion: "v0_1_8"},
		{Name: "v0.1.9", AppliedAtHeight: 78677, DecoderVersion: "v0_1_9"}, // unregistered → falls back to v0_1_8 (same range)
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
		{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
		{Name: "v0.1.28", AppliedAtHeight: 287932, DecoderVersion: "v0_1_28"},
	}
	r, err := NewStaticRouter(ups, DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}
	cases := []struct {
		height int64
		want   string
	}{
		{78670, "v0_1_0"},   // pre-v0.1.8 era
		{78671, "v0_1_8"},   // v0.1.8 boundary
		{78680, "v0_1_8"},   // v0.1.9 era → nearest registered earlier = v0_1_8 (shape-correct)
		{247893, "v0_1_27"}, // v0.1.27 boundary — previously fell back to v0_1_20 (WRONG events)
		{287931, "v0_1_27"},
		{287932, "v0_1_28"},
	}
	for _, c := range cases {
		d, err := r.DecoderFor(c.height)
		if err != nil {
			t.Fatalf("DecoderFor(%d): %v", c.height, err)
		}
		if d.Version() != c.want {
			t.Errorf("DecoderFor(%d) = %s, want %s", c.height, d.Version(), c.want)
		}
	}
}

// TestDecoderForGenesisVersionUnregisteredFallsBackToUpgrades verifies that
// when genesisVersion is NOT in the registry, DecoderFor falls back to the
// earliest registered version found in the upgrades list.
// This exercises the second fallback loop (lines 86-90 in router.go).
func TestDecoderForGenesisVersionUnregisteredFallsBackToUpgrades(t *testing.T) {
	reg := map[string]decoders.Decoder{
		"v0_1_10": v0_1_10.Decoder{},
	}
	// genesisVersion "v0_1_0" is NOT in the registry; v0_1_10 IS.
	r, err := NewStaticRouter([]Upgrade{
		{Name: "v0.1.10", AppliedAtHeight: 78683, DecoderVersion: "v0_1_10"},
	}, reg, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	// height 1 is before any upgrade — genesis version is unregistered,
	// so the first pass yields no chosen decoder; the fallback loop must find v0_1_10.
	d, err := r.DecoderFor(1)
	if err != nil {
		t.Fatalf("DecoderFor(1): %v", err)
	}
	if d.Version() != "v0_1_10" {
		t.Fatalf("expected fallback to v0_1_10, got %s", d.Version())
	}
}

var _ = v0_1_8.Decoder{}  // ensure import is used
var _ = v0_1_27.Decoder{} // ensure import is used
