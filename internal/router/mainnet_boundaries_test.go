package router_test

import (
	"testing"

	"github.com/pokt-network/pocketscribe/internal/fixturereport"
	"github.com/pokt-network/pocketscribe/internal/router"
)

// TestDecoderForAllMainnetBoundaries pins DecoderFor at EVERY mainnet upgrade
// boundary (±1) against the era expectations machine-derived from the break
// map (docs/research/supplier-shape-breaks.md): breaks at v0_1_8 and v0_1_27;
// v0_1_0/10/20/28/29/30 registered as range anchors. Spec test 15 extended to
// the full table (spec §9 Phase F).
func TestDecoderForAllMainnetBoundaries(t *testing.T) {
	r, err := router.NewStaticRouter(fixturereport.MainnetUpgrades(), router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	// expected decoder version AT the boundary (and until the next boundary).
	eras := []struct {
		name   string
		height int64
		want   string
	}{
		{"genesis", 1, "v0_1_0"},
		{"v0.1.2", 78621, "v0_1_0"}, {"v0.1.3", 78632, "v0_1_0"}, {"v0.1.4", 78641, "v0_1_0"},
		{"v0.1.5", 78654, "v0_1_0"}, {"v0.1.6", 78659, "v0_1_0"}, {"v0.1.7", 78665, "v0_1_0"},
		{"v0.1.8", 78671, "v0_1_8"}, {"v0.1.9", 78678, "v0_1_8"},
		{"v0.1.10", 78683, "v0_1_10"}, {"v0.1.11", 78689, "v0_1_10"}, {"v0.1.12", 78697, "v0_1_10"},
		{"v0.1.13", 80510, "v0_1_10"}, {"v0.1.14", 93825, "v0_1_10"}, {"v0.1.15", 94370, "v0_1_10"},
		{"v0.1.16", 99293, "v0_1_10"}, {"v0.1.17", 102142, "v0_1_10"}, {"v0.1.18", 116100, "v0_1_10"},
		{"v0.1.19", 117454, "v0_1_10"},
		{"v0.1.20", 135297, "v0_1_20"}, {"v0.1.21", 138931, "v0_1_20"}, {"v0.1.22", 155173, "v0_1_20"},
		{"v0.1.23", 161109, "v0_1_20"}, {"v0.1.24", 161169, "v0_1_20"}, {"v0.1.25", 190974, "v0_1_20"},
		{"v0.1.26", 190979, "v0_1_20"},
		{"v0.1.27", 247893, "v0_1_27"},
		{"v0.1.28", 287932, "v0_1_28"},
		{"v0.1.29", 382250, "v0_1_29"},
		{"v0.1.30", 484473, "v0_1_30"}, {"v0.1.31", 635506, "v0_1_30"}, {"v0.1.33", 703870, "v0_1_30"},
		{"v0.1.34", 788945, "v0_1_34"},
	}
	for i, e := range eras {
		dec, err := r.DecoderFor(e.height)
		if err != nil {
			t.Fatalf("%s @%d: %v", e.name, e.height, err)
		}
		if dec.Version() != e.want {
			t.Errorf("%s @%d: decoder %s, want %s", e.name, e.height, dec.Version(), e.want)
		}
		// One height BEFORE each boundary must resolve to the PREVIOUS era.
		if i > 0 {
			prev := eras[i-1] //nolint:gosec // G602 false positive: guarded by i>0 above
			dec, err := r.DecoderFor(e.height - 1)
			if err != nil {
				t.Fatalf("%s @%d-1: %v", e.name, e.height, err)
			}
			if dec.Version() != prev.want {
				t.Errorf("@%d (just below %s): decoder %s, want %s", e.height-1, e.name, dec.Version(), prev.want)
			}
		}
	}
}
