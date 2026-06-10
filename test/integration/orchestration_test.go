//go:build integration

package integration

import (
	"context"
	"slices"
	"testing"
)

// genesisV0_1_0 is the mainnet genesis decoder version, in decoder-dir
// spelling on purpose — exercises protover normalization at every call site.
const genesisV0_1_0 = "v0_1_0"

func requiredSet(t *testing.T, h int64, genesis string) []string {
	t.Helper()
	s := storeFrom(t)
	names, err := s.RequiredSet(context.Background(), h, genesis)
	if err != nil {
		t.Fatalf("RequiredSet(%d): %v", h, err)
	}
	return names
}

func TestDynamicRequiredSetPerHeight(t *testing.T) { // spec test 23 (§11.1)
	pg.Reset(t)
	s := storeFrom(t)
	mustRegister(t, s, "blocklike", "v0.1.0")
	mustRegister(t, s, "late", "v0.1.20")
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	// H < first_valid: the late consumer is NOT in required_set…
	if got := requiredSet(t, 135296, genesisV0_1_0); !slices.Equal(got, []string{"blocklike"}) {
		t.Fatalf("required_set(135296) = %v, want [blocklike]", got)
	}
	// …and H ≥ first_valid: it is.
	if got := requiredSet(t, 135297, genesisV0_1_0); !slices.Equal(got, []string{"blocklike", "late"}) {
		t.Fatalf("required_set(135297) = %v, want [blocklike late]", got)
	}

	// Sealing follows: H seals WITHOUT the late consumer below its first_valid…
	setConsolidation(t, "blocklike", 200000)
	assertSealed(t, s, 135296, genesisV0_1_0, true)
	// …but not at/after it until the late consumer catches up.
	assertSealed(t, s, 135297, genesisV0_1_0, false)
	setConsolidation(t, "late", 135297)
	assertSealed(t, s, 135297, genesisV0_1_0, true)
}
