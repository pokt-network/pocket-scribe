package store

import (
	"math"
	"testing"
)

func TestFirstValidHeight(t *testing.T) {
	upgrades := map[string]int64{
		"v0.1.20": 135297,
		"v0.1.27": 247893,
	}
	cases := []struct {
		name, firstValid, genesis string
		ups                       map[string]int64
		want                      int64
		wantErr                   bool
	}{
		// V <= genesis → valid from height 1 (spec §4.10 first branch).
		{name: "equal to genesis", firstValid: "v0.1.0", genesis: "v0.1.0", ups: upgrades, want: 1},
		{name: "below genesis", firstValid: "v0.1.0", genesis: "v0_1_33", ups: nil, want: 1},
		{name: "underscored both", firstValid: "v0_1_20", genesis: "v0_1_33", ups: nil, want: 1},
		// V in upgrades → the applied height.
		{name: "upgrade member", firstValid: "v0.1.20", genesis: "v0.1.0", ups: upgrades, want: 135297},
		{name: "upgrade member underscored", firstValid: "v0_1_27", genesis: "v0_1_0", ups: upgrades, want: 247893},
		// V > genesis and not applied → dormant.
		{name: "dormant", firstValid: "v0.2.0", genesis: "v0.1.0", ups: upgrades, want: DormantHeight},
		{name: "dormant empty upgrades", firstValid: "v0.1.20", genesis: "v0.1.0", ups: nil, want: DormantHeight},
		// Error paths (real, not padding).
		{name: "bad first_valid", firstValid: "nope", genesis: "v0.1.0", ups: nil, wantErr: true},
		{name: "bad genesis", firstValid: "v0.1.0", genesis: "nope", ups: nil, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := firstValidHeight(c.firstValid, c.genesis, c.ups)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}
}

func TestDormantHeightIsMaxInt64(t *testing.T) {
	// The sentinel must be unreachable by any real chain height.
	if DormantHeight != math.MaxInt64 {
		t.Fatalf("DormantHeight = %d", DormantHeight)
	}
}
