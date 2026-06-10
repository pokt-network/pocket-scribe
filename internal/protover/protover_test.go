package protover

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{in: "v0.1.30", want: "v0.1.30"},
		{in: "v0_1_30", want: "v0.1.30"}, // decoder-dir spelling accepted
		{in: " v0.1.0 ", want: "v0.1.0"}, // boundary trims whitespace
		// x/mod/semver accepts two-component versions; Canonical pads the
		// patch. Intentional leniency — poktroll tags are always 3-component.
		{in: "v0.1", want: "v0.1.0"},
		{in: "0.1.30", wantErr: true}, // missing v prefix → invalid
		{in: "v0.1.x", wantErr: true},
		{in: "", wantErr: true},
		{in: "garbage", wantErr: true},
	}
	for _, c := range cases {
		got, err := Normalize(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("Normalize(%q): want error, got %q", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Normalize(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCompareSemverNotLexicographic(t *testing.T) {
	// The spec §4.10 callout: "v0.1.10" > "v0.1.9" (lexicographic gets this wrong).
	if Compare("v0.1.10", "v0.1.9") <= 0 {
		t.Fatal("v0.1.10 must compare greater than v0.1.9")
	}
	if Compare("v0.1.20", "v0.1.20") != 0 {
		t.Fatal("equal versions must compare 0")
	}
	if Compare("v0.1.2", "v0.2.0") >= 0 {
		t.Fatal("v0.1.2 must compare less than v0.2.0")
	}
}

func TestToDecoderDir(t *testing.T) {
	got, err := ToDecoderDir("v0.1.30")
	if err != nil || got != "v0_1_30" {
		t.Fatalf("ToDecoderDir(v0.1.30) = %q, %v; want v0_1_30", got, err)
	}
	if _, err := ToDecoderDir("nope"); err == nil {
		t.Fatal("ToDecoderDir must reject invalid input")
	}
}
