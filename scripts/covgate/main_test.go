package main

import (
	"strings"
	"testing"
)

// syntheticProfile is a 5-line cover profile:
//   - one decoder file fully covered (all 10 stmts hit)
//   - one internal file at 50% (4 of 8 stmts hit: 2 covered blocks + 2 zero blocks)
const syntheticProfile = `mode: atomic
github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/decoder.go:5.1,10.2 10 1
github.com/pokt-network/pocketscribe/internal/consumer/batch.go:20.1,22.2 4 1
github.com/pokt-network/pocketscribe/internal/consumer/batch.go:25.1,27.2 4 1
github.com/pokt-network/pocketscribe/internal/consumer/batch.go:30.1,32.2 4 0
github.com/pokt-network/pocketscribe/internal/consumer/batch.go:35.1,37.2 4 0
`

func TestParseProfile_AggregatesCorrectly(t *testing.T) {
	pkgs := parseProfile(strings.NewReader(syntheticProfile))

	// Decoder package: 10 stmts, all covered.
	dec := pkgs["internal/decoders/v0_1_0"]
	if dec == nil {
		t.Fatal("decoder package missing from parsed output")
	}
	if dec.stmts != 10 {
		t.Errorf("decoder stmts = %d, want 10", dec.stmts)
	}
	if dec.covered != 10 {
		t.Errorf("decoder covered = %d, want 10", dec.covered)
	}

	// Consumer package: 16 stmts total (4+4+4+4), 8 covered (4+4 from count>0 blocks).
	con := pkgs["internal/consumer"]
	if con == nil {
		t.Fatal("consumer package missing from parsed output")
	}
	if con.stmts != 16 {
		t.Errorf("consumer stmts = %d, want 16", con.stmts)
	}
	if con.covered != 8 {
		t.Errorf("consumer covered = %d, want 8", con.covered)
	}
}

func TestReport_ReturnsTrueWhenBelowBar(t *testing.T) {
	pkgs := parseProfile(strings.NewReader(syntheticProfile))

	var out strings.Builder
	failed := report(pkgs, &out)
	if !failed {
		t.Error("report returned false (no failure), want true (consumer at 50% < 90%)")
	}

	output := out.String()
	// Decoder should be at 100% and ok.
	if !strings.Contains(output, "internal/decoders/v0_1_0") {
		t.Error("decoder package missing from report output")
	}
	if !strings.Contains(output, "100.0%") {
		t.Error("decoder should report 100.0%")
	}
	// Consumer should be at 50% and FAIL.
	if !strings.Contains(output, "FAIL") {
		t.Error("expected FAIL in report output for consumer at 50%")
	}
}

func TestReport_SkipsZeroStatementPackages(t *testing.T) {
	// A package with only zero-statement blocks should be skipped entirely.
	zeroProfile := `mode: atomic
github.com/pokt-network/pocketscribe/internal/version/version.go:1.1,2.2 0 0
github.com/pokt-network/pocketscribe/internal/consumer/batch.go:5.1,7.2 5 5
`
	pkgs := parseProfile(strings.NewReader(zeroProfile))
	var out strings.Builder
	report(pkgs, &out)

	// version package has 0 stmts (the block says numStmt=0) — should not appear.
	if strings.Contains(out.String(), "internal/version") {
		t.Error("zero-statement package should be skipped in report")
	}
}

func TestParseProfile_ExcludesGenAndProto(t *testing.T) {
	excludeProfile := `mode: atomic
github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/types.pb.go:5.1,7.2 3 3
github.com/pokt-network/pocketscribe/internal/proto/gen/envelope.pb.go:5.1,7.2 3 3
github.com/pokt-network/pocketscribe/internal/proto/something.go:5.1,7.2 3 3
github.com/pokt-network/pocketscribe/cmd/ps/main.go:5.1,7.2 5 5
`
	pkgs := parseProfile(strings.NewReader(excludeProfile))
	// gen/ directories and internal/proto must be excluded.
	for pkg := range pkgs {
		if hasSegment(pkg, "gen") {
			t.Errorf("gen/ package should be excluded: %s", pkg)
		}
		if pkg == "internal/proto" {
			t.Errorf("internal/proto should be excluded: %s", pkg)
		}
		if strings.HasPrefix(pkg, "cmd/") {
			t.Errorf("cmd/ package should be excluded: %s", pkg)
		}
	}
}

func TestParseProfile_ExcludesAppCompositionRoots(t *testing.T) {
	// internal/app and internal/app/* are composition roots: excluded from the
	// gate (integration-covered wiring). A sibling like internal/application
	// must NOT be excluded (prefix is segment-exact, not substring).
	profile := `mode: atomic
github.com/pokt-network/pocketscribe/internal/app/root.go:5.1,7.2 4 0
github.com/pokt-network/pocketscribe/internal/app/x/cmd.go:5.1,7.2 6 0
github.com/pokt-network/pocketscribe/internal/application/thing.go:5.1,7.2 3 3
`
	pkgs := parseProfile(strings.NewReader(profile))
	if _, ok := pkgs["internal/app"]; ok {
		t.Error("internal/app should be excluded from the gate")
	}
	if _, ok := pkgs["internal/app/x"]; ok {
		t.Error("internal/app/x should be excluded from the gate")
	}
	a := pkgs["internal/application"]
	if a == nil || a.stmts != 3 {
		t.Fatalf("internal/application must NOT be excluded (not under internal/app/): %+v", a)
	}
	if len(pkgs) != 1 {
		t.Fatalf("want 1 package, got %d", len(pkgs))
	}
}

func TestParseProfile_GenExclusionIsExactSegment(t *testing.T) {
	// A package merely CONTAINING "gen" in a segment name must NOT be excluded;
	// a malformed line without ':' must be skipped, not panic.
	profile := `mode: atomic
github.com/pokt-network/pocketscribe/internal/generic/thing.go:5.1,7.2 4 4
malformed-line-without-colon 4 4
`
	pkgs := parseProfile(strings.NewReader(profile))
	a := pkgs["internal/generic"]
	if a == nil || a.stmts != 4 || a.covered != 4 {
		t.Fatalf("internal/generic should be counted (not excluded as gen/): %+v", a)
	}
	if len(pkgs) != 1 {
		t.Fatalf("want 1 package, got %d", len(pkgs))
	}
}
