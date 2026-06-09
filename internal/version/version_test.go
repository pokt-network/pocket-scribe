package version

import (
	"strings"
	"testing"
)

func TestStringContainsAllFields(t *testing.T) {
	Version, Commit, Date = "v1.2.3", "abc1234", "2026-06-08T00:00:00Z"
	got := String()
	for _, want := range []string{"v1.2.3", "abc1234", "2026-06-08T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, missing %q", got, want)
		}
	}
}

func TestDefaults(t *testing.T) {
	if Version == "" || Commit == "" || Date == "" {
		t.Fatal("version vars must have non-empty defaults")
	}
}
