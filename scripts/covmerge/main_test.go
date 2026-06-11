package main

import (
	"strings"
	"testing"
)

func TestMergeReader_SumsCountsPerBlock(t *testing.T) {
	// Two profiles with overlapping blocks: counts should be summed.
	prof1 := strings.NewReader("mode: atomic\n" +
		"github.com/pokt-network/pocketscribe/internal/foo/foo.go:10.2,12.3 2 1\n" +
		"github.com/pokt-network/pocketscribe/internal/foo/foo.go:15.1,16.2 1 0\n")
	prof2 := strings.NewReader("mode: atomic\n" +
		"github.com/pokt-network/pocketscribe/internal/foo/foo.go:10.2,12.3 2 3\n" +
		"github.com/pokt-network/pocketscribe/internal/bar/bar.go:5.1,7.2 3 2\n")

	counts := map[string]int64{}
	order := []string{}
	if err := mergeReader(prof1, counts, &order); err != nil {
		t.Fatalf("mergeReader prof1: %v", err)
	}
	if err := mergeReader(prof2, counts, &order); err != nil {
		t.Fatalf("mergeReader prof2: %v", err)
	}

	// Block appearing in both files: counts summed (1+3=4).
	overlapKey := "github.com/pokt-network/pocketscribe/internal/foo/foo.go:10.2,12.3 2"
	if got := counts[overlapKey]; got != 4 {
		t.Errorf("overlap block count = %d, want 4", got)
	}
	// Block only in prof1, count 0.
	zeroKey := "github.com/pokt-network/pocketscribe/internal/foo/foo.go:15.1,16.2 1"
	if got := counts[zeroKey]; got != 0 {
		t.Errorf("zero block count = %d, want 0", got)
	}
	// Block only in prof2, count 2.
	barKey := "github.com/pokt-network/pocketscribe/internal/bar/bar.go:5.1,7.2 3"
	if got := counts[barKey]; got != 2 {
		t.Errorf("bar block count = %d, want 2", got)
	}
	// Order: 3 unique keys, first occurrence order.
	if len(order) != 3 {
		t.Errorf("order len = %d, want 3", len(order))
	}
}
