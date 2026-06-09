//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestE2EContiguousAdvance(t *testing.T) { // spec test 1 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")
	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, h.store, "noop-a", 5, 15*time.Second)
	if got := processedCount(t, "noop-a"); got != 5 {
		t.Fatalf("processed rows = %d, want 5", got)
	}
}

func TestE2EForcedGapRecordedThenFilled(t *testing.T) { // spec tests 2 + 10 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")

	// 1,2 then 4 — height 4 arrives while 3 is missing (out-of-order vs the gap).
	publishHeights(t, 1, 2, 4)
	waitCursor(t, h.store, "noop-a", 2, 15*time.Second)

	// Give the runtime time to process the out-of-order 4 and freeze.
	time.Sleep(750 * time.Millisecond)
	if cur, _ := h.store.ConsolidatedUpTo(context.Background(), "noop-a"); cur != 2 {
		t.Fatalf("cursor = %d, want frozen at 2 (gap at 3)", cur)
	}
	if got := testutil.ToFloat64(h.metrics.GapsTotal.WithLabelValues("noop-a")); got < 1 {
		t.Fatalf("expected a recorded gap, gaps_total = %v", got)
	}
	if ok, _ := h.store.HasProcessed(context.Background(), "noop-a", 4); !ok {
		t.Fatal("height 4 should be recorded despite the gap")
	}

	// Fill the gap → cursor jumps to 4.
	publishHeights(t, 3)
	waitCursor(t, h.store, "noop-a", 4, 15*time.Second)
}

func TestE2EDuplicateMsgIDNoDuplicateRow(t *testing.T) { // spec test 11 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	h := startRuntime(t, stream, "noop-a")

	publishHeightTwice(t, 1)
	waitCursor(t, h.store, "noop-a", 1, 15*time.Second)

	// Allow any (improbable) second delivery to land before asserting.
	time.Sleep(500 * time.Millisecond)
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed_heights rows for (noop-a,1) = %d, want 1", n)
	}
}

func TestE2EPerHeightSealOneAndTwoConsumers(t *testing.T) { // spec tests 7 + 8 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)
	a := startRuntime(t, stream, "noop-a")
	b := startRuntime(t, stream, "noop-b")

	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, a.store, "noop-a", 5, 15*time.Second)
	waitCursor(t, b.store, "noop-b", 5, 15*time.Second)

	// Both active consumers crossed 5 → sealed.
	assertSealed(t, a.store, 5, true)

	// Introduce a third REQUIRED consumer that never processes (cursor stays 0).
	// The required set now includes it, so H=5 is no longer sealed (AND-gating).
	if err := a.store.RegisterConsumer(context.Background(), "noop-c", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	assertSealed(t, a.store, 5, false)
}

func TestE2EKillAndRestartResumes(t *testing.T) { // spec test 3 (end-to-end)
	pg.Reset(t)
	stream := freshStream(t)

	h1 := startRuntime(t, stream, "noop-a")
	publishHeights(t, 1, 2, 3)
	waitCursor(t, h1.store, "noop-a", 3, 15*time.Second)
	h1.stop() // kill

	publishHeights(t, 4, 5, 6)

	h2 := startRuntime(t, stream, "noop-a") // same durable + consumer name
	waitCursor(t, h2.store, "noop-a", 6, 15*time.Second)

	if got := processedCount(t, "noop-a"); got != 6 {
		t.Fatalf("processed rows after restart = %d, want 6 (no duplicates, no skips)", got)
	}
}
