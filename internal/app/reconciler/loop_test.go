package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

// phase G: runLoop syncs IMMEDIATELY on start, then on every tick; errors are
// counted and do not stop the loop; ctx cancel exits with ctx.Err().
func TestRunLoopImmediateThenTicks(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewReconciler(reg)
	calls := make(chan struct{}, 16)
	var n atomic.Int32
	sync := func(_ context.Context) (int, error) {
		calls <- struct{}{}
		cur := n.Add(1)
		if cur == 2 {
			return 0, errors.New("lcd down")
		}
		return 3, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, 20*time.Millisecond, sync, slog.Default(), m) }()

	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("no immediate first sync")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("tick %d never fired", i+1)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := testutil.ToFloat64(m.SyncErrors); got < 1 {
		t.Fatalf("SyncErrors = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(m.Syncs); got < 2 {
		t.Fatalf("Syncs = %v, want >= 2", got)
	}
}
