//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/test/fixtures/synthetic"
)

const dedupeWindow = 2 * time.Minute

// freshStream ensures the POKT stream exists and starts clean so each test
// starts from an empty stream with a reset dedup window (the NATS server is
// shared across the package). We delete the stream first if it exists — a plain
// Purge keeps the server-side dedup cache active for the 2-minute window,
// causing duplicate-MsgID drops when back-to-back tests reuse the same heights.
func freshStream(t *testing.T) jetstream.Stream {
	t.Helper()
	ctx := context.Background()
	// Delete the existing stream (if any) to flush the dedup window, then
	// recreate it. Ignore "stream not found" errors.
	_ = nats.Client.JetStream().DeleteStream(ctx, natsx.StreamName)
	stream, err := nats.Client.EnsureStream(ctx, dedupeWindow)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	return stream
}

// publishHeightsTo publishes one synthetic block message per height to js, each
// with a deterministic Nats-Msg-Id.
func publishHeightsTo(t *testing.T, js jetstream.JetStream, heights ...int64) {
	t.Helper()
	ctx := context.Background()
	for _, h := range heights {
		subj := natsx.BlockSubject(h)
		if _, err := js.Publish(ctx, subj, synthetic.MarkerData(h),
			jetstream.WithMsgID(natsx.MsgID(subj, h, 0))); err != nil {
			t.Fatalf("publish height %d: %v", h, err)
		}
	}
}

// publishHeights publishes to the shared NATS server.
func publishHeights(t *testing.T, heights ...int64) {
	t.Helper()
	publishHeightsTo(t, nats.Client.JetStream(), heights...)
}

// publishHeightTwice publishes height h twice with the SAME Nats-Msg-Id (shared
// server) to exercise server-side dedup.
func publishHeightTwice(t *testing.T, h int64) {
	t.Helper()
	ctx := context.Background()
	js := nats.Client.JetStream()
	subj := natsx.BlockSubject(h)
	id := natsx.MsgID(subj, h, 0)
	for i := 0; i < 2; i++ {
		if _, err := js.Publish(ctx, subj, synthetic.MarkerData(h), jetstream.WithMsgID(id)); err != nil {
			t.Fatalf("publish dup %d: %v", h, err)
		}
	}
}

// waitConnected blocks until the client's NATS connection is (re-)established or
// the timeout elapses.
func waitConnected(t *testing.T, c *natsx.Client, timeout time.Duration) {
	t.Helper()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		if c.Conn().IsConnected() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("nats client did not reconnect within timeout")
		case <-tick.C:
		}
	}
}

// durableConsumer creates/updates a durable pull consumer named `name` filtered
// to block subjects with the given AckWait. It is deleted on cleanup.
func durableConsumer(t *testing.T, stream jetstream.Stream, name string, ackWait time.Duration) jetstream.Consumer {
	t.Helper()
	cons, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: natsx.BlockSubjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    -1,
	})
	if err != nil {
		t.Fatalf("create durable consumer %s: %v", name, err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(context.Background(), name) })
	return cons
}

// runtimeHandle bundles a running runtime with what tests need to inspect/stop it.
type runtimeHandle struct {
	name    string
	store   *store.Store
	metrics *metrics.Consumer
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

// startRuntime builds a store + NoOp runtime named `name`, binds it to a durable
// on stream, and runs it in a goroutine. Stopped automatically on cleanup.
func startRuntime(t *testing.T, stream jetstream.Stream, name string) *runtimeHandle {
	t.Helper()
	s, err := store.New(context.Background(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	cons := durableConsumer(t, stream, name, 2*time.Second)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := consumer.NewRuntime(consumer.Config{
		Handler:  consumer.NewNoOpHandler(name, "v0.1.0"),
		Store:    s,
		Consumer: cons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h := &runtimeHandle{name: name, store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		_ = rt.Run(ctx)
	}()
	t.Cleanup(h.stop)
	return h
}

// stop cancels the runtime, waits for it to exit, and closes its store. Idempotent.
func (h *runtimeHandle) stop() {
	h.once.Do(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
		}
		h.store.Close()
	})
}

// waitCursor polls a consumer's consolidated_up_to until it reaches want or the
// timeout elapses.
func waitCursor(t *testing.T, s *store.Store, name string, want int64, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	var last int64
	for {
		if cur, err := s.ConsolidatedUpTo(ctx, name); err == nil {
			last = cur
			if cur >= want {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("waitCursor(%s): reached %d, want >= %d within %s", name, last, want, timeout)
		case <-tick.C:
		}
	}
}

// waitHasProcessed polls store.HasProcessed until the consumer has a
// processed_heights row for height or the timeout elapses.  Use this helper
// (instead of waitCursor) when fixture heights are non-contiguous, because the
// contiguous cursor never advances past a gap in the height sequence.
func waitHasProcessed(t *testing.T, s *store.Store, name string, height int64, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		if ok, err := s.HasProcessed(ctx, name, height); err == nil && ok {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("waitHasProcessed(%s, %d): not processed within %s", name, height, timeout)
		case <-tick.C:
		}
	}
}

// processedCount returns how many processed_heights rows a consumer has.
func processedCount(t *testing.T, name string) int {
	t.Helper()
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name=$1`, name).Scan(&n); err != nil {
		t.Fatalf("processedCount(%s): %v", name, err)
	}
	return n
}
