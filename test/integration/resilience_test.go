//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	tc "github.com/pokt-network/pocketscribe/test/testcontainers"
)

func TestAckAfterCommitRedelivery(t *testing.T) { // spec test 4
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	stream := freshStream(t)
	cons := durableConsumer(t, stream, "noop-a", 1*time.Second)

	publishHeights(t, 1)

	// First delivery: commit, then "crash" (do NOT ack).
	batch, err := cons.Fetch(1)
	if err != nil {
		t.Fatalf("fetch #1: %v", err)
	}
	got := 0
	for range batch.Messages() {
		got++
		if _, err := s.ProcessHeight(ctx, "noop-a", 1, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process #1: %v", err)
		}
		// no Ack — simulate crash after commit, before ack
	}
	if got != 1 {
		t.Fatalf("expected 1 message on first fetch, got %d", got)
	}

	// Wait past AckWait so the server redelivers.
	time.Sleep(1500 * time.Millisecond)

	// Redelivery: process again (idempotent) and ack.
	batch2, err := cons.Fetch(1)
	if err != nil {
		t.Fatalf("fetch #2: %v", err)
	}
	redelivered := 0
	for msg := range batch2.Messages() {
		redelivered++
		if _, err := s.ProcessHeight(ctx, "noop-a", 1, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process #2: %v", err)
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if redelivered != 1 {
		t.Fatalf("expected redelivery of the un-acked message, got %d", redelivered)
	}

	// Exactly one row: no duplicate, no skip.
	if c := processedCount(t, "noop-a"); c != 1 {
		t.Fatalf("processed rows = %d, want 1 (ack-after-commit holds)", c)
	}
	if cur, _ := s.ConsolidatedUpTo(ctx, "noop-a"); cur != 1 {
		t.Fatalf("cursor = %d, want 1", cur)
	}
}

func TestNatsRestartConsumerReconnects(t *testing.T) { // spec test 5
	ctx := context.Background()
	pg.Reset(t)

	// Dedicated fixed-port NATS so the same client reconnects after a bounce.
	nc := tc.NATSFixedPort(t, "14222")
	stream, err := nc.Client.EnsureStream(ctx, dedupeWindow)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	// One long-lived runtime bound to this server's durable.
	h := startRuntime(t, stream, "noop-a")
	publishHeightsTo(t, nc.Client.JetStream(), 1, 2, 3)
	waitCursor(t, h.store, "noop-a", 3, 20*time.Second)

	// Bounce the NATS server. A Stop+Start (unlike Terminate) keeps the
	// container filesystem, so the JetStream file storage — stream, durable, and
	// already-published messages — survives.
	stopTimeout := 20 * time.Second
	if err := nc.Container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop nats: %v", err)
	}
	if err := nc.Container.Start(ctx); err != nil {
		t.Fatalf("start nats: %v", err)
	}

	// The runtime's client auto-reconnects (MaxReconnects(-1)) and its reconnect
	// loop re-establishes the subscription. Messages published after the restart
	// are delivered and consolidated — no loss, no duplicates.
	waitConnected(t, nc.Client, 30*time.Second)
	publishHeightsTo(t, nc.Client.JetStream(), 4, 5, 6)
	waitCursor(t, h.store, "noop-a", 6, 45*time.Second)
	if got := processedCount(t, "noop-a"); got != 6 {
		t.Fatalf("processed rows = %d, want 6 (no loss, no dup across NATS restart)", got)
	}
}

func TestPostgresRestartWaitsThenRecovers(t *testing.T) { // spec test 6
	ctx := context.Background()
	// Dedicated fixed-port container so the same pool reconnects after restart.
	fp := tc.PostgresFixedPort(t, "15432")
	s, err := newStoreFromDSN(t, fp.DSN)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	for _, h := range []int64{1, 2, 3} {
		if _, err := s.ProcessHeight(ctx, "noop-a", h, func(context.Context, pgx.Tx) error { return nil }); err != nil {
			t.Fatalf("process %d: %v", h, err)
		}
	}

	// Stop Postgres.
	stopTimeout := 20 * time.Second
	if err := fp.Container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}

	// While down, processing fails: the cursor cannot advance ("the consumer
	// waits"). We assert only the failure here — querying the cursor while
	// Postgres is down would itself error. The recovery assertion below
	// (cursor == 4) is what proves no advance happened during the outage.
	if _, err := s.ProcessHeight(ctx, "noop-a", 4, func(context.Context, pgx.Tx) error { return nil }); err == nil {
		t.Fatal("expected ProcessHeight to fail while Postgres is down")
	}

	// Bring Postgres back on the same port; the pool reconnects.
	if err := fp.Container.Start(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	// Retry until the pool re-establishes a connection.
	recovered := false
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for !recovered {
		if _, err := s.ProcessHeight(ctx, "noop-a", 4, func(context.Context, pgx.Tx) error { return nil }); err == nil {
			recovered = true
			break
		}
		select {
		case <-deadline:
			t.Fatal("Postgres did not recover within 30s")
		case <-tick.C:
		}
	}

	if cur, _ := s.ConsolidatedUpTo(ctx, "noop-a"); cur != 4 {
		t.Fatalf("cursor after recovery = %d, want 4", cur)
	}
	if c := processedCount4(t, s); c != 1 {
		t.Fatalf("processed rows for height 4 = %d, want 1 (no duplicate)", c)
	}
}

func TestMultipleConsumersCrashRecover(t *testing.T) { // spec test 12
	pg.Reset(t)
	stream := freshStream(t)

	a := startRuntime(t, stream, "noop-a")
	b := startRuntime(t, stream, "noop-b")
	publishHeights(t, 1, 2, 3, 4, 5)
	waitCursor(t, a.store, "noop-a", 5, 15*time.Second)
	waitCursor(t, b.store, "noop-b", 5, 15*time.Second)
	assertSealed(t, a.store, 5, true)

	// Both crash simultaneously.
	a.stop()
	b.stop()

	publishHeights(t, 6, 7, 8)

	a2 := startRuntime(t, stream, "noop-a")
	b2 := startRuntime(t, stream, "noop-b")
	waitCursor(t, a2.store, "noop-a", 8, 15*time.Second)
	waitCursor(t, b2.store, "noop-b", 8, 15*time.Second)
	assertSealed(t, a2.store, 8, true)

	if got := processedCount(t, "noop-a"); got != 8 {
		t.Fatalf("noop-a processed rows = %d, want 8", got)
	}
	if got := processedCount(t, "noop-b"); got != 8 {
		t.Fatalf("noop-b processed rows = %d, want 8", got)
	}
}

// Ensure the jetstream import is used (it's referenced by the batch.Messages()
// return type and needed for the Fetch call signature).
var _ = jetstream.AckExplicitPolicy
