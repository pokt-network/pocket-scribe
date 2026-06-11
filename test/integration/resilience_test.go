//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
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
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := s.ProcessHeight(ctx, "noop-a", 4, func(context.Context, pgx.Tx) error { return nil }); err == nil {
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
	assertSealed(t, a.store, 5, genesisV0_1_0, true)

	// Both crash simultaneously.
	a.stop()
	b.stop()

	publishHeights(t, 6, 7, 8)

	a2 := startRuntime(t, stream, "noop-a")
	b2 := startRuntime(t, stream, "noop-b")
	waitCursor(t, a2.store, "noop-a", 8, 15*time.Second)
	waitCursor(t, b2.store, "noop-b", 8, 15*time.Second)
	assertSealed(t, a2.store, 8, genesisV0_1_0, true)

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

// supplierHistoryCountAt returns COUNT(*) FROM supplier_history at the given height.
// Unlike supplierHistoryCount (in batch_valves_test.go, which is pinned to
// valveTestHeight), this helper accepts an arbitrary height and is used by the
// partial-restart tests below.
func supplierHistoryCountAt(t *testing.T, height int64) int {
	t.Helper()
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM supplier_history WHERE block_height=$1`, height).Scan(&n); err != nil {
		t.Fatalf("supplierHistoryCountAt(%d): %v", height, err)
	}
	return n
}

// TestPartialSimultaneousRestart (phase G, spec test 13) proves that the
// AND-seal holds correctly when only ONE of two consumers crashes and the other
// keeps processing.
//
// Two consumers: a NoOp "block" runtime (subscribes to BlockSubjectFilter) and
// a supplier BatchRuntime (subscribes to KV + block subjects).  Block envelopes
// are published via publishEnvelope — a valid BlockEnvelope proto — so both
// consumers can decode them.  Heights 1-6 are v0.1.0-era quiet blocks
// (no supplier KV data), so supplier_history stays empty throughout;
// only cursor advancement and AND-seal semantics are exercised.
//
// Sequence:
//  1. Start NoOp "block" runtime + supplier BatchRuntime.
//  2. Publish block envelopes for heights 1-3; both cursors reach 3.
//  3. Heights 1-3 are sealed (AND: both past 3).
//  4. Kill ONLY the supplier consumer.
//  5. Publish block envelopes for heights 4-6 — NoOp processes them;
//     supplier durable accumulates them unacked.
//  6. Assert: NoOp cursor >= 6; supplier cursor still at 3;
//     heights 4, 5, 6 are NOT sealed (AND-seal holds with lagging member).
//  7. Restart supplier (same durable; NATS redelivers envelope msgs 4-6).
//  8. Assert: supplier cursor reaches 6; heights 4-6 are now sealed.
func TestPartialSimultaneousRestart(t *testing.T) { // spec test 13
	pg.Reset(t)
	stream := freshStream(t)
	ids := loadDecoderVersionIDs(t)

	const blockTimeNano int64 = 1_700_000_200_000_000_000
	js := nats.Client.JetStream()

	// Start NoOp "block" consumer and supplier BatchRuntime.
	blockRH := startRuntime(t, stream, "block")
	supplierRH1 := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
		MaxRows:    5000,
		MaxAge:     30 * time.Second,
		EvictAfter: 5 * time.Minute,
	})
	waitConsumerRegistered(t)

	// Publish block envelopes for heights 1-3 (quiet v0.1.0-era: no supplier KV).
	// publishEnvelope publishes a real BlockEnvelope proto on pokt.block.{H} —
	// the NoOp runtime advances its cursor; the supplier runtime closes the height
	// with 0 fan-out msgs and advances its cursor.
	for _, h := range []int64{1, 2, 3} {
		publishEnvelope(t, js, h, blockTimeNano)
	}
	waitCursor(t, blockRH.store, "block", 3, 20*time.Second)
	waitCursor(t, supplierRH1.store, "supplier", 3, 20*time.Second)
	assertSealed(t, blockRH.store, 3, genesisV0_1_0, true)

	// Crash the supplier consumer only.
	// Read the cursor before stopping (stop() closes the store pool).
	curBeforeCrash, _ := supplierRH1.store.ConsolidatedUpTo(context.Background(), "supplier")
	supplierRH1.stop()

	// Publish block envelopes for heights 4-6 — NoOp processes; supplier
	// durable accumulates the unacked envelope messages.
	for _, h := range []int64{4, 5, 6} {
		publishEnvelope(t, js, h, blockTimeNano)
	}
	waitCursor(t, blockRH.store, "block", 6, 20*time.Second)

	// Supplier cursor must still be at 3 (crashed, not processing).
	if curBeforeCrash > 3 {
		t.Fatalf("supplier cursor at crash time = %d, want <= 3", curBeforeCrash)
	}
	// Confirm via DB that the cursor has not advanced while crashed.
	var supplierCurAfterCrash int64
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT COALESCE(consolidated_up_to, 0) FROM consumer_consolidation WHERE consumer_name='supplier'`,
	).Scan(&supplierCurAfterCrash); err != nil {
		t.Fatalf("read supplier cursor after crash: %v", err)
	}
	if supplierCurAfterCrash > 3 {
		t.Fatalf("supplier cursor in DB after crash = %d, want <= 3", supplierCurAfterCrash)
	}

	// AND-seal must NOT fire for any height > 3 (supplier is lagging).
	for _, h := range []int64{4, 5, 6} {
		assertSealed(t, blockRH.store, h, genesisV0_1_0, false)
	}

	// Restart supplier on the same durable — NATS redelivers envelope msgs 4-6.
	supplierRH2 := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
		MaxRows:    5000,
		MaxAge:     30 * time.Second,
		EvictAfter: 5 * time.Minute,
	})

	waitCursor(t, supplierRH2.store, "supplier", 6, 30*time.Second)

	// Heights 4-6 must now be sealed (both consumers have passed them).
	for _, h := range []int64{4, 5, 6} {
		assertSealed(t, supplierRH2.store, h, genesisV0_1_0, true)
	}
}

// TestRestartWithOpenPartialFlush (phase G, spec test 14) proves idempotent
// convergence when a consumer crashes AFTER a partial flush (rows committed to
// DB, cursor NOT advanced, fan-out messages unacked).
//
// Sequence:
//  1. startBatchRuntime with MaxRows=3.
//  2. Bootstrap heights 1-2 as prior art (cursor primed).
//  3. Publish 5 fan-out KV messages for height 3 (real decodable Supplier
//     payloads, distinct operators).  With MaxRows=3, the size valve fires
//     after the 3rd message, committing 3 rows before the envelope arrives.
//     The cursor remains at 2 (height 3 not yet closed).
//  4. Wait for partial flush metric — rows committed, cursor still at 2.
//  5. Kill supplier BEFORE publishing the envelope.
//  6. Restart supplier with a fresh BatchRuntime on the same durable.
//     NATS redelivers the 5 unacked fan-out messages (re-buffers them).
//  7. Publish envelope for height 3 — runtime flushes idempotently
//     (ON CONFLICT absorbs the 3 rows already committed) and advances cursor.
//  8. Assert: supplier_history count at h=3 == 5 (exact, no dupes);
//     consolidated_up_to == 3; processed_heights has exactly 1 row.
func TestRestartWithOpenPartialFlush(t *testing.T) { // spec test 14
	pg.Reset(t)
	stream := freshStream(t)
	ids := loadDecoderVersionIDs(t)

	const (
		testHeight    int64 = 3
		blockTimeNano int64 = 1_700_000_100_000_000_000
		fanOutCount         = 5
	)

	// Bootstrap heights 1 and 2 as contiguous prior art for the cursor.
	blockRH := startBlockRuntime(t, stream, "block")
	bootstrapHeights(t, 1, 2)
	waitCursor(t, blockRH.store, "block", 2, 20*time.Second)

	// Start supplier with MaxRows=3 so 5 fan-out msgs trigger 1 size-valve
	// partial flush (after msg 3) before the envelope arrives.
	supplierRH1 := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
		MaxRows:    3,
		MaxAge:     30 * time.Second,
		EvictAfter: 5 * time.Minute,
	})
	waitConsumerRegistered(t)
	// Ensure supplier cursor reaches 2 before we publish height 3 fan-out
	// messages.  The supplier durable uses DeliverAllPolicy and replays the
	// bootstrapped heights 1-2 as quiet blocks (0 supplier KV messages).
	waitCursor(t, supplierRH1.store, "supplier", 2, 20*time.Second)

	// Publish 5 fan-out KV messages for height 3; each carries a distinct
	// operator so each decodes to a distinct supplier_history row.
	js := nats.Client.JetStream()
	publishFanOutMsgs(t, js, testHeight, fanOutCount, blockTimeNano)

	// Wait for the size valve to fire at least once — confirms partial rows
	// are committed to DB before the envelope arrives.
	waitPartialFlushes(t, supplierRH1.metrics, "supplier", "size", 1, 15*time.Second)

	// Verify partial rows are in DB and cursor is NOT yet at testHeight.
	if got := supplierHistoryCountAt(t, testHeight); got == 0 {
		t.Fatalf("no supplier_history rows at h=%d after partial flush; want > 0", testHeight)
	}
	if cursorAtHeight(t, supplierRH1.store, "supplier", testHeight) {
		t.Fatalf("supplier cursor already at %d before envelope; partial flush should NOT advance cursor", testHeight)
	}

	// Kill supplier BEFORE publishing the envelope (cursor still at 2, msgs unacked).
	supplierRH1.stop()

	// Restart supplier — same durable; NATS redelivers unacked fan-out messages.
	// AckWait on the durable is 2s (set by startBatchRuntime); NATS redelivers all
	// 5 unacked fan-out msgs within one AckWait cycle.
	supplierRH2 := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
		MaxRows:    3,
		MaxAge:     30 * time.Second,
		EvictAfter: 5 * time.Minute,
	})

	// Wait for the size valve to fire on the SECOND runtime — this confirms that
	// NATS has redelivered at least MaxRows=3 of the 5 fan-out msgs and they are
	// buffered.  Publishing the envelope BEFORE redelivery would close the height
	// with a partial buffer (< 5 msgs) and produce fewer rows.
	waitPartialFlushes(t, supplierRH2.metrics, "supplier", "size", 1, 20*time.Second)

	// Now publish the envelope — all 5 msgs are buffered; FlushHeight writes
	// the remaining 2 and idempotently absorbs the 3 already committed.
	publishEnvelope(t, js, testHeight, blockTimeNano)

	// Wait for supplier to process height 3.
	waitHasProcessed(t, supplierRH2.store, "supplier", testHeight, 30*time.Second)

	ctx := context.Background()

	// Exact row count: 5 distinct operators → 5 rows; idempotency absorbs the
	// 3 rows committed during the partial flush before the crash.
	if got := supplierHistoryCountAt(t, testHeight); got != fanOutCount {
		t.Errorf("supplier_history rows at h=%d = %d, want exactly %d (no dupes after partial-flush + restart)", testHeight, got, fanOutCount)
	}

	// processed_heights must have exactly 1 row (idempotent upsert, not double-insert).
	var phCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=$1`, testHeight,
	).Scan(&phCount); err != nil {
		t.Fatalf("count processed_heights: %v", err)
	}
	if phCount != 1 {
		t.Errorf("processed_heights rows for (supplier,%d) = %d, want 1 (idempotent after crash+restart)", testHeight, phCount)
	}

	// ConsolidatedUpTo must reach testHeight (1-2-3 are contiguous).
	if cur, err := supplierRH2.store.ConsolidatedUpTo(ctx, "supplier"); err != nil || cur < testHeight {
		t.Errorf("ConsolidatedUpTo(supplier) = %d, %v; want >= %d", cur, err, testHeight)
	}
}
