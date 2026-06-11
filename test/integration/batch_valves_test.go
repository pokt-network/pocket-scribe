//go:build integration

// batch_valves_test.go — phase G: size/time valves and eviction end-to-end.
//
// Test 28a (partial flush via size valve): publish 12 supplier fan-out
// messages for height H with MaxRows=5 — the size valve fires twice,
// writing partial rows to DB while the cursor is still at H-1.  Publishing
// the envelope then closes the height, acks all messages, and advances the
// cursor with exact row counts (idempotency absorbs the partial flushes).
//
// Test 28b (eviction + redelivery): publish fan-out for H+1 with NO envelope.
// MaxAge=200ms EvictAfter=1s causes the buffer to be evicted.  Publishing the
// envelope afterward causes the runtime to Nak (rebuilding check fails) until
// NATS redelivers the fan-out messages.  The cursor eventually reaches H+1
// with exact row counts (idempotency absorbs the partial rows written before
// eviction).
package integration

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	storetypes "cosmossdk.io/store/types"
	natsgo "github.com/nats-io/nats.go"

	"github.com/nats-io/nats.go/jetstream"
	v010shared "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0/gen/pocket/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// startBatchRuntime builds and starts a BatchRuntime for the supplier consumer
// with customisable BatchConfig knobs.  The NATS consumer is created with an
// explicit AckWait of 2 s so redeliveries after eviction are fast and
// deterministic (EvictAfter=1s fits cleanly within one AckWait cycle).
//
// Returns a *runtimeHandle (cancel/done/store/metrics) exactly as
// startSupplierRuntime does, but with caller-supplied BatchConfig overrides.
func startBatchRuntime(t *testing.T, stream jetstream.Stream, ids map[string]int16, cfg runtime.BatchConfig) *runtimeHandle {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("startBatchRuntime: store.New: %v", err)
	}

	rtr, err := router.NewStaticRouter(upgradesForFixtures, router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("startBatchRuntime: NewStaticRouter: %v", err)
	}

	h := supplierhandler.New(rtr, ids)

	filters := make([]string, 0, 3+len(supplierhandler.EventTypes))
	filters = append(filters, natsx.TxSubjectFilter, natsx.KVSubjectFilter("supplier"), natsx.BlockSubjectFilter)
	for _, et := range supplierhandler.EventTypes {
		filters = append(filters, natsx.EventSubjectFilter(et))
	}
	jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "supplier",
		FilterSubjects: filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     -1,
		// 2 s AckWait: fast redeliveries when buffers are evicted with EvictAfter=1s.
		AckWait:       2 * time.Second,
		MaxAckPending: -1,
	})
	if err != nil {
		t.Fatalf("startBatchRuntime: create consumer: %v", err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(ctx, "supplier") })

	m := metrics.NewConsumer(prometheus.NewRegistry())
	cfg.Handler = h
	cfg.Store = s
	cfg.Consumer = jsCons
	cfg.Logger = pslog.New(io.Discard, slog.LevelError)
	cfg.Metrics = m

	rt := runtime.NewBatchRuntime(cfg)
	cancelCtx, cancel := context.WithCancel(ctx)
	rh := &runtimeHandle{name: "supplier", store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rh.done)
		_ = rt.Run(cancelCtx)
	}()
	t.Cleanup(rh.stop)
	return rh
}

// valveOperator returns the deterministic operator address for fan-out msg i.
func valveOperator(i int) string {
	return "pokt1valve" + strconv.Itoa(i)
}

// kvPayloadSupplierRecord builds a REAL decodable StoreKVPair payload: a
// Supplier/operator_address/<op>/ key whose value is a marshalled v0_1_0
// pocket.shared.Supplier proto.  At low heights the StaticRouter routes to the
// v0_1_0 decoder, which decodes this into a SupplierSnapshot — partial flushes
// therefore write REAL supplier_history rows (one per distinct operator).
func kvPayloadSupplierRecord(t *testing.T, op string) []byte {
	t.Helper()
	sup := v010shared.Supplier{OperatorAddress: op, OwnerAddress: op}
	val, err := sup.Marshal()
	if err != nil {
		t.Fatalf("kvPayloadSupplierRecord: marshal Supplier: %v", err)
	}
	kv := storetypes.StoreKVPair{
		StoreKey: "supplier",
		Key:      []byte("Supplier/operator_address/" + op + "/"),
		Value:    val,
		Delete:   false,
	}
	raw, err := kv.Marshal()
	if err != nil {
		t.Fatalf("kvPayloadSupplierRecord: marshal StoreKVPair: %v", err)
	}
	return raw
}

// publishFanOutMsgs publishes count KV messages for the supplier store at
// height h, each carrying a decodable Supplier record (distinct operator per
// message → distinct supplier_history row) and a Pocket-Block-Time header so
// the size/time valves can fire AND write real rows.
func publishFanOutMsgs(t *testing.T, js jetstream.JetStream, height int64, count int, blockTimeNano int64) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		subj := natsx.KVSubject("supplier", height)
		msgID := natsx.MsgID(subj, height, i)
		msg := &natsgo.Msg{
			Subject: subj,
			Data:    kvPayloadSupplierRecord(t, valveOperator(i)),
			Header:  natsgo.Header{},
		}
		msg.Header.Set(natsx.HeaderBlockTime, strconv.FormatInt(blockTimeNano, 10))
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
			t.Fatalf("publishFanOutMsgs h=%d i=%d: %v", height, i, err)
		}
	}
}

// valveTestHeight is the height under test in both valve sub-tests (heights 1
// and 2 are bootstrapped as contiguous prior art for the cursor).
const valveTestHeight int64 = 3

// supplierHistoryCount returns COUNT(*) FROM supplier_history at valveTestHeight.
func supplierHistoryCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := pg.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM supplier_history WHERE block_height=$1`, valveTestHeight).Scan(&n); err != nil {
		t.Fatalf("supplierHistoryCount: %v", err)
	}
	return n
}

// publishEnvelope publishes a BlockEnvelope on pokt.block.{H} with
// Pocket-Block-Time stamped, mirroring the sidecar's fan-out.
func publishEnvelope(t *testing.T, js jetstream.JetStream, height int64, blockTimeNano int64) {
	t.Helper()
	ctx := context.Background()
	env := &psv1.BlockEnvelope{
		Height:            height,
		TimeUnixNano:      blockTimeNano,
		ChainId:           "pocket",
		PublishedMsgCount: 0,
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("publishEnvelope h=%d: marshal: %v", height, err)
	}
	subj := natsx.BlockSubject(height)
	msgID := natsx.MsgID(subj, height, 0)
	msg := &natsgo.Msg{Subject: subj, Data: raw, Header: natsgo.Header{}}
	msg.Header.Set(natsx.HeaderBlockTime, strconv.FormatInt(blockTimeNano, 10))
	if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
		t.Fatalf("publishEnvelope h=%d: %v", height, err)
	}
}

// waitPartialFlushes polls the PartialFlushes counter until it reaches at least
// want or timeout elapses.  Used to observe that the size/time valve fired
// before the envelope arrives.
func waitPartialFlushes(t *testing.T, m *metrics.Consumer, consumer, reason string, want float64, timeout time.Duration) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(timeout)
	for {
		if got := testutil.ToFloat64(m.PartialFlushes.WithLabelValues(consumer, reason)); got >= want {
			return
		}
		select {
		case <-deadline:
			got := testutil.ToFloat64(m.PartialFlushes.WithLabelValues(consumer, reason))
			t.Fatalf("waitPartialFlushes(%s,%s): got %.0f, want >= %.0f within %s", consumer, reason, got, want, timeout)
		case <-tick.C:
		}
	}
}

// cursorAtHeight returns true iff the consumer's consolidated_up_to >= h.
func cursorAtHeight(t *testing.T, s *store.Store, name string, h int64) bool {
	t.Helper()
	cur, err := s.ConsolidatedUpTo(context.Background(), name)
	if err != nil {
		return false
	}
	return cur >= h
}

// TestBatchValvesSizeFlushAndEviction (spec tests 28a + 28b; ADR-024 amendment Phase G)
// validates the size valve and eviction path end-to-end using the real
// BatchRuntime + NATS + Postgres integration harness.
func TestBatchValvesSizeFlushAndEviction(t *testing.T) {
	// ── 28a: partial flush via size valve ──────────────────────────────────────
	t.Run("size_valve_partial_flush", func(t *testing.T) {
		pg.Reset(t)
		stream := freshStream(t)
		ids := loadDecoderVersionIDs(t)

		// Also need the block consumer running so we can check cursor semantics.
		blockRH := startBlockRuntime(t, stream, "block")

		// Contiguous heights 1 and 2 are used as "prior art" to prime the block
		// consumer cursor before H=3 (the height under test).
		bootstrapHeights(t, 1, 2)
		waitCursor(t, blockRH.store, "block", 2, 20*time.Second)

		// Start supplier runtime with MaxRows=5 so 12 fan-out msgs trigger 2 size
		// valve partial flushes BEFORE the envelope arrives.
		const testHeight = valveTestHeight
		// blockTimeNano: use a fixed non-zero value (chain-consensus time simulation).
		// This is test scaffolding; it is NOT used for chain-data rows (Invariant 1).
		const blockTimeNano int64 = 1_700_000_000_000_000_000

		supplierRH := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
			MaxRows:    5,
			MaxAge:     30 * time.Second, // disable time valve for this sub-test
			EvictAfter: 5 * time.Minute,
		})
		waitConsumerRegistered(t, "supplier", 5*time.Second)

		// Publish 12 fan-out KV messages for height 3. With MaxRows=5 this should
		// trigger 2 partial flushes (after msg 5 and after msg 10).
		js := nats.Client.JetStream()
		publishFanOutMsgs(t, js, testHeight, 12, blockTimeNano)

		// Wait for at least 2 partial flushes (size valve).
		waitPartialFlushes(t, supplierRH.metrics, "supplier", "size", 2, 15*time.Second)

		// MANDATORY DB signal (spec): partial-flush rows are PRESENT in DB while
		// the cursor is still behind.  2 size flushes × MaxRows=5 = 10 committed
		// supplier_history rows (metric increments only AFTER FlushOnly commits).
		if got := supplierHistoryCount(t); got < 10 {
			t.Fatalf("size valve: supplier_history rows at h=%d = %d before envelope, want >= 10 (2 partial flushes of 5)", testHeight, got)
		}

		// At this point the cursor for "supplier" must still be BEHIND testHeight
		// (envelope not yet published → height not processed → cursor not advanced).
		if cursorAtHeight(t, supplierRH.store, "supplier", testHeight) {
			t.Fatalf("size valve test: supplier cursor already at %d before envelope was published", testHeight)
		}

		// Now publish the envelope to close the height.
		publishEnvelope(t, js, testHeight, blockTimeNano)

		// Cursor must advance to testHeight.
		waitHasProcessed(t, supplierRH.store, "supplier", testHeight, 30*time.Second)

		// Exact row count, no dupes: 12 distinct operators → 12 rows.
		if got := supplierHistoryCount(t); got != 12 {
			t.Errorf("supplier_history rows at h=%d = %d after envelope, want exactly 12 (no dupes)", testHeight, got)
		}

		// consolidated_up_to must be EXACTLY testHeight (heights 1-3 contiguous).
		if cur, err := supplierRH.store.ConsolidatedUpTo(context.Background(), "supplier"); err != nil || cur != testHeight {
			t.Errorf("ConsolidatedUpTo(supplier) = %d, %v; want exactly %d", cur, err, testHeight)
		}

		// processed_heights has exactly one row (no double-insert despite partial flushes).
		ctx := context.Background()
		var phCount int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=$1`, testHeight,
		).Scan(&phCount); err != nil {
			t.Fatalf("count processed_heights: %v", err)
		}
		if phCount != 1 {
			t.Errorf("processed_heights rows for (supplier,%d) = %d, want 1 (idempotent after partial flushes)", testHeight, phCount)
		}

		// Verify at least 2 size partial flushes were recorded.
		if got := testutil.ToFloat64(supplierRH.metrics.PartialFlushes.WithLabelValues("supplier", "size")); got < 2 {
			t.Errorf("PartialFlushes(size) = %.0f, want >= 2", got)
		}

		// After the envelope is processed there should be 0 evictions.
		if got := testutil.ToFloat64(supplierRH.metrics.Evictions.WithLabelValues("supplier")); got != 0 {
			t.Errorf("Evictions = %.0f, want 0 (size-valve path, no evictions)", got)
		}
	})

	// ── 28b: eviction + redelivery ─────────────────────────────────────────────
	t.Run("eviction_and_redelivery", func(t *testing.T) {
		pg.Reset(t)
		stream := freshStream(t)
		ids := loadDecoderVersionIDs(t)

		// Prime heights 1 and 2 for block consumer (cursor context).
		blockRH := startBlockRuntime(t, stream, "block")
		bootstrapHeights(t, 1, 2)
		waitCursor(t, blockRH.store, "block", 2, 20*time.Second)

		const testHeight = valveTestHeight
		const blockTimeNano int64 = 1_700_000_000_100_000_000

		// MaxAge=200ms, EvictAfter=1s: the sweep goroutine will evict the buffer for
		// testHeight after ~1s because no envelope arrives.  AckWait=2s (set in
		// startBatchRuntime) ensures NATS redelivers within one AckWait cycle after
		// eviction.
		supplierRH := startBatchRuntime(t, stream, ids, runtime.BatchConfig{
			MaxRows:    5000, // disable size valve
			MaxAge:     200 * time.Millisecond,
			EvictAfter: time.Second,
		})
		waitConsumerRegistered(t, "supplier", 5*time.Second)

		// Publish fan-out messages WITHOUT publishing the envelope.
		js := nats.Client.JetStream()
		publishFanOutMsgs(t, js, testHeight, 6, blockTimeNano)

		// Wait for the eviction metric to fire (buffer dropped because no envelope).
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		evictDeadline := time.After(10 * time.Second)
	evictWait:
		for {
			if got := testutil.ToFloat64(supplierRH.metrics.Evictions.WithLabelValues("supplier")); got >= 1 {
				break evictWait
			}
			select {
			case <-evictDeadline:
				got := testutil.ToFloat64(supplierRH.metrics.Evictions.WithLabelValues("supplier"))
				t.Fatalf("eviction did not fire within 10s; Evictions=%.0f", got)
			case <-tick.C:
			}
		}

		// Cursor must NOT be at testHeight (envelope never published).
		if cursorAtHeight(t, supplierRH.store, "supplier", testHeight) {
			t.Fatalf("eviction test: cursor already at %d despite no envelope", testHeight)
		}

		// The time valve (MaxAge=200ms << EvictAfter=1s) flushed the partial rows
		// to DB before the buffer was evicted — they must be PRESENT now.
		if got := supplierHistoryCount(t); got != 6 {
			t.Errorf("supplier_history rows at h=%d after eviction = %d, want 6 (time-valve partial flush preceded eviction)", testHeight, got)
		}

		// Now publish the envelope.  The first delivery will be Nak'd by the
		// eviction fence (rebuilt buffer's seen-count < recorded count).  NATS will
		// redeliver; subsequent attempts succeed once the fan-out messages are
		// redelivered and the buffer is rebuilt.
		publishEnvelope(t, js, testHeight, blockTimeNano)

		// Wait for the cursor to advance (generous timeout: 2×AckWait + processing).
		waitHasProcessed(t, supplierRH.store, "supplier", testHeight, 30*time.Second)

		// Eviction metric must be >= 1 (fired at least once).
		if got := testutil.ToFloat64(supplierRH.metrics.Evictions.WithLabelValues("supplier")); got < 1 {
			t.Errorf("Evictions = %.0f, want >= 1", got)
		}

		// processed_heights has exactly one row (idempotent despite partial rows
		// written before eviction and redelivery).
		ctx := context.Background()
		var phCount int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=$1`, testHeight,
		).Scan(&phCount); err != nil {
			t.Fatalf("count processed_heights: %v", err)
		}
		if phCount != 1 {
			t.Errorf("processed_heights rows for (supplier,%d) = %d, want 1 (idempotent after eviction+redelivery)", testHeight, phCount)
		}

		// Exact final rows: idempotency absorbed the pre-eviction partial rows.
		if got := supplierHistoryCount(t); got != 6 {
			t.Errorf("supplier_history rows at h=%d final = %d, want exactly 6 (idempotent re-flush after redelivery)", testHeight, got)
		}

		// consolidated_up_to must be EXACTLY testHeight.
		if cur, err := supplierRH.store.ConsolidatedUpTo(ctx, "supplier"); err != nil || cur != testHeight {
			t.Errorf("ConsolidatedUpTo(supplier) = %d, %v; want exactly %d", cur, err, testHeight)
		}
	})
}
