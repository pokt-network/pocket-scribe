//go:build integration

// batch_runtime_crash_test.go — invariant 5 evidence for BatchRuntime:
// ack happens strictly AFTER Postgres commit; crashes between commit and ack,
// or between envelope arrival and flush completion, produce identical final
// state via redelivery + idempotent upserts.
//
// Two scenarios:
//
//	Test 22a (failFlushOnce): a wrapper BatchHandler that returns an error on
//	the FIRST FlushHeight call (before writing any rows). The envelope is Nak'd;
//	the runtime retries; idempotent ON CONFLICT logic ensures no duplicate rows
//	and the cursor advances exactly once.
//
//	Test 22b (contextCancelMidFlight): the runtime context is canceled after
//	the height's messages are published but before the runtime finishes
//	processing. A fresh runtime is started; NATS redelivers; the final state
//	must be identical to a clean run.
package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// ─────────────────────────────────────────────────────────────────────────────
// faultSupplierHandler wraps supplierhandler.Handler as a runtime.BatchHandler
// and injects an error on the FIRST FlushHeight call (BEFORE writing any rows).
// This simulates a crash after the envelope arrives but before flush completes.
// The Postgres transaction is rolled back; the envelope is Nak'd; on redelivery
// the second call delegates to the real supplier handler.
// ─────────────────────────────────────────────────────────────────────────────

type faultSupplierHandler struct {
	inner     *supplierhandler.Handler
	callCount atomic.Int32
}

func (h *faultSupplierHandler) ID() string                { return h.inner.ID() }
func (h *faultSupplierHandler) FirstValidVersion() string { return h.inner.FirstValidVersion() }
func (h *faultSupplierHandler) FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []runtime.Message) error {
	n := h.callCount.Add(1)
	if n == 1 {
		// First call: return error WITHOUT writing. The runtime rolls back the tx,
		// Nak's the envelope, and the NATS server redelivers.
		return errors.New("simulated mid-flush crash (invariant 5 test)")
	}
	return h.inner.FlushHeight(ctx, tx, env, msgs)
}

// newFaultSupplierRuntime builds a BatchRuntime backed by faultSupplierHandler.
// It uses a short AckWait (5s) so the envelope is redelivered quickly after the
// first fault.
func newFaultSupplierRuntime(t *testing.T, stream jetstream.Stream, ids map[string]int16) (*runtimeHandle, *faultSupplierHandler) {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	rtr, err := router.NewStaticRouter(upgradesForFixtures, router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}

	inner := supplierhandler.New(rtr, ids)
	faultH := &faultSupplierHandler{inner: inner}

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
		AckWait:        5 * time.Second,
		MaxAckPending:  -1,
	})
	if err != nil {
		t.Fatalf("create fault supplier consumer: %v", err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(ctx, "supplier") })

	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := runtime.NewBatchRuntime(runtime.BatchConfig{
		Handler:  faultH,
		Store:    s,
		Consumer: jsCons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	cancelCtx, cancel := context.WithCancel(ctx)
	rh := &runtimeHandle{name: "supplier", store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rh.done)
		_ = rt.Run(cancelCtx)
	}()
	t.Cleanup(rh.stop)
	return rh, faultH
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 22a: fail-flush-once (Nak → redelivery → idempotent final state)
// ─────────────────────────────────────────────────────────────────────────────

// TestBatchRuntimeFailFlushOnce (spec test 22a) proves invariant 5:
//   - A BatchRuntime that faults on the first FlushHeight call (before writing)
//     must Nak the envelope.
//   - On redelivery, the second FlushHeight call succeeds.
//   - Final row counts must be IDENTICAL to a clean run (no duplicates, no skips).
//   - processed_heights has exactly 1 row for the height.
//   - cursor (HasProcessed) confirms the height is recorded.
func TestBatchRuntimeFailFlushOnce(t *testing.T) { // spec test 22a
	pg.Reset(t)
	stream := freshStream(t)
	ids := loadDecoderVersionIDs(t)

	// Start block runtime (required for the height's block row + cursor).
	blockRH := startBlockRuntime(t, stream, "block")

	// Bootstrap a single fixture height (v0_1_28 era, height 290584).
	// This produces the full fan-out: KV + events + tx messages + block envelope.
	bootstrapHeights(t, 290584)

	// Wait for block runtime to process the height.
	waitHasProcessed(t, blockRH.store, "block", 290584, 30*time.Second)

	// Start the fault supplier runtime (first FlushHeight call returns an error,
	// second attempt succeeds).
	supplierRH, faultH := newFaultSupplierRuntime(t, stream, ids)

	// Wait for the fault supplier runtime to process the height (second attempt succeeds).
	// Timeout is generous: AckWait=5s + processing time.
	waitHasProcessed(t, supplierRH.store, "supplier", 290584, 60*time.Second)

	// Assert the fault was triggered at least once.
	if n := faultH.callCount.Load(); n < 2 {
		t.Errorf("FlushHeight call count = %d, want >= 2 (fault + success)", n)
	}

	ctx := context.Background()

	// Assert: processed_heights has exactly one row for this height (no double-insert).
	var phCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=290584`,
	).Scan(&phCount); err != nil {
		t.Fatalf("count processed_heights: %v", err)
	}
	if phCount != 1 {
		t.Errorf("processed_heights rows for (supplier,290584) = %d, want 1 (invariant 5: no dup)", phCount)
	}

	// Assert: msg_stake_supplier row count matches expected (92 from the fixture).
	// ON CONFLICT DO UPDATE / DO NOTHING means a redelivered flush produces zero new rows.
	var msgStakeCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM msg_stake_supplier WHERE block_height=290584`,
	).Scan(&msgStakeCount); err != nil {
		t.Fatalf("count msg_stake_supplier: %v", err)
	}
	if msgStakeCount != 92 {
		t.Errorf("msg_stake_supplier rows at 290584 = %d, want 92 (no duplicates after redelivery)", msgStakeCount)
	}

	// Assert: height is recorded (cursor may not reach 290584 since the height is
	// non-contiguous, but HasProcessed must be true).
	if ok, err := supplierRH.store.HasProcessed(ctx, "supplier", 290584); err != nil || !ok {
		t.Errorf("HasProcessed(supplier, 290584) = %v, %v; want true, nil", ok, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 22b: context-cancel mid-flight (restart + redelivery → identical state)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// intraTxFaultHandler wraps supplierhandler.Handler and, on the FIRST
// FlushHeight call, delegates to the real inner handler (writing all supplier
// rows into the open tx) and THEN returns an error. This forces the BatchRuntime
// to roll back a transaction that already contained real writes — the gap covered
// by this test: faultSupplierHandler faults BEFORE writing, so the rollback path
// for "writes happened but error returned" was never exercised.
// On the second call the handler succeeds normally (no error).
// ─────────────────────────────────────────────────────────────────────────────

type intraTxFaultHandler struct {
	inner     *supplierhandler.Handler
	callCount atomic.Int32
}

func (h *intraTxFaultHandler) ID() string                { return h.inner.ID() }
func (h *intraTxFaultHandler) FirstValidVersion() string { return h.inner.FirstValidVersion() }
func (h *intraTxFaultHandler) FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []runtime.Message) error {
	n := h.callCount.Add(1)
	if n == 1 {
		// First call: do the real writes inside the tx, then return an error.
		// The BatchRuntime will roll back, discarding those writes.
		// On redelivery the second call proceeds normally.
		if err := h.inner.FlushHeight(ctx, tx, env, msgs); err != nil {
			return err // real failure — propagate (test would still pass on retry)
		}
		return errors.New("simulated intra-tx fault after real writes (invariant 5 test)")
	}
	return h.inner.FlushHeight(ctx, tx, env, msgs)
}

// newIntraTxFaultRuntime builds a BatchRuntime backed by intraTxFaultHandler
// with a short AckWait so the envelope is redelivered quickly after the fault.
func newIntraTxFaultRuntime(t *testing.T, stream jetstream.Stream, ids map[string]int16) (*runtimeHandle, *intraTxFaultHandler) {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}

	rtr, err := router.NewStaticRouter(upgradesForFixtures, router.DefaultRegistry(), "v0_1_0")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}

	inner := supplierhandler.New(rtr, ids)
	faultH := &intraTxFaultHandler{inner: inner}

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
		AckWait:        5 * time.Second,
		MaxAckPending:  -1,
	})
	if err != nil {
		t.Fatalf("create intra-tx fault consumer: %v", err)
	}
	t.Cleanup(func() { _ = stream.DeleteConsumer(ctx, "supplier") })

	m := metrics.NewConsumer(prometheus.NewRegistry())
	rt := runtime.NewBatchRuntime(runtime.BatchConfig{
		Handler:  faultH,
		Store:    s,
		Consumer: jsCons,
		Logger:   pslog.New(io.Discard, slog.LevelError),
		Metrics:  m,
	})
	cancelCtx, cancel := context.WithCancel(ctx)
	rh := &runtimeHandle{name: "supplier", store: s, metrics: m, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(rh.done)
		_ = rt.Run(cancelCtx)
	}()
	t.Cleanup(rh.stop)
	return rh, faultH
}

// TestIntraTxFaultRollsBackPartialWrites (spec test 22c) proves that the
// BatchRuntime's rollback is effective even when FlushHeight has already
// performed real Postgres writes before returning an error.
//
// Sequence:
//  1. Bootstrap height 290584 (v0.1.28 era, ~92 msg_stake_supplier rows).
//  2. Start block runtime; wait for it to process the height.
//  3. Start supplier runtime backed by intraTxFaultHandler.
//  4. First FlushHeight: writes supplier rows into tx → returns error → rollback.
//     Assert: 0 supplier rows for 290584, cursor < 290584, HasProcessed=false.
//  5. Fault disarmed; NATS redelivers envelope.
//  6. Second FlushHeight succeeds; rows committed.
//     Assert: expected row count, cursor advanced, HasProcessed=true.
func TestIntraTxFaultRollsBackPartialWrites(t *testing.T) { // spec test 22c
	pg.Reset(t)
	stream := freshStream(t)
	ids := loadDecoderVersionIDs(t)

	// Start block runtime and bootstrap fixture height.
	blockRH := startBlockRuntime(t, stream, "block")
	bootstrapHeights(t, 290584)
	waitHasProcessed(t, blockRH.store, "block", 290584, 30*time.Second)

	// Start supplier runtime with the intra-tx fault handler.
	supplierRH, faultH := newIntraTxFaultRuntime(t, stream, ids)

	ctx := context.Background()

	// Wait for the FIRST call to complete (fault triggered); use a short poll
	// on callCount rather than sleeping a fixed duration.
	deadline := time.After(30 * time.Second)
	for faultH.callCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("intraTxFaultHandler: first FlushHeight call never arrived within 30s")
		case <-time.After(50 * time.Millisecond):
		}
	}
	// Give the rollback time to complete before asserting.
	time.Sleep(200 * time.Millisecond)

	// Assert AFTER fault, BEFORE retry: rollback must have discarded all writes.
	var supplierRowsAfterFault int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM msg_stake_supplier WHERE block_height=290584`,
	).Scan(&supplierRowsAfterFault); err != nil {
		t.Fatalf("count msg_stake_supplier after fault: %v", err)
	}
	if supplierRowsAfterFault != 0 {
		t.Errorf("msg_stake_supplier rows after rolled-back tx = %d, want 0 (rollback must discard intra-tx writes)", supplierRowsAfterFault)
	}

	okAfterFault, err := supplierRH.store.HasProcessed(ctx, "supplier", 290584)
	if err != nil {
		t.Fatalf("HasProcessed after fault: %v", err)
	}
	if okAfterFault {
		t.Error("HasProcessed(supplier,290584) = true after fault; rollback must not advance processed_heights")
	}

	curAfterFault, err := supplierRH.store.ConsolidatedUpTo(ctx, "supplier")
	if err != nil {
		t.Fatalf("ConsolidatedUpTo after fault: %v", err)
	}
	if curAfterFault >= 290584 {
		t.Errorf("cursor = %d after fault, want < 290584 (rollback must not advance cursor)", curAfterFault)
	}

	// Wait for redelivery and successful second attempt.
	waitHasProcessed(t, supplierRH.store, "supplier", 290584, 60*time.Second)

	// Assert fault was triggered exactly once before success.
	if n := faultH.callCount.Load(); n < 2 {
		t.Errorf("FlushHeight call count = %d, want >= 2 (fault + success)", n)
	}

	// Assert final state: rows committed, idempotent (no duplicates).
	var finalRows int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM msg_stake_supplier WHERE block_height=290584`,
	).Scan(&finalRows); err != nil {
		t.Fatalf("count msg_stake_supplier after success: %v", err)
	}
	if finalRows != 92 {
		t.Errorf("msg_stake_supplier rows after success = %d, want 92 (idempotent: no dup, no missing)", finalRows)
	}

	// Assert exactly one processed_heights row (idempotent upsert, not double-insert).
	var phCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=290584`,
	).Scan(&phCount); err != nil {
		t.Fatalf("count processed_heights: %v", err)
	}
	if phCount != 1 {
		t.Errorf("processed_heights rows = %d, want 1 (invariant 5: no dup after rollback+retry)", phCount)
	}
}

// TestBatchRuntimeContextCancelAndRestart (spec test 22b) proves invariant 5
// from the other angle: cancel the runtime context after messages are published
// but before (or during) processing.  A fresh runtime restarts and NATS
// redelivers; the final state must be identical to a single clean run.
//
// We use v0.1.0 quiet heights (1, 2, 3) so the test is fast: the supplier
// runtime produces zero data rows and only advances the cursor.
func TestBatchRuntimeContextCancelAndRestart(t *testing.T) { // spec test 22b
	pg.Reset(t)
	stream := freshStream(t)
	ids := loadDecoderVersionIDs(t)

	// Bootstrap quiet heights first; messages are in NATS before any runtime starts.
	bootstrapHeights(t, 1, 2, 3)

	// Start block runtime and drain it.
	blockRH := startBlockRuntime(t, stream, "block")
	waitCursor(t, blockRH.store, "block", 3, 20*time.Second)

	// Start supplier runtime; register it so IsSealed is aware; then immediately cancel.
	supplierRH1 := startSupplierRuntime(t, stream, ids)
	waitConsumerRegistered(t)
	supplierRH1.stop() // cancel before (or mid) processing

	// Restart with a fresh runtime. NATS redelivers all unacked envelope messages.
	// Short AckWait on the existing durable means redelivery is fast.
	supplierRH2 := startSupplierRuntime(t, stream, ids)
	waitCursor(t, supplierRH2.store, "supplier", 3, 30*time.Second)

	// Sealed: both consumers at or past height 3.
	assertSealed(t, supplierRH2.store, 3, genesisV0_1_0, true)

	ctx := context.Background()

	// Assert: processed_heights has EXACTLY one row per height (no duplicates).
	for _, h := range []int64{1, 2, 3} {
		var n int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM processed_heights WHERE consumer_name='supplier' AND height=$1`, h,
		).Scan(&n); err != nil {
			t.Fatalf("count processed_heights h=%d: %v", h, err)
		}
		if n != 1 {
			t.Errorf("processed_heights (supplier,%d) = %d, want 1 (invariant 5: no dup after cancel+restart)", h, n)
		}
	}

	// Assert: no supplier data rows at quiet heights (v0.1.0 era has no suppliers).
	var supplierHistCount int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM supplier_history WHERE block_height IN (1,2,3)`,
	).Scan(&supplierHistCount); err != nil {
		t.Fatalf("count supplier_history: %v", err)
	}
	if supplierHistCount != 0 {
		t.Errorf("supplier_history rows at quiet heights = %d, want 0 (no phantom rows after cancel+restart)", supplierHistCount)
	}
}
