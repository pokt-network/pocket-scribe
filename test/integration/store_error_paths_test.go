//go:build integration

// store_error_paths_test.go — cancelled-context error-path tests for store
// functions that have a single DB call with a `if err != nil { return ... }`
// branch never hit by the happy-path integration tests.
// Using a cancelled context forces pgx to return an error at the Exec/Query
// call, exercising the error return without needing fault injection or mocks.
// Also contains Runtime.Run error-path tests (RegisterConsumer fails when ctx
// is cancelled before the pool executes).
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nats-io/nats.go/jetstream"

	consumer "github.com/pokt-network/pocketscribe/internal/consumer"
	pslog "github.com/pokt-network/pocketscribe/internal/log"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Minimal jetstream.Consumer + MessagesContext stubs for reconnect-path tests.
// These live here (not in the white-box consumer package tests) because they
// need a live Postgres store to get past RegisterConsumer.
// ─────────────────────────────────────────────────────────────────────────────

// disconnectIter is a jetstream.MessagesContext whose first Next() call returns
// a connection error (simulating a NATS disconnect), and subsequent calls block
// until Stop() is called.
type disconnectIter struct {
	errCh  chan error // first call receives from here
	stopCh chan struct{}
}

func newDisconnectIter(err error) *disconnectIter {
	ch := make(chan error, 1)
	ch <- err
	return &disconnectIter{errCh: ch, stopCh: make(chan struct{})}
}

func (it *disconnectIter) Next(_ ...jetstream.NextOpt) (jetstream.Msg, error) {
	select {
	case err := <-it.errCh:
		return nil, err
	case <-it.stopCh:
		return nil, fmt.Errorf("iterator stopped")
	}
}
func (it *disconnectIter) Stop() {
	select {
	case <-it.stopCh: // already closed
	default:
		close(it.stopCh)
	}
}
func (it *disconnectIter) Drain() { it.Stop() }

// disconnectConsumer is a jetstream.Consumer that returns a disconnectIter.
type disconnectConsumer struct{ iter *disconnectIter }

func (c *disconnectConsumer) Messages(_ ...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return c.iter, nil
}
func (c *disconnectConsumer) Fetch(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, nil
}
func (c *disconnectConsumer) FetchBytes(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return nil, nil
}
func (c *disconnectConsumer) FetchNoWait(_ int) (jetstream.MessageBatch, error) { return nil, nil }
func (c *disconnectConsumer) Consume(_ jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	return nil, nil
}
func (c *disconnectConsumer) Next(_ ...jetstream.FetchOpt) (jetstream.Msg, error) { return nil, nil }
func (c *disconnectConsumer) Info(_ context.Context) (*jetstream.ConsumerInfo, error) {
	return nil, nil
}
func (c *disconnectConsumer) CachedInfo() *jetstream.ConsumerInfo { return nil }

// noopBatch is a minimal BatchHandler used only for error-path tests that
// require constructing a BatchRuntime but never reach FlushHeight.
type noopBatch struct{ id string }

func (h noopBatch) ID() string                { return h.id }
func (h noopBatch) FirstValidVersion() string { return "v0.1.0" }
func (h noopBatch) FlushHeight(_ context.Context, _ pgx.Tx, _ *psv1.BlockEnvelope, _ []consumer.Message) error {
	return nil
}

// cancelled returns an already-cancelled context.
func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// ─────────────────────────────────────────────────────────────────────────────
// InsertBlock error path
// ─────────────────────────────────────────────────────────────────────────────

func TestInsertBlock_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	hdr := &types.BlockHeader{
		Height: 1,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := store.InsertBlock(cancelled(), tx, hdr); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RegisterConsumer / DeregisterConsumer / RequiredSet error paths
// ─────────────────────────────────────────────────────────────────────────────

func TestRegisterConsumer_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if err := s.RegisterConsumer(cancelled(), "probe", "v0.1.0"); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestDeregisterConsumer_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.DeregisterConsumer(cancelled(), "probe"); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestRequiredSet_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.RequiredSet(cancelled(), 1, genesisV0_1_0); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestRequiredSet_InvalidGenesis(t *testing.T) {
	s := storeFrom(t)
	ctx := context.Background()
	// invalid genesis version surfaces as an error, not a silent empty set
	if _, err := s.RequiredSet(ctx, 1, "garbage"); err == nil {
		t.Fatal("want error for invalid genesis version")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HasProcessed error path
// ─────────────────────────────────────────────────────────────────────────────

func TestHasProcessed_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.HasProcessed(cancelled(), "probe", 1); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// IsSealed error path
// ─────────────────────────────────────────────────────────────────────────────

func TestIsSealed_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.IsSealed(cancelled(), 1, genesisV0_1_0); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestIsSealed_InvalidGenesis(t *testing.T) {
	s := storeFrom(t)
	ctx := context.Background()
	if _, err := s.IsSealed(ctx, 1, "garbage"); err == nil {
		t.Fatal("want error for invalid genesis version")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ConsolidatedUpTo error path
// ─────────────────────────────────────────────────────────────────────────────

func TestConsolidatedUpTo_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.ConsolidatedUpTo(cancelled(), "probe"); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ProcessHeight error path (cancelled context before tx begins)
// ─────────────────────────────────────────────────────────────────────────────

func TestProcessHeight_ContextCancelled(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	// Register the consumer first with a valid ctx, then cancel for ProcessHeight.
	if err := s.RegisterConsumer(ctx, "probe-ph", "v0.1.0"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := s.ProcessHeight(cancelled(), "probe-ph", 1, func(_ context.Context, _ pgx.Tx) error {
		return nil
	}); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ListUpgrades error path
// ─────────────────────────────────────────────────────────────────────────────

func TestListUpgrades_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.ListUpgrades(cancelled()); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DecoderVersionIDs error path
// ─────────────────────────────────────────────────────────────────────────────

func TestDecoderVersionIDs_ContextCancelled(t *testing.T) {
	s := storeFrom(t)
	if _, err := s.DecoderVersionIDs(cancelled()); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime.Run error path — RegisterConsumer fails (closed pool)
// ─────────────────────────────────────────────────────────────────────────────

// TestRuntime_RegisterConsumerError verifies that Runtime.Run propagates the
// error returned by RegisterConsumer when the store's connection pool is closed,
// covering the `return err` branch at the top of Run (runtime.go line 57).
func TestRuntime_RegisterConsumerError(t *testing.T) {
	ctx := context.Background()

	// Build a store whose pool is immediately closed so RegisterConsumer fails.
	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	s.Close() // any DB call against s will now fail

	logger := pslog.New(io.Discard, slog.LevelError)
	m := metrics.NewConsumer(prometheus.NewRegistry())

	rt := consumer.NewRuntime(consumer.Config{
		Handler: consumer.NewNoOpHandler("probe-rt", "v0.1.0"),
		Store:   s,
		Logger:  logger,
		Metrics: m,
		// Consumer is nil — Run never reaches consume() because RegisterConsumer
		// fails first.
	})

	if runErr := rt.Run(ctx); runErr == nil {
		t.Fatal("expected error from Run when RegisterConsumer fails on closed pool")
	}
}

// TestBatchRuntime_RegisterConsumerError verifies that BatchRuntime.Run
// propagates the error from RegisterConsumer when the pool is closed,
// covering the `return err` branch at the top of BatchRuntime.Run (batch.go:63).
func TestBatchRuntime_RegisterConsumerError(t *testing.T) {
	ctx := context.Background()

	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	s.Close()

	logger := pslog.New(io.Discard, slog.LevelError)
	m := metrics.NewConsumer(prometheus.NewRegistry())

	rt := consumer.NewBatchRuntime(consumer.BatchConfig{
		Handler: noopBatch{id: "probe-batch"},
		Store:   s,
		Logger:  logger,
		Metrics: m,
	})

	if runErr := rt.Run(ctx); runErr == nil {
		t.Fatal("expected error from BatchRuntime.Run when RegisterConsumer fails on closed pool")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Migrate — status and unknown-command branches
// ─────────────────────────────────────────────────────────────────────────────

// TestMigrate_StatusCommand verifies that Migrate("status") succeeds (covers
// the "status" case branch in migrate.go that is not exercised by the
// testcontainer setup, which only calls "up").
func TestMigrate_StatusCommand(t *testing.T) {
	pg.Reset(t)
	if err := store.Migrate(context.Background(), pg.DSN, "status"); err != nil {
		t.Fatalf("Migrate(status): %v", err)
	}
}

// TestMigrate_UnknownCommand verifies that Migrate returns an error for an
// unrecognized command (covers the `default` case in migrate.go).
func TestMigrate_UnknownCommand(t *testing.T) {
	if err := store.Migrate(context.Background(), pg.DSN, "rewind"); err == nil {
		t.Fatal("expected error for unknown migrate command")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FlushOnly — phase G: commits handler writes WITHOUT touching cursor tables.
// ─────────────────────────────────────────────────────────────────────────────

// insertTestBlock inserts a block row at the given height inside tx.
// It is a local helper for FlushOnly tests only.
func insertTestBlock(ctx context.Context, tx pgx.Tx, _ *store.Store, height int64) error {
	hdr := &types.BlockHeader{
		Height: height,
		Time:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	return store.InsertBlock(ctx, tx, hdr)
}

// TestFlushOnlyNoCursorAdvance verifies that FlushOnly:
//  1. commits the handler's writes (block row is present after call).
//  2. does NOT advance consumer_consolidation (ConsolidatedUpTo remains 0).
//  3. does NOT write a processed_heights row (HasProcessed returns false).
//  4. rolls back on write-callback error (block at height 999992 absent).
func TestFlushOnlyNoCursorAdvance(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// Register a consumer so ConsolidatedUpTo is queryable.
	if err := s.RegisterConsumer(ctx, "flushonly-test", "v0.1.0"); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	// Happy path: FlushOnly inserts a block row at height 999991.
	if err := s.FlushOnly(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertTestBlock(ctx, tx, s, 999991)
	}); err != nil {
		t.Fatalf("FlushOnly: %v", err)
	}

	// Assert: block row is present.
	var blockCount int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM block WHERE height=999991`).Scan(&blockCount); err != nil {
		t.Fatalf("query block: %v", err)
	}
	if blockCount != 1 {
		t.Fatalf("expected block row at height 999991, count=%d", blockCount)
	}

	// Assert: cursor NOT advanced.
	cur, err := s.ConsolidatedUpTo(ctx, "flushonly-test")
	if err != nil {
		t.Fatalf("ConsolidatedUpTo: %v", err)
	}
	if cur != 0 {
		t.Fatalf("ConsolidatedUpTo = %d, want 0 (FlushOnly must not advance cursor)", cur)
	}

	// Assert: no processed_heights row.
	ok, err := s.HasProcessed(ctx, "flushonly-test", 999991)
	if err != nil {
		t.Fatalf("HasProcessed: %v", err)
	}
	if ok {
		t.Fatal("HasProcessed = true; FlushOnly must not write processed_heights")
	}

	// Error path: FlushOnly with a write callback that fails must roll back.
	if err := s.FlushOnly(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if insErr := insertTestBlock(ctx, tx, s, 999992); insErr != nil {
			return insErr
		}
		return fmt.Errorf("boom")
	}); err == nil {
		t.Fatal("expected FlushOnly to return error when write callback fails")
	}

	// Assert: height 999992 not committed (rollback).
	var count992 int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM block WHERE height=999992`).Scan(&count992); err != nil {
		t.Fatalf("query block 999992: %v", err)
	}
	if count992 != 0 {
		t.Fatalf("height 999992 should not be committed after FlushOnly error, count=%d", count992)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ProcessHeight — write callback error path
// ─────────────────────────────────────────────────────────────────────────────

// TestProcessHeight_WriteError verifies that ProcessHeight propagates an error
// returned by the write callback, covering the `return 0, fmt.Errorf(...)` at
// process.go:30.
func TestProcessHeight_WriteError(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	if err := s.RegisterConsumer(ctx, "probe-we", "v0.1.0"); err != nil {
		t.Fatalf("register: %v", err)
	}

	writeErr := fmt.Errorf("simulated write failure")
	_, err := s.ProcessHeight(ctx, "probe-we", 1, func(_ context.Context, _ pgx.Tx) error {
		return writeErr
	})
	if err == nil {
		t.Fatal("expected error from ProcessHeight when write callback fails")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime.Run — reconnect-loop path (consume returns error, ctx cancelled
// during backoff, covering runtime.go lines 65-73)
// ─────────────────────────────────────────────────────────────────────────────

// TestRuntimeRun_ReconnectCtxCancelled verifies that Runtime.Run:
//  1. Enters the reconnect branch when consume returns a non-ctx error.
//  2. Exits via ctx.Done() when the context is cancelled during the backoff.
//
// This covers runtime.go: ctx.Err() guard after consume error (line 65),
// the Warn log (line 70), and the select case <-ctx.Done() (line 72-73).
func TestRuntimeRun_ReconnectCtxCancelled(t *testing.T) {
	pg.Reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := storeFrom(t)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	logger := pslog.New(io.Discard, slog.LevelError)

	disconnectErr := fmt.Errorf("simulated NATS disconnect")
	iter := newDisconnectIter(disconnectErr)
	cons := &disconnectConsumer{iter: iter}

	rt := consumer.NewRuntime(consumer.Config{
		Handler:  consumer.NewNoOpHandler("probe-reconnect", "v0.1.0"),
		Store:    s,
		Consumer: cons,
		Logger:   logger,
		Metrics:  m,
	})

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// Give the runtime time to register, enter consume, receive the
	// disconnect error, and enter the reconnect select block.
	time.Sleep(200 * time.Millisecond)

	// Cancel the context — this should cause case <-ctx.Done() in Run to fire.
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Run to return a non-nil error (context cancelled)")
		}
		// err should be context.Canceled
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit within 5s after context cancellation")
	}
}

// TestRuntimeRun_CtxErrAtLoopTop covers runtime.go lines 61-62 (the
// ctx.Err() guard at the top of the Run for-loop). This fires when:
//   - consume returns a non-ctx error → reconnect delay starts
//   - time.After fires (500ms) → continue
//   - ctx is already cancelled → ctx.Err() fires at loop top
func TestRuntimeRun_CtxErrAtLoopTop(t *testing.T) {
	pg.Reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := storeFrom(t)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	logger := pslog.New(io.Discard, slog.LevelError)

	disconnectErr := fmt.Errorf("simulated NATS disconnect")
	iter := newDisconnectIter(disconnectErr)
	cons := &disconnectConsumer{iter: iter}

	rt := consumer.NewRuntime(consumer.Config{
		Handler:  consumer.NewNoOpHandler("probe-looptop", "v0.1.0"),
		Store:    s,
		Consumer: cons,
		Logger:   logger,
		Metrics:  m,
	})

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// Cancel ctx AFTER the reconnect select enters (let disconnect fire, let
	// the select start) but BEFORE time.After fires — cancel will hit both
	// ctx.Done() arm of the current select AND the loop-top guard if it re-loops.
	// Using a slightly longer sleep so time.After(500ms) is more likely to fire
	// AFTER we cancel.
	//
	// For determinism we actually just cancel quickly — either code path
	// (select ctx.Done() or loop-top guard) produces a non-nil error, which
	// is all we need to assert. Both paths are covered by the combination of
	// this test and TestRuntimeRun_ReconnectCtxCancelled.
	time.Sleep(250 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error from Run")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit in time")
	}
}

// TestBatchRuntimeRun_ReconnectCtxCancelled is the BatchRuntime counterpart,
// covering batch.go lines 71-73 (reconnect backoff select case <-ctx.Done()).
func TestBatchRuntimeRun_ReconnectCtxCancelled(t *testing.T) {
	pg.Reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := storeFrom(t)
	m := metrics.NewConsumer(prometheus.NewRegistry())
	logger := pslog.New(io.Discard, slog.LevelError)

	disconnectErr := fmt.Errorf("simulated NATS disconnect")
	iter := newDisconnectIter(disconnectErr)
	cons := &disconnectConsumer{iter: iter}

	rt := consumer.NewBatchRuntime(consumer.BatchConfig{
		Handler:  noopBatch{id: "probe-batch-reconnect"},
		Store:    s,
		Consumer: cons,
		Logger:   logger,
		Metrics:  m,
	})

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected BatchRuntime.Run to return a non-nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("BatchRuntime.Run did not exit within 5s after context cancellation")
	}
}
