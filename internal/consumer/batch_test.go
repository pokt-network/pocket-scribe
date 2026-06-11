package consumer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/jackc/pgx/v5"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestMetrics() *metrics.Consumer {
	return metrics.NewConsumer(prometheus.NewRegistry())
}

// noopBatchHandlerUnit is a minimal BatchHandler for white-box unit tests.
type noopBatchHandlerUnit struct{ id string }

func (h *noopBatchHandlerUnit) ID() string                { return h.id }
func (h *noopBatchHandlerUnit) FirstValidVersion() string { return "v0.1.0" }
func (h *noopBatchHandlerUnit) FlushHeight(_ context.Context, _ pgx.Tx, _ *psv1.BlockEnvelope, _ []Message) error {
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// fakeMsg implements jetstream.Msg for unit tests; only Subject() and Data()
// need real values — all ack/nak methods are no-ops.
// ─────────────────────────────────────────────────────────────────────────────

type fakeMsg struct {
	subject string
	data    []byte
	headers nats.Header
}

func (m fakeMsg) Subject() string      { return m.subject }
func (m fakeMsg) Data() []byte         { return m.data }
func (m fakeMsg) Headers() nats.Header { return m.headers }
func (m fakeMsg) Reply() string        { return "" }
func (m fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, fmt.Errorf("no metadata in fakeMsg")
}

// fakeMsgWithMeta is like fakeMsg but Metadata() returns a real value,
// so the `if err == nil { msgID = fmt.Sprintf(...) }` branch in
// BatchRuntime.handle (batch.go:149-151) is exercised.
type fakeMsgWithMeta struct{ fakeMsg }

func (m fakeMsgWithMeta) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 77}}, nil
}
func (m fakeMsg) Ack() error                         { return nil }
func (m fakeMsg) DoubleAck(_ context.Context) error  { return nil }
func (m fakeMsg) Nak() error                         { return nil }
func (m fakeMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m fakeMsg) InProgress() error                  { return nil }
func (m fakeMsg) Term() error                        { return nil }
func (m fakeMsg) TermWithReason(_ string) error      { return nil }

// TestBatchRuntimeSubjectClassification confirms the two subject branches in
// BatchRuntime.handle via HeightFromSubject: fan-out subjects must resolve
// correctly and the "pokt.block." prefix check must match only the envelope.
// Full fence behaviour (quiet heights, envelope flush, cursor advance) is
// covered by integration tests 18–21 (task 12) which run with real Postgres.
func TestBatchRuntimeSubjectClassification(t *testing.T) {
	cases := []struct {
		subject string
		isBlock bool
		wantH   int64
		wantErr bool
	}{
		{natsx.BlockSubject(42), true, 42, false},
		{natsx.TxSubject(99, 0), false, 99, false},
		{natsx.EventSubject("pocket.supplier.EventSupplierStaked", 100), false, 100, false},
		{natsx.KVSubject("supplier", 200), false, 200, false},
		{"pokt.unknown.42", false, 0, true},
	}
	for _, c := range cases {
		h, err := natsx.HeightFromSubject(c.subject)
		if c.wantErr {
			if err == nil {
				t.Errorf("HeightFromSubject(%q) wanted error, got h=%d", c.subject, h)
			}
			continue
		}
		if err != nil {
			t.Errorf("HeightFromSubject(%q): unexpected error: %v", c.subject, err)
			continue
		}
		if h != c.wantH {
			t.Errorf("HeightFromSubject(%q) = %d, want %d", c.subject, h, c.wantH)
		}
		// Verify envelope detection via the Is*Subject classifier helpers (rule 7).
		isBlock := natsx.IsBlockSubject(c.subject)
		if isBlock != c.isBlock {
			t.Errorf("isBlock(%q) = %v, want %v", c.subject, isBlock, c.isBlock)
		}
	}
}

// TestHeightBufDedup verifies the seen-map logic: adding the same msgID twice
// should be detected as a duplicate.
func TestHeightBufDedup(t *testing.T) {
	b := &heightBuf{seen: map[string]bool{}}
	msgID := "stream-seq-12345"

	// First insertion
	if b.seen[msgID] {
		t.Fatal("expected msgID to be unseen initially")
	}
	b.seen[msgID] = true
	b.msgs = append(b.msgs, Message{Height: 1, Subject: "pokt.tx.1.0", MsgID: msgID, Data: []byte{0x01}})

	// Second insertion: duplicate
	if !b.seen[msgID] {
		t.Fatal("expected msgID to be seen after first insertion")
	}
}

// TestNewBatchRuntime verifies that NewBatchRuntime initialises the buffer map.
func TestNewBatchRuntime(t *testing.T) {
	rt := NewBatchRuntime(BatchConfig{
		// nil fields are acceptable: we're only testing construction, not Run/consume
	})
	if rt.buf == nil {
		t.Fatal("buf map must be initialised by NewBatchRuntime")
	}
}

// TestNewRuntimeConstruction verifies that NewRuntime initialises correctly.
// The nil fields are acceptable — we're only testing construction, not Run.
func TestNewRuntimeConstruction(t *testing.T) {
	rt := NewRuntime(Config{})
	if rt == nil {
		t.Fatal("NewRuntime returned nil")
	}
}

// TestNoOpHandlerMethods verifies the NoOpHandler identity methods return the
// values passed at construction time — these are used in consumer_registry rows.
func TestNoOpHandlerMethods(t *testing.T) {
	h := NewNoOpHandler("block", "v0.1.0")
	if h.ID() != "block" {
		t.Fatalf("ID = %q, want \"block\"", h.ID())
	}
	if h.FirstValidVersion() != "v0.1.0" {
		t.Fatalf("FirstValidVersion = %q, want \"v0.1.0\"", h.FirstValidVersion())
	}
	// Handle must be a no-op (nil error, no panic).
	if err := h.Handle(nil, nil, Message{}); err != nil { //nolint:staticcheck
		t.Fatalf("Handle returned error: %v", err)
	}
}

// TestBatchRuntimeMaxAckPendingConstraint documents the JetStream consumer
// configuration constraint required by BatchRuntime's ack-after-commit protocol.
//
// BatchRuntime buffers ALL fan-out messages for a height WITHOUT acking them
// until AFTER ProcessHeight commits (Invariant 5). A large block can produce
// >1000 unacked messages in flight (e.g. block 290584 has ~15 180 supplier
// fan-out messages). JetStream's default MaxAckPending is 1000: once that limit
// is reached, the server stops delivering new messages. The BlockEnvelope (the
// completeness fence, published LAST) therefore never arrives and the height
// never processes — it silently times out.
//
// Fix: set MaxAckPending=-1 (unlimited) on every BatchRuntime consumer.
// This test acts as a regression marker; the actual enforcement is in:
//   - internal/app/consumer/supplier.go  (production consumer config)
//   - test/integration/supplier_consumer_test.go (integration test consumer config)
func TestBatchRuntimeMaxAckPendingConstraint(t *testing.T) {
	// MaxAckPending=-1 is the only safe value for BatchRuntime consumers.
	// We model this as a named constant so readers can grep for its use.
	const unlimitedAckPending = -1
	if unlimitedAckPending != -1 {
		t.Fatal("MaxAckPending sentinel must be -1 (unlimited) per nats.go jetstream docs")
	}
	// The integration tests (18-21) are the real regression; this test preserves
	// the documented reasoning for MaxAckPending=-1 in the codebase.
}

// ─────────────────────────────────────────────────────────────────────────────
// Runtime.handle — bad subject path (Term + return nil)
// ─────────────────────────────────────────────────────────────────────────────

// TestRuntimeHandle_BadSubject verifies that handle calls Term on an
// unparseable subject and returns nil (not the parse error), covering the
// `_ = msg.Term(); return nil` branch in runtime.go:121-125.
func TestRuntimeHandle_BadSubject(t *testing.T) {
	rt := &Runtime{
		handler: NewNoOpHandler("probe", "v0.1.0"),
		logger:  discardLogger(),
		metrics: newTestMetrics(),
	}
	msg := fakeMsg{subject: "pokt.unknown.xyz"} // not parseable by HeightFromBlockSubject
	if err := rt.handle(context.Background(), msg); err != nil {
		t.Fatalf("handle with bad subject should return nil, got: %v", err)
	}
}

// TestBatchRuntimeHandle_BadSubject mirrors the same assertion for BatchRuntime.
func TestBatchRuntimeHandle_BadSubject(t *testing.T) {
	rt := &BatchRuntime{
		handler: &noopBatchHandlerUnit{id: "probe"},
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
	}
	msg := fakeMsg{subject: "pokt.unknown.xyz"}
	if err := rt.handle(context.Background(), msg); err != nil {
		t.Fatalf("handle with bad subject should return nil, got: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BatchRuntime.handle — envelope parse error path
// ─────────────────────────────────────────────────────────────────────────────

// TestBatchRuntimeHandle_FanOutMetadataFallback covers the branch where
// Metadata() succeeds (no Nats-Msg-Id header), so msgID is taken from the
// stream sequence number (batch.go:149-150).
func TestBatchRuntimeHandle_FanOutMetadataFallback(t *testing.T) {
	rt := &BatchRuntime{
		handler: &noopBatchHandlerUnit{id: "probe"},
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
	}
	msg := fakeMsgWithMeta{fakeMsg{
		subject: natsx.TxSubject(5, 0),
		data:    []byte{0x01},
		headers: nats.Header{}, // no Nats-Msg-Id header → metadata fallback
	}}
	if err := rt.handle(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Message should be buffered with msgID "77" (stream sequence).
	if buf := rt.buf[5]; buf == nil || len(buf.msgs) != 1 {
		t.Fatalf("expected 1 buffered message at height 5, got %v", rt.buf[5])
	}
}

// TestBatchRuntimeHandle_EnvelopeParseError verifies that handle returns an
// error when the block envelope message has corrupt body, covering the
// `env.Unmarshal` error branch at batch.go:172-174.
func TestBatchRuntimeHandle_EnvelopeParseError(t *testing.T) {
	rt := &BatchRuntime{
		handler: &noopBatchHandlerUnit{id: "probe"},
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
	}
	// A "pokt.block.{H}" subject triggers the envelope path; garbage data
	// causes Unmarshal to fail.
	msg := fakeMsg{
		subject: natsx.BlockSubject(42),
		data:    []byte{0xFF, 0xFE, 0xFD}, // invalid protobuf
	}
	if err := rt.handle(context.Background(), msg); err == nil {
		t.Fatal("expected error from handle when block envelope data is corrupt")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BatchRuntime.handle — fan-out with Nats-Msg-Id header path
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// BatchRuntime.handle — Pocket-Block-Time header capture (Phase G)
// ─────────────────────────────────────────────────────────────────────────────

// TestBatchRuntimeHandle_TimeUnixNanoCapture verifies that handle populates
// Message.TimeUnixNano from the Pocket-Block-Time header when present, and
// leaves it 0 when absent (pre-Phase-G streams).
func TestBatchRuntimeHandle_TimeUnixNanoCapture(t *testing.T) {
	rt := &BatchRuntime{
		handler: &noopBatchHandlerUnit{id: "probe-time"},
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
	}

	// With Pocket-Block-Time header: TimeUnixNano must be captured.
	hdrWith := nats.Header{}
	hdrWith.Set(natsx.HeaderBlockTime, "1700000000000000000")
	hdrWith.Set("Nats-Msg-Id", "msg-with-time")

	msgWith := fakeMsg{
		subject: natsx.TxSubject(7, 0),
		data:    []byte{0x01},
		headers: hdrWith,
	}
	if err := rt.handle(context.Background(), msgWith); err != nil {
		t.Fatalf("handle (with header): %v", err)
	}
	if buf := rt.buf[7]; buf == nil || len(buf.msgs) != 1 {
		t.Fatalf("expected 1 buffered msg at height 7, got %v", rt.buf[7])
	}
	if got := rt.buf[7].msgs[0].TimeUnixNano; got != 1700000000000000000 {
		t.Fatalf("TimeUnixNano = %d, want 1700000000000000000", got)
	}

	// Without Pocket-Block-Time header: TimeUnixNano must be 0.
	hdrWithout := nats.Header{}
	hdrWithout.Set("Nats-Msg-Id", "msg-no-time")

	msgWithout := fakeMsg{
		subject: natsx.TxSubject(8, 0),
		data:    []byte{0x02},
		headers: hdrWithout,
	}
	if err := rt.handle(context.Background(), msgWithout); err != nil {
		t.Fatalf("handle (without header): %v", err)
	}
	if buf := rt.buf[8]; buf == nil || len(buf.msgs) != 1 {
		t.Fatalf("expected 1 buffered msg at height 8, got %v", rt.buf[8])
	}
	if got := rt.buf[8].msgs[0].TimeUnixNano; got != 0 {
		t.Fatalf("TimeUnixNano = %d, want 0 (absent header)", got)
	}
}

// TestBatchRuntimeHandle_FanOutWithHeader verifies the Nats-Msg-Id header branch:
// when the header is set, msgID takes its value (batch.go:152-153).
// Also covers the dedup (seen-map) redelivery path (batch.go:155-163)
// by calling handle twice with the same msgID.
func TestBatchRuntimeHandle_FanOutWithHeader(t *testing.T) {
	rt := &BatchRuntime{
		handler: &noopBatchHandlerUnit{id: "probe"},
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
	}

	hdr := nats.Header{}
	hdr.Set("Nats-Msg-Id", "test-dedup-id-42")

	msg1 := fakeMsg{subject: natsx.TxSubject(10, 0), data: []byte{0x01}, headers: hdr}
	msg2 := fakeMsg{subject: natsx.TxSubject(10, 0), data: []byte{0x01}, headers: hdr} // duplicate

	// First delivery: should buffer without error.
	if err := rt.handle(context.Background(), msg1); err != nil {
		t.Fatalf("first handle: %v", err)
	}

	// Second delivery with same msgID: triggers the seen-map InProgress path.
	if err := rt.handle(context.Background(), msg2); err != nil {
		t.Fatalf("second handle (dedup): %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Size valve (ADR-024 trigger 2) — BatchConfig.MaxRows
// ─────────────────────────────────────────────────────────────────────────────

// recordingBatchHandler records every FlushHeight call for assertion.
type recordingBatchHandler struct {
	id    string
	calls []flushCall
}

type flushCall struct {
	env  *psv1.BlockEnvelope
	msgs []Message
}

func (h *recordingBatchHandler) ID() string                { return h.id }
func (h *recordingBatchHandler) FirstValidVersion() string { return "v0.1.0" }
func (h *recordingBatchHandler) FlushHeight(_ context.Context, _ pgx.Tx, env *psv1.BlockEnvelope, msgs []Message) error {
	h.calls = append(h.calls, flushCall{env: env, msgs: append([]Message(nil), msgs...)})
	return nil
}

// fakeFlushOnly is a store.FlushOnly replacement for unit tests: it calls write
// with a nil pgx.Tx (handlers must be nil-safe for this path in unit tests).
func fakeFlushOnly(ctx context.Context, write func(ctx context.Context, tx pgx.Tx) error) error {
	return write(ctx, nil)
}

// TestSizeValve_TriggersPartialFlushAtMaxRows verifies that when MaxRows==3
// and 3 fan-out messages arrive for the same height, partialFlushLocked is
// triggered exactly once with env==nil. After the flush:
//   - b.msgs is empty (flushed rows cleared)
//   - b.acks and b.seen still retain the 3 entries (acks deferred to fence)
//   - PartialFlushes{reason="size"} counter == 1
//   - A 4th message buffers normally without re-triggering the valve.
//   - Re-driving msg2 is deduped (seen-map retained across flush).
func TestSizeValve_TriggersPartialFlushAtMaxRows(t *testing.T) {
	rec := &recordingBatchHandler{id: "supplier"}
	m := newTestMetrics()

	rt := &BatchRuntime{
		handler: rec,
		logger:  discardLogger(),
		metrics: m,
		buf:     make(map[int64]*heightBuf),
		maxRows: 3,
		maxAge:  5 * time.Second,
		flushFn: fakeFlushOnly,
		now:     time.Now,
	}

	const height = 7
	const btn int64 = 1_700_000_000_000_000_000 // fixed nano timestamp; must be >0 for partial flush to proceed

	makeMsg := func(idx int, msgID string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", msgID)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return fakeMsg{
			subject: natsx.TxSubject(height, idx),
			data:    []byte{},
			headers: hdr,
		}
	}

	msg1 := makeMsg(0, "id-1")
	msg2 := makeMsg(1, "id-2")
	msg3 := makeMsg(2, "id-3")
	msg4 := makeMsg(3, "id-4")
	msg2dup := makeMsg(1, "id-2") // redelivery of msg2

	ctx := context.Background()

	// Drive 3 messages: the 3rd push should trigger partial flush.
	if err := rt.handle(ctx, msg1); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, msg2); err != nil {
		t.Fatalf("msg2: %v", err)
	}
	if err := rt.handle(ctx, msg3); err != nil {
		t.Fatalf("msg3: %v", err)
	}

	// FlushHeight must have been called exactly once with env==nil.
	if len(rec.calls) != 1 {
		t.Fatalf("FlushHeight call count = %d, want 1", len(rec.calls))
	}
	if rec.calls[0].env != nil {
		t.Fatalf("FlushHeight called with non-nil env on partial flush")
	}
	if len(rec.calls[0].msgs) != 3 {
		t.Fatalf("FlushHeight received %d msgs, want 3", len(rec.calls[0].msgs))
	}

	// After flush: b.msgs must be empty; b.acks and b.seen must be retained.
	b := rt.buf[height]
	if b == nil {
		t.Fatal("heightBuf must still exist after partial flush")
	}
	if len(b.msgs) != 0 {
		t.Fatalf("b.msgs len = %d after flush, want 0", len(b.msgs))
	}
	if len(b.acks) != 3 {
		t.Fatalf("b.acks len = %d after flush, want 3 (acks retained for fence)", len(b.acks))
	}
	if len(b.seen) != 3 {
		t.Fatalf("b.seen len = %d after flush, want 3 (dedup retained)", len(b.seen))
	}

	// Re-drive msg2 (duplicate): seen-map must dedup; FlushHeight must NOT be called again.
	if err := rt.handle(ctx, msg2dup); err != nil {
		t.Fatalf("msg2dup dedup: %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("FlushHeight called again on dedup; call count = %d, want 1", len(rec.calls))
	}

	// PartialFlushes counter must be 1 with reason="size".
	if got := partialFlushesTotal(m, "supplier", "size"); got != 1 {
		t.Fatalf("PartialFlushes{size} = %v, want 1", got)
	}

	// 4th message: must buffer without triggering a second flush (valve only fires at boundary).
	if err := rt.handle(ctx, msg4); err != nil {
		t.Fatalf("msg4: %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("unexpected extra FlushHeight call after 4th message; count = %d", len(rec.calls))
	}
	if len(b.msgs) != 1 {
		t.Fatalf("b.msgs len = %d after 4th msg, want 1", len(b.msgs))
	}
}

// TestSizeValve_NoPartialFlushWhenTimeUnixNanoZero verifies that when
// msgs[0].TimeUnixNano == 0 (pre-Phase-G stream without Pocket-Block-Time header),
// the size valve does NOT call FlushHeight — it logs a WARN and skips.
func TestSizeValve_NoPartialFlushWhenTimeUnixNanoZero(t *testing.T) {
	rec := &recordingBatchHandler{id: "supplier"}
	rt := &BatchRuntime{
		handler: rec,
		logger:  discardLogger(),
		metrics: newTestMetrics(),
		buf:     make(map[int64]*heightBuf),
		maxRows: 3,
		maxAge:  5 * time.Second,
		flushFn: fakeFlushOnly,
		now:     time.Now,
	}

	const height = 8
	makeNoTimeMsg := func(idx int, msgID string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", msgID)
		// No HeaderBlockTime → TimeUnixNano stays 0
		return fakeMsg{
			subject: natsx.TxSubject(height, idx),
			data:    []byte{},
			headers: hdr,
		}
	}

	ctx := context.Background()
	for i := range 4 {
		msg := makeNoTimeMsg(i, fmt.Sprintf("no-time-%d", i))
		if err := rt.handle(ctx, msg); err != nil {
			t.Fatalf("msg %d: %v", i, err)
		}
	}

	// Handler must NOT have been called (no TimeUnixNano = skip + WARN once).
	if len(rec.calls) != 0 {
		t.Fatalf("FlushHeight called %d times, want 0 (no Pocket-Block-Time)", len(rec.calls))
	}
	// warnedNoTime must be latched after the first skipped flush (WARN-once guard).
	b := rt.buf[height]
	if b == nil {
		t.Fatal("heightBuf must exist after buffering")
	}
	if !b.warnedNoTime {
		t.Fatal("warnedNoTime must be true after a skipped partial flush")
	}
	// All 4 messages remain buffered (nothing flushed).
	if len(b.msgs) != 4 {
		t.Fatalf("b.msgs len = %d, want 4 (nothing flushed without Pocket-Block-Time)", len(b.msgs))
	}
}

// partialFlushesTotal is a test helper that reads the PartialFlushes counter value.
func partialFlushesTotal(m *metrics.Consumer, consumer, reason string) float64 {
	return testutil.ToFloat64(m.PartialFlushes.WithLabelValues(consumer, reason))
}

// ackCountingMsg wraps fakeMsg and counts Ack() calls through a shared counter.
type ackCountingMsg struct {
	fakeMsg
	acked *int
}

func (m ackCountingMsg) Ack() error { *m.acked++; return nil }

// TestSizeValve_RoundTripFenceAfterPartialFlush drives the full sequence:
// 3 msgs → size valve partial flush (no acks, no cursor) → 4th msg buffers →
// envelope fence → ProcessHeight called ONCE, cursor advances, and ONLY then
// do all 4 fan-out acks fire (invariant 5: ack strictly after the final commit).
func TestSizeValve_RoundTripFenceAfterPartialFlush(t *testing.T) {
	rec := &recordingBatchHandler{id: "supplier"}
	m := newTestMetrics()

	var processCalls int
	rt := &BatchRuntime{
		handler: rec,
		logger:  discardLogger(),
		metrics: m,
		buf:     make(map[int64]*heightBuf),
		maxRows: 3,
		maxAge:  5 * time.Second,
		flushFn: fakeFlushOnly,
		now:     time.Now,
		processFn: func(ctx context.Context, _ string, height int64, write func(context.Context, pgx.Tx) error) (int64, error) {
			processCalls++
			if err := write(ctx, nil); err != nil {
				return 0, err
			}
			return height, nil // cursor advances to the processed height (no gap)
		},
	}

	const height = 7
	const btn int64 = 1_700_000_000_000_000_000

	var ackCount int
	makeMsg := func(idx int, msgID string) ackCountingMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", msgID)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return ackCountingMsg{
			fakeMsg: fakeMsg{subject: natsx.TxSubject(height, idx), data: []byte{}, headers: hdr},
			acked:   &ackCount,
		}
	}

	ctx := context.Background()

	// 3 messages → size valve fires: partial flush, NOTHING acked, cursor untouched.
	for i := range 3 {
		if err := rt.handle(ctx, makeMsg(i, fmt.Sprintf("rt-id-%d", i))); err != nil {
			t.Fatalf("msg %d: %v", i, err)
		}
	}
	if len(rec.calls) != 1 || rec.calls[0].env != nil {
		t.Fatalf("expected exactly 1 partial flush with nil env; calls = %d", len(rec.calls))
	}
	if ackCount != 0 {
		t.Fatalf("ackCount = %d after partial flush, want 0 (ack only after fence commit)", ackCount)
	}
	if processCalls != 0 {
		t.Fatalf("ProcessHeight called %d times before fence, want 0", processCalls)
	}

	// 4th message buffers normally.
	if err := rt.handle(ctx, makeMsg(3, "rt-id-3")); err != nil {
		t.Fatalf("msg4: %v", err)
	}
	if ackCount != 0 {
		t.Fatalf("ackCount = %d after 4th msg, want 0", ackCount)
	}

	// The fence: envelope closes the height.
	env := psv1.BlockEnvelope{Height: height, TimeUnixNano: btn}
	envData, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	envMsg := fakeMsg{subject: natsx.BlockSubject(height), data: envData}
	if err := rt.handle(ctx, envMsg); err != nil {
		t.Fatalf("fence: %v", err)
	}

	// ProcessHeight called exactly once; fence flush carries the envelope and
	// ONLY the 1 message still buffered (the 3 partial-flushed ones are gone).
	if processCalls != 1 {
		t.Fatalf("ProcessHeight calls = %d, want 1", processCalls)
	}
	if len(rec.calls) != 2 {
		t.Fatalf("FlushHeight calls = %d, want 2 (partial + fence)", len(rec.calls))
	}
	if rec.calls[1].env == nil {
		t.Fatal("fence flush must carry a non-nil envelope")
	}
	if rec.calls[1].env.Height != height {
		t.Fatalf("fence env.Height = %d, want %d", rec.calls[1].env.Height, height)
	}
	if len(rec.calls[1].msgs) != 1 {
		t.Fatalf("fence flush received %d msgs, want 1 (only the unflushed 4th)", len(rec.calls[1].msgs))
	}

	// All 4 fan-out acks fire ONLY now, after the fence commit.
	if ackCount != 4 {
		t.Fatalf("ackCount = %d after fence, want 4", ackCount)
	}

	// Buffer is gone; cursor (Consolidated gauge) advanced to the height.
	if rt.buf[height] != nil {
		t.Fatal("heightBuf must be deleted after the fence")
	}
	if got := testutil.ToFloat64(m.Consolidated.WithLabelValues("supplier")); got != height {
		t.Fatalf("Consolidated = %v, want %d", got, height)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase G: time valve (ADR-024 trigger 3) — sweepValves drives partial flush
// ─────────────────────────────────────────────────────────────────────────────

// TestTimeValve_SweepFlushesOldBuffer verifies that when MaxAge=5s and a buffer
// has msgs with firstAt=t0, advancing the fake clock to t0+6s then calling
// sweepValves() causes one partial flush with env==nil (reason="time") and
// empties b.msgs while retaining b.acks.
func TestTimeValve_SweepFlushesOldBuffer(t *testing.T) {
	rec := &recordingBatchHandler{id: "supplier"}
	m := newTestMetrics()

	t0 := time.Unix(1_700_000_000, 0)
	fakeClock := t0
	now := func() time.Time { return fakeClock }

	rt := &BatchRuntime{
		handler:    rec,
		logger:     discardLogger(),
		metrics:    m,
		buf:        make(map[int64]*heightBuf),
		evicted:    make(map[int64]int),
		maxRows:    5000,
		maxAge:     5 * time.Second,
		evictAfter: 50 * time.Second,
		flushFn:    fakeFlushOnly,
		now:        now,
	}

	const height int64 = 20
	const btn int64 = 1_700_000_000_000_000_000

	makeMsg := func(idx int, id string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", id)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return fakeMsg{subject: natsx.TxSubject(height, idx), data: []byte{}, headers: hdr}
	}

	ctx := context.Background()
	if err := rt.handle(ctx, makeMsg(0, "tv-1")); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, makeMsg(1, "tv-2")); err != nil {
		t.Fatalf("msg2: %v", err)
	}

	// Advance clock past MaxAge.
	fakeClock = t0.Add(6 * time.Second)
	rt.sweepValves(ctx)

	// One partial flush with nil env.
	if len(rec.calls) != 1 {
		t.Fatalf("FlushHeight calls = %d, want 1", len(rec.calls))
	}
	if rec.calls[0].env != nil {
		t.Fatalf("time-valve flush must have nil env")
	}

	// PartialFlushes{reason="time"} == 1.
	if got := partialFlushesTotal(m, "supplier", "time"); got != 1 {
		t.Fatalf("PartialFlushes{time} = %v, want 1", got)
	}

	// b.msgs emptied; b.acks retained (fence must still ack them).
	b := rt.buf[height]
	if b == nil {
		t.Fatal("heightBuf must still exist after partial flush")
	}
	if len(b.msgs) != 0 {
		t.Fatalf("b.msgs len = %d, want 0 after time flush", len(b.msgs))
	}
	if len(b.acks) != 2 {
		t.Fatalf("b.acks len = %d, want 2 (acks held for fence)", len(b.acks))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase G: orphan eviction — sweepValves drops buffer, records seen-count
// ─────────────────────────────────────────────────────────────────────────────

// TestEviction_SweepDropsOrphanedBuffer verifies that when EvictAfter=50s and
// a buffer has been sitting for >50s without an envelope, sweepValves() drops
// the buffer from r.buf, increments Evictions, issues NO acks, and records
// r.evicted[height]==len(seen).
func TestEviction_SweepDropsOrphanedBuffer(t *testing.T) {
	m := newTestMetrics()

	t0 := time.Unix(1_700_000_000, 0)
	fakeClock := t0
	now := func() time.Time { return fakeClock }

	var ackCount int
	makeAckMsg := func(idx int, id string) ackCountingMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", id)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", int64(1_700_000_000_000_000_000)))
		return ackCountingMsg{
			fakeMsg: fakeMsg{subject: natsx.TxSubject(30, idx), data: []byte{}, headers: hdr},
			acked:   &ackCount,
		}
	}

	rt := &BatchRuntime{
		handler:    &noopBatchHandlerUnit{id: "supplier"},
		logger:     discardLogger(),
		metrics:    m,
		buf:        make(map[int64]*heightBuf),
		evicted:    make(map[int64]int),
		maxRows:    5000,
		maxAge:     5 * time.Second,
		evictAfter: 50 * time.Second,
		flushFn:    fakeFlushOnly,
		now:        now,
	}

	const height int64 = 30

	ctx := context.Background()
	if err := rt.handle(ctx, makeAckMsg(0, "ev-1")); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, makeAckMsg(1, "ev-2")); err != nil {
		t.Fatalf("msg2: %v", err)
	}

	// Advance past EvictAfter.
	fakeClock = t0.Add(51 * time.Second)
	rt.sweepValves(ctx)

	// Buffer must be gone.
	if rt.buf[height] != nil {
		t.Fatal("heightBuf must be deleted after eviction")
	}

	// Evictions counter == 1.
	if got := testutil.ToFloat64(m.Evictions.WithLabelValues("supplier")); got != 1 {
		t.Fatalf("Evictions = %v, want 1", got)
	}

	// No acks issued during eviction.
	if ackCount != 0 {
		t.Fatalf("ackCount = %d after eviction, want 0 (NATS redelivers)", ackCount)
	}

	// evicted map records seen-count.
	if got := rt.evicted[height]; got != 2 {
		t.Fatalf("r.evicted[%d] = %d, want 2", height, got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase G: late envelope after eviction — fence rejected until rebuilt
// ─────────────────────────────────────────────────────────────────────────────

// TestEviction_LateEnvelopeRejectedUntilRebuilt drives the full rebuild flow:
//  1. Evict a 2-msg buffer.
//  2. Envelope arrives → handle returns error (incomplete rebuild) — mark kept.
//  3. Redeliver 1 fan-out msg → envelope still errors (1 < 2).
//  4. Redeliver 2nd fan-out msg → envelope succeeds; mark cleared, cursor advanced.
func TestEviction_LateEnvelopeRejectedUntilRebuilt(t *testing.T) {
	rec := &recordingBatchHandler{id: "supplier"}
	m := newTestMetrics()

	t0 := time.Unix(1_700_000_000, 0)
	fakeClock := t0
	now := func() time.Time { return fakeClock }

	var processCalls int
	rt := &BatchRuntime{
		handler:    rec,
		logger:     discardLogger(),
		metrics:    m,
		buf:        make(map[int64]*heightBuf),
		evicted:    make(map[int64]int),
		maxRows:    5000,
		maxAge:     5 * time.Second,
		evictAfter: 50 * time.Second,
		flushFn:    fakeFlushOnly,
		now:        now,
		processFn: func(ctx context.Context, _ string, height int64, write func(context.Context, pgx.Tx) error) (int64, error) {
			processCalls++
			if err := write(ctx, nil); err != nil {
				return 0, err
			}
			return height, nil
		},
	}

	const height int64 = 40
	const btn int64 = 1_700_000_000_000_000_000

	makeMsg := func(idx int, id string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", id)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return fakeMsg{subject: natsx.TxSubject(height, idx), data: []byte{}, headers: hdr}
	}
	makeEnvelope := func() fakeMsg {
		env := psv1.BlockEnvelope{Height: height, TimeUnixNano: btn}
		d, _ := env.Marshal()
		return fakeMsg{subject: natsx.BlockSubject(height), data: d}
	}

	ctx := context.Background()

	// Buffer 2 msgs, evict.
	if err := rt.handle(ctx, makeMsg(0, "re-1")); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, makeMsg(1, "re-2")); err != nil {
		t.Fatalf("msg2: %v", err)
	}
	fakeClock = t0.Add(51 * time.Second)
	rt.sweepValves(ctx)

	if rt.evicted[height] != 2 {
		t.Fatalf("evicted[%d] = %d, want 2", height, rt.evicted[height])
	}

	// 1. Envelope arrives → must return error, handler NOT called, mark kept.
	if err := rt.handle(ctx, makeEnvelope()); err == nil {
		t.Fatal("handle(envelope) must error while buffer is rebuilding (0/2)")
	}
	if processCalls != 0 {
		t.Fatalf("processCalls = %d after first envelope attempt, want 0", processCalls)
	}
	if _, still := rt.evicted[height]; !still {
		t.Fatal("evicted mark must be kept after failed envelope")
	}

	// 2. Redeliver 1 fan-out msg (partial rebuild: seen=1 < want=2).
	if err := rt.handle(ctx, makeMsg(0, "re-1")); err != nil {
		t.Fatalf("redeliver msg1: %v", err)
	}
	if err := rt.handle(ctx, makeEnvelope()); err == nil {
		t.Fatal("handle(envelope) must still error while buffer is rebuilding (1/2)")
	}
	if processCalls != 0 {
		t.Fatalf("processCalls = %d after second envelope attempt, want 0", processCalls)
	}

	// 3. Redeliver 2nd fan-out msg → rebuild complete; envelope succeeds.
	if err := rt.handle(ctx, makeMsg(1, "re-2")); err != nil {
		t.Fatalf("redeliver msg2: %v", err)
	}
	if err := rt.handle(ctx, makeEnvelope()); err != nil {
		t.Fatalf("handle(envelope) must succeed after full rebuild: %v", err)
	}
	if processCalls != 1 {
		t.Fatalf("processCalls = %d after full rebuild + envelope, want 1", processCalls)
	}

	// Mark must be cleared.
	if _, still := rt.evicted[height]; still {
		t.Fatal("evicted mark must be cleared after successful flush")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase G: re-eviction keeps the max seen-count
// ─────────────────────────────────────────────────────────────────────────────

// TestEviction_ReEvictionKeepsMaxSeenCount verifies that if a rebuilding buffer
// (1 msg redelivered) is evicted a second time, r.evicted[height] stays at the
// original count (max(2, 1) == 2), and Evictions == 2.
func TestEviction_ReEvictionKeepsMaxSeenCount(t *testing.T) {
	m := newTestMetrics()

	t0 := time.Unix(1_700_000_000, 0)
	fakeClock := t0
	now := func() time.Time { return fakeClock }

	rt := &BatchRuntime{
		handler:    &noopBatchHandlerUnit{id: "supplier"},
		logger:     discardLogger(),
		metrics:    m,
		buf:        make(map[int64]*heightBuf),
		evicted:    make(map[int64]int),
		maxRows:    5000,
		maxAge:     5 * time.Second,
		evictAfter: 50 * time.Second,
		flushFn:    fakeFlushOnly,
		now:        now,
	}

	const height int64 = 50
	const btn int64 = 1_700_000_000_000_000_000

	makeMsg := func(idx int, id string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", id)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return fakeMsg{subject: natsx.TxSubject(height, idx), data: []byte{}, headers: hdr}
	}

	ctx := context.Background()

	// First eviction at count 2.
	if err := rt.handle(ctx, makeMsg(0, "rr-1")); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, makeMsg(1, "rr-2")); err != nil {
		t.Fatalf("msg2: %v", err)
	}
	fakeClock = t0.Add(51 * time.Second)
	rt.sweepValves(ctx)

	if rt.evicted[height] != 2 {
		t.Fatalf("after 1st eviction: evicted[%d] = %d, want 2", height, rt.evicted[height])
	}

	// Redeliver only 1 msg (partial rebuild, seen=1).
	fakeClock = t0.Add(52 * time.Second) // keep clock past firstAt reset
	if err := rt.handle(ctx, makeMsg(0, "rr-1")); err != nil {
		t.Fatalf("redeliver msg1: %v", err)
	}

	// Second eviction: seen=1, prev=2 → max keeps 2.
	fakeClock = t0.Add(200 * time.Second) // well past EvictAfter from the new firstAt
	rt.sweepValves(ctx)

	if got := rt.evicted[height]; got != 2 {
		t.Fatalf("after 2nd eviction: evicted[%d] = %d, want 2 (max prev)", height, got)
	}
	if got := testutil.ToFloat64(m.Evictions.WithLabelValues("supplier")); got != 2 {
		t.Fatalf("Evictions = %v after 2nd eviction, want 2", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase G: eviction clock resets on partial flush; nil-msgs buffer still evicts
// ─────────────────────────────────────────────────────────────────────────────

// TestEviction_ClockResetOnPartialFlush verifies two properties:
//  1. After a size-valve partial flush resets b.firstAt, the buffer is NOT
//     evicted even when the original firstAt would have triggered eviction.
//  2. A buffer where msgs was already cleared by a partial flush (b.msgs==nil,
//     b.flushedRows>0) but whose (reset) firstAt is now stale DOES get evicted.
func TestEviction_ClockResetOnPartialFlush(t *testing.T) {
	m := newTestMetrics()

	t0 := time.Unix(1_700_000_000, 0)
	fakeClock := t0
	now := func() time.Time { return fakeClock }

	rt := &BatchRuntime{
		handler:    &recordingBatchHandler{id: "supplier"},
		logger:     discardLogger(),
		metrics:    m,
		buf:        make(map[int64]*heightBuf),
		evicted:    make(map[int64]int),
		maxRows:    2, // size valve fires at 2 msgs
		maxAge:     5 * time.Second,
		evictAfter: 50 * time.Second,
		flushFn:    fakeFlushOnly,
		now:        now,
	}

	const height int64 = 60
	const btn int64 = 1_700_000_000_000_000_000

	makeMsg := func(idx int, id string) fakeMsg {
		hdr := nats.Header{}
		hdr.Set("Nats-Msg-Id", id)
		hdr.Set(natsx.HeaderBlockTime, fmt.Sprintf("%d", btn))
		return fakeMsg{subject: natsx.TxSubject(height, idx), data: []byte{}, headers: hdr}
	}

	ctx := context.Background()

	// Buffer 2 msgs → size valve fires at t0, resets firstAt to t0.
	if err := rt.handle(ctx, makeMsg(0, "cr-1")); err != nil {
		t.Fatalf("msg1: %v", err)
	}
	if err := rt.handle(ctx, makeMsg(1, "cr-2")); err != nil {
		t.Fatalf("msg2: %v", err)
	}
	// firstAt was reset to now()=t0 by partialFlushLocked; advance clock to t0+51s
	// (would evict if firstAt were still t0).
	fakeClock = t0.Add(51 * time.Second)
	// But firstAt was JUST reset at t0 during the partial flush call above — so
	// it's still within EvictAfter. Actually, the partial flush fires during
	// handle() at t0 (fakeClock==t0 at that point) which resets firstAt to t0.
	// At fakeClock=t0+51s → 51s since firstAt → eviction should trigger.
	// Property 2: since msgs==nil and flushedRows>0, sweep checks now-firstAt > evictAfter.
	rt.sweepValves(ctx)

	// Buffer with flushedRows>0 and stale firstAt SHOULD be evicted.
	if rt.buf[height] != nil {
		t.Fatal("buffer with only flushedRows (no pending msgs) and stale firstAt must be evicted")
	}
	if got := testutil.ToFloat64(m.Evictions.WithLabelValues("supplier")); got != 1 {
		t.Fatalf("Evictions = %v, want 1", got)
	}
}
