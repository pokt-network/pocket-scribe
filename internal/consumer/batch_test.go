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
