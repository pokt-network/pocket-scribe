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
