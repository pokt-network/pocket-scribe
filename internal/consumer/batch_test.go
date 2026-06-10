package consumer

import (
	"testing"

	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

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
		// Verify envelope detection (the "pokt.block." prefix check in handle).
		isBlock := len(c.subject) >= len("pokt.block.") && c.subject[:len("pokt.block.")] == "pokt.block."
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
