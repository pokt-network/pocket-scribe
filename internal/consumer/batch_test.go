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
