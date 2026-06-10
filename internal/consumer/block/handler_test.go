package block

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/types"
)

type fakeInserter struct{ got *types.BlockHeader }

func (f *fakeInserter) InsertBlock(_ context.Context, _ pgx.Tx, h *types.BlockHeader) error {
	f.got = h
	return nil
}

// TestHandlerID verifies the stable consumer name.
func TestHandlerID(t *testing.T) {
	h := New(&fakeInserter{})
	if h.ID() != "block" {
		t.Fatalf("ID = %q, want \"block\"", h.ID())
	}
}

// TestHandlerFirstValidVersion verifies the first-valid version string.
func TestHandlerFirstValidVersion(t *testing.T) {
	h := New(&fakeInserter{})
	if h.FirstValidVersion() != "v0.1.0" {
		t.Fatalf("FirstValidVersion = %q, want \"v0.1.0\"", h.FirstValidVersion())
	}
}

// TestHandleCorruptEnvelope verifies that invalid envelope bytes return a
// descriptive error wrapping the height.
func TestHandleCorruptEnvelope(t *testing.T) {
	h := New(&fakeInserter{})
	err := h.Handle(context.Background(), nil, consumer.Message{Height: 42, Data: []byte{0xff, 0xfe, 0x00}})
	if err == nil {
		t.Fatal("expected error for corrupt BlockEnvelope bytes")
	}
	if err.Error() == "" {
		t.Fatal("error message should not be empty")
	}
}

// TestHandleDecodesEnvelopeAndInserts verifies that Handle maps a BlockEnvelope
// to a BlockHeader correctly (no router, no meta bytes — ADR-022 amendment).
func TestHandleDecodesEnvelopeAndInserts(t *testing.T) {
	ts := time.Date(2025, 5, 28, 19, 32, 0, 0, time.UTC)
	env := &psv1.BlockEnvelope{
		Height:            190974,
		TimeUnixNano:      ts.UnixNano(),
		Hash:              "abcdef01",
		ProposerAddress:   "deadbeef",
		ChainId:           "pocket",
		TxCount:           3,
		EventCount:        12,
		KvCount:           55,
		PublishedMsgCount: 70,
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal BlockEnvelope: %v", err)
	}

	fi := &fakeInserter{}
	h := New(fi)
	if err := h.Handle(context.Background(), nil, consumer.Message{Height: 190974, Data: raw}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if fi.got == nil {
		t.Fatal("inserter was not called")
	}
	if fi.got.Height != 190974 {
		t.Errorf("Height = %d, want 190974", fi.got.Height)
	}
	if fi.got.TxCount != 3 {
		t.Errorf("TxCount = %d, want 3", fi.got.TxCount)
	}
	if fi.got.Hash != "abcdef01" {
		t.Errorf("Hash = %q, want abcdef01", fi.got.Hash)
	}
	if fi.got.ProposerAddress != "deadbeef" {
		t.Errorf("ProposerAddress = %q, want deadbeef", fi.got.ProposerAddress)
	}
	if !fi.got.Time.Equal(ts) {
		t.Errorf("Time = %v, want %v", fi.got.Time, ts)
	}
}
