package supplier

import (
	"context"
	"errors"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// ──────────────────────────────────────────────────────────────────────────────
// Test doubles
// ──────────────────────────────────────────────────────────────────────────────

// fakeRouter satisfies the Router interface for unit tests that don't need a real decoder.
type fakeRouter struct {
	dec decoders.Decoder
	err error
}

func (f *fakeRouter) DecoderFor(_ int64) (decoders.Decoder, error) {
	return f.dec, f.err
}

// noopDecoder satisfies decoders.Decoder; every method is a no-op.
type noopDecoder struct{}

func (noopDecoder) Version() string                                        { return "v0_noop" }
func (noopDecoder) DecodeBlockHeader(_ []byte) (*types.BlockHeader, error) { return nil, nil }
func (noopDecoder) DecodeSupplierMsg(_ string, _ []byte) (*types.SupplierMsg, error) {
	return nil, nil
}
func (noopDecoder) DecodeSupplierEvent(_ string, _ []types.EventAttr) (*types.SupplierEvent, error) {
	return nil, nil
}
func (noopDecoder) DecodeSupplierKV(_, _ []byte, _ bool) (*types.SupplierKVRecord, error) {
	return nil, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Identity tests
// ──────────────────────────────────────────────────────────────────────────────

// TestHandlerID verifies the stable consumer name.
func TestHandlerID(t *testing.T) {
	h := New(&fakeRouter{}, nil)
	if h.ID() != "supplier" {
		t.Fatalf("ID = %q, want \"supplier\"", h.ID())
	}
}

// TestHandlerFirstValidVersion verifies the first-valid version string.
func TestHandlerFirstValidVersion(t *testing.T) {
	h := New(&fakeRouter{}, nil)
	if h.FirstValidVersion() != "v0.1.0" {
		t.Fatalf("FirstValidVersion = %q, want \"v0.1.0\"", h.FirstValidVersion())
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// FlushHeight unit tests (no DB / NATS required)
// ──────────────────────────────────────────────────────────────────────────────

// TestFlushHeightQuietHeight verifies that FlushHeight returns nil immediately
// when msgs is empty (quiet height — no supplier activity at this block).
// The router must NOT be called; passing nil tx is safe.
func TestFlushHeightQuietHeight(t *testing.T) {
	h := New(&fakeRouter{}, nil)
	env := &psv1.BlockEnvelope{Height: 99999}
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{}); err != nil {
		t.Fatalf("FlushHeight quiet height: unexpected error: %v", err)
	}
}

// TestFlushHeightNilMsgsIsQuiet verifies that a nil slice (not just empty) is
// also treated as a quiet height.
func TestFlushHeightNilMsgsIsQuiet(t *testing.T) {
	h := New(&fakeRouter{}, nil)
	env := &psv1.BlockEnvelope{Height: 1}
	if err := h.FlushHeight(context.Background(), nil, env, nil); err != nil {
		t.Fatalf("FlushHeight nil msgs: unexpected error: %v", err)
	}
}

// TestFlushHeightRouterError verifies that FlushHeight propagates a router error
// when there is at least one message (non-quiet height — router IS invoked).
func TestFlushHeightRouterError(t *testing.T) {
	routerErr := errors.New("router: height not covered")
	h := New(&fakeRouter{err: routerErr}, nil)
	env := &psv1.BlockEnvelope{Height: 1}
	msgs := []consumer.Message{{Height: 1, Subject: "pokt.tx.1.0", Data: []byte{0x01}}}
	err := h.FlushHeight(context.Background(), nil, env, msgs)
	if err == nil {
		t.Fatal("expected router error to be propagated")
	}
	if !errors.Is(err, routerErr) {
		t.Fatalf("expected routerErr in chain, got %v", err)
	}
}

// TestFlushHeightUnknownVersionID verifies that FlushHeight returns an error
// when the decoded version tag is not present in the versionIDs map.
func TestFlushHeightUnknownVersionID(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{}) // empty map
	env := &psv1.BlockEnvelope{Height: 1}
	msgs := []consumer.Message{{Height: 1, Subject: "pokt.tx.1.0", Data: []byte{0x01}}}
	err := h.FlushHeight(context.Background(), nil, env, msgs)
	if err == nil {
		t.Fatal("expected error: decoder version not found in versionIDs")
	}
}

// TestFlushHeightUnexpectedSubject verifies that FlushHeight returns an error
// for a buffered message whose subject prefix is not pokt.tx.*, pokt.events.*,
// or pokt.kv.*.
func TestFlushHeightUnexpectedSubject(t *testing.T) {
	// store.DecoderTag converts "v0_noop" → "v0.noop" (underscores → dots).
	// The handler calls store.DecoderTag(dec.Version()) when looking up the map.
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := &psv1.BlockEnvelope{Height: 1}
	msgs := []consumer.Message{{Height: 1, Subject: "pokt.unknown.1", Data: []byte{0x01}}}
	err := h.FlushHeight(context.Background(), nil, env, msgs)
	if err == nil {
		t.Fatal("expected error: unexpected subject prefix")
	}
}
