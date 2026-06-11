package supplier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

// ─────────────────────────────────────────────────────────────────────────────
// FlushHeight with nil envelope (ADR-024 trigger 2-3 partial flush contract)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushHeightNilEnvelope_PartialFlushUsesMessageTime verifies that when
// env==nil (partial flush triggered by size or time valve), FlushHeight
// derives height and block_time from msgs[0] rather than the envelope.
// Specifically: types.Position.Time == time.Unix(0, msgs[0].TimeUnixNano).UTC().
// The router is invoked with msgs[0].Height; the nil tx is safe because
// noopDecoder returns nil for every message, so no store call is made.
func TestFlushHeightNilEnvelope_PartialFlushUsesMessageTime(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})

	const wantHeight int64 = 102542
	const wantNano int64 = 1_700_000_000_000_000_000

	// Use a KV subject: StoreKVPair with empty bytes unmarshals to zero values,
	// and noopDecoder.DecodeSupplierKV returns (nil, nil) — no store call needed.
	msg := consumer.Message{
		Height:       wantHeight,
		Subject:      "pokt.kv.supplier.102542",
		MsgID:        "msg-partial-1",
		TimeUnixNano: wantNano,
		Data:         nil, // empty proto bytes — noopDecoder returns nil, no store call
	}

	// Intercept the router call to verify the height passed is from msg.Height.
	var routerCalledWith int64
	capturingRouter := &capturingFakeRouter{
		dec:          noopDecoder{},
		onDecoderFor: func(h int64) { routerCalledWith = h },
	}
	h2 := New(capturingRouter, map[string]int16{"v0.noop": 1})

	if err := h2.FlushHeight(context.Background(), nil, nil, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight nil-env: unexpected error: %v", err)
	}
	if routerCalledWith != wantHeight {
		t.Fatalf("router called with height %d, want %d", routerCalledWith, wantHeight)
	}
	_ = h // suppress unused warning for non-capturing handler
}

// capturingFakeRouter is like fakeRouter but calls onDecoderFor on each invocation.
type capturingFakeRouter struct {
	dec          decoders.Decoder
	err          error
	onDecoderFor func(int64)
}

func (c *capturingFakeRouter) DecoderFor(h int64) (decoders.Decoder, error) {
	if c.onDecoderFor != nil {
		c.onDecoderFor(h)
	}
	return c.dec, c.err
}

// capturePosDecoder returns a SupplierKVRecord with a non-nil Supplier and
// retains the pointer: the handler stamps Position on that exact record BEFORE
// the store call, so the test can assert the Position used for the rows.
type capturePosDecoder struct {
	noopDecoder
	captured *types.SupplierKVRecord
}

func (d *capturePosDecoder) DecodeSupplierKV(_, _ []byte, _ bool) (*types.SupplierKVRecord, error) {
	rec := &types.SupplierKVRecord{Supplier: &types.SupplierSnapshot{OperatorAddress: "pokt1capture"}}
	d.captured = rec
	return rec, nil
}

// fakeTx is a minimal pgx.Tx for unit tests: only Exec is implemented (returns
// success); any other method panics via the nil embedded interface.
type fakeTx struct{ pgx.Tx }

func (fakeTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

// TestFlushHeightNilEnvelope_BlockTimeDerivedFromMessage verifies that in the
// nil-env partial-flush path the Position stamped on rows is EXACTLY
// {Height: msgs[0].Height, Time: time.Unix(0, msgs[0].TimeUnixNano).UTC()}.
// capturePosDecoder retains the decoded record; the handler mutates its
// Position before calling store.InsertSupplierSnapshot (fakeTx absorbs the Exec).
func TestFlushHeightNilEnvelope_BlockTimeDerivedFromMessage(t *testing.T) {
	const wantHeight int64 = 102542
	const wantNano int64 = 1_700_000_000_000_000_000
	wantTime := time.Unix(0, wantNano).UTC()

	dec := &capturePosDecoder{}
	h := New(&fakeRouter{dec: dec}, map[string]int16{"v0.noop": 1})
	msg := consumer.Message{
		Height:       wantHeight,
		Subject:      "pokt.kv.supplier.102542",
		MsgID:        "kv-partial-1",
		TimeUnixNano: wantNano,
		Data:         nil, // empty proto bytes — StoreKVPair unmarshals to zero values
	}
	if err := h.FlushHeight(context.Background(), fakeTx{}, nil, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight nil-env KV path: unexpected error: %v", err)
	}
	if dec.captured == nil || dec.captured.Supplier == nil {
		t.Fatal("decoder was not invoked / record not captured")
	}
	got := dec.captured.Supplier.Position
	if got.Height != wantHeight {
		t.Fatalf("Position.Height = %d, want %d", got.Height, wantHeight)
	}
	if !got.Time.Equal(wantTime) {
		t.Fatalf("Position.Time = %v, want %v", got.Time, wantTime)
	}
	if loc := got.Time.Location(); loc != time.UTC {
		t.Fatalf("Position.Time location = %v, want UTC", loc)
	}
}

// TestFlushHeightNilEnvelope_NilMsgsReturnsError verifies that FlushHeight
// with env==nil AND empty msgs returns an error (no source for block_time).
func TestFlushHeightNilEnvelope_NilMsgsReturnsError(t *testing.T) {
	h := New(&fakeRouter{}, nil)
	err := h.FlushHeight(context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error: partial flush with no messages and no envelope")
	}
}

// TestFlushHeightNilEnvelope_ZeroTimeUnixNanoReturnsError verifies that
// FlushHeight with env==nil and msgs[0].TimeUnixNano==0 returns an error
// (block_time cannot be derived — ADR-022 Pocket-Block-Time header missing).
func TestFlushHeightNilEnvelope_ZeroTimeUnixNanoReturnsError(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	msg := consumer.Message{
		Height:       102542,
		Subject:      "pokt.tx.102542.0",
		MsgID:        "no-time-partial",
		TimeUnixNano: 0, // no Pocket-Block-Time header
		Data:         nil,
	}
	err := h.FlushHeight(context.Background(), nil, nil, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected error: partial flush requires Pocket-Block-Time (msgs[0].TimeUnixNano > 0)")
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
