package supplier

// handler_errors_test.go — covers flushTx/flushEvent/flushKV error paths and
// the "non-nil decoded, no known sub-type" fall-through branches not exercised
// by handler_branches_test.go or the integration suite. All tests are pure unit
// (no DB / NATS required).

import (
	"context"
	"errors"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	anypb "github.com/cosmos/gogoproto/types/any"
	"github.com/jackc/pgx/v5/pgconn"

	storetypes "cosmossdk.io/store/types"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/types"

	suppliergen "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test doubles
// ─────────────────────────────────────────────────────────────────────────────

// errTx is a pgx.Tx whose Exec always returns the provided error.
type errTx struct {
	fakeTx
	err error
}

func (e errTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, e.err
}

// errorDecoder returns errors from all Decode methods.
type errorDecoder struct {
	noopDecoder
	decodeErr error
}

func (d errorDecoder) DecodeSupplierMsg(_ string, _ []byte) (*types.SupplierMsg, error) {
	return nil, d.decodeErr
}
func (d errorDecoder) DecodeSupplierEvent(_ string, _ []types.EventAttr) (*types.SupplierEvent, error) {
	return nil, d.decodeErr
}
func (d errorDecoder) DecodeSupplierKV(_, _ []byte, _ bool) (*types.SupplierKVRecord, error) {
	return nil, d.decodeErr
}

// emptyEventDecoder returns a non-nil SupplierEvent with no recognised sub-type —
// exercises the trailing "return nil" in flushEvent.
type emptyEventDecoder struct{ noopDecoder }

func (emptyEventDecoder) DecodeSupplierEvent(_ string, _ []types.EventAttr) (*types.SupplierEvent, error) {
	return &types.SupplierEvent{}, nil
}

// emptyKVDecoder returns a non-nil SupplierKVRecord with no recognised sub-type —
// exercises the trailing "return nil" in flushKV.
type emptyKVDecoder struct{ noopDecoder }

func (emptyKVDecoder) DecodeSupplierKV(_, _ []byte, _ bool) (*types.SupplierKVRecord, error) {
	return &types.SupplierKVRecord{}, nil
}

// stakeDecoder returns a SupplierMsg with Stake populated so flushTx reaches
// store.InsertMsgStakeSupplier.
type stakeDecoder struct{ noopDecoder }

func (stakeDecoder) DecodeSupplierMsg(_ string, _ []byte) (*types.SupplierMsg, error) {
	return &types.SupplierMsg{Stake: &types.MsgStakeSupplier{}}, nil
}

// unstakeDecoder returns a SupplierMsg with Unstake populated so flushTx
// reaches store.InsertMsgUnstakeSupplier.
type unstakeDecoder struct{ noopDecoder }

func (unstakeDecoder) DecodeSupplierMsg(_ string, _ []byte) (*types.SupplierMsg, error) {
	return &types.SupplierMsg{Unstake: &types.MsgUnstakeSupplier{}}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Build helpers
// ─────────────────────────────────────────────────────────────────────────────

// validTxEnv returns a complete, well-formed TxWithResult that will pass all
// unmarshal steps and result in code==0 (success). The tx body has a single
// message with the given typeURL and value bytes.
func validTxEnv(t *testing.T, typeURL string, value []byte) consumer.Message {
	t.Helper()
	result := abci.ExecTxResult{Code: 0}
	resultBytes, err := result.Marshal()
	if err != nil {
		t.Fatalf("marshal ExecTxResult: %v", err)
	}
	tx := sdktx.Tx{
		Body:     &sdktx.TxBody{Messages: []*anypb.Any{{TypeUrl: typeURL, Value: value}}},
		AuthInfo: &sdktx.AuthInfo{Fee: &sdktx.Fee{}},
	}
	txBytes, err := tx.Marshal()
	if err != nil {
		t.Fatalf("marshal sdktx.Tx: %v", err)
	}
	wrapped := psv1.TxWithResult{Tx: txBytes, Result: resultBytes}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal TxWithResult: %v", err)
	}
	return consumer.Message{
		Height:       999,
		Subject:      "pokt.tx.999.0",
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		Data:         data,
	}
}

// validEventMsg returns a well-formed EventInBlock message containing a valid
// abci.Event with the given type and no attributes.
func validEventMsg(t *testing.T, height int64, subject string) consumer.Message {
	t.Helper()
	ev := abci.Event{Type: "pocket.supplier.EventSupplierStaked"}
	evBytes, err := ev.Marshal()
	if err != nil {
		t.Fatalf("marshal abci.Event: %v", err)
	}
	wrapped := psv1.EventInBlock{Event: evBytes, TxIndex: 0, EventIndex: 0}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal EventInBlock: %v", err)
	}
	return consumer.Message{
		Height:       height,
		Subject:      subject,
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		Data:         data,
	}
}

// validKVMsg returns a well-formed KV message with an empty StoreKVPair.
func validKVMsg(height int64) consumer.Message {
	kv := storetypes.StoreKVPair{}
	kvBytes, _ := kv.Marshal()
	return consumer.Message{
		Height:       height,
		Subject:      "pokt.kv.supplier.999",
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		Data:         kvBytes,
	}
}

func testEnv999() *psv1.BlockEnvelope {
	return &psv1.BlockEnvelope{
		Height:       999,
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — bad tx subject (HeightFromTxSubject error)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_BadSubject_HeightParseError verifies that flushTx propagates the
// error returned by HeightFromTxSubject when the subject is malformed.
// The subject passes IsTxSubject (prefix matches) but the height token is not
// a valid integer, triggering the error branch at line 117.
func TestFlushTx_BadSubject_HeightParseError(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	// "pokt.tx.NOT_AN_INT.0" passes IsTxSubject but fails HeightFromTxSubject.
	msg := consumer.Message{
		Height:  999,
		Subject: "pokt.tx.NOT_AN_INT.0",
		Data:    []byte{},
	}
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected HeightFromTxSubject parse error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — corrupt TxWithResult (unmarshal error)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_CorruptTxWithResult verifies that corrupt TxWithResult bytes
// propagate a descriptive error (line 121-122 in flushTx).
func TestFlushTx_CorruptTxWithResult(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	msg := consumer.Message{
		Height:  999,
		Subject: "pokt.tx.999.0",
		Data:    []byte{0xff, 0xfe, 0xfd}, // corrupt proto bytes
	}
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected TxWithResult unmarshal error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — corrupt ExecTxResult (unmarshal error)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_CorruptExecTxResult verifies that corrupt ExecTxResult bytes
// in the Result field propagate an error (line 126-128 in flushTx). We craft
// a TxWithResult whose Result field has an invalid byte sequence.
func TestFlushTx_CorruptExecTxResult(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	// Build a TxWithResult with corrupt Result bytes (non-empty so the unmarshal
	// branch is entered, but not valid ExecTxResult proto).
	wrapped := psv1.TxWithResult{
		Tx:     []byte{}, // empty Tx is fine; Result is checked first
		Result: []byte{0xff, 0xfe, 0xfd},
	}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal TxWithResult: %v", err)
	}
	msg := consumer.Message{
		Height:  999,
		Subject: "pokt.tx.999.0",
		Data:    data,
	}
	err = h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected ExecTxResult unmarshal error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — corrupt cosmosTx (unmarshal error)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_CorruptCosmosTx verifies that a corrupt Tx field in TxWithResult
// propagates an error after the ExecTxResult.Code==0 check passes (line 134-135).
func TestFlushTx_CorruptCosmosTx(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	result := abci.ExecTxResult{Code: 0}
	resultBytes, err := result.Marshal()
	if err != nil {
		t.Fatalf("marshal ExecTxResult: %v", err)
	}
	wrapped := psv1.TxWithResult{
		Tx:     []byte{0xff, 0xfe, 0xfd}, // corrupt sdktx.Tx bytes
		Result: resultBytes,
	}
	data, merr := wrapped.Marshal()
	if merr != nil {
		t.Fatalf("marshal TxWithResult: %v", merr)
	}
	msg := consumer.Message{Height: 999, Subject: "pokt.tx.999.0", Data: data}
	err = h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected cosmosTx unmarshal error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — DecodeSupplierMsg error
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_DecodeSupplierMsgError verifies that an error from
// dec.DecodeSupplierMsg is propagated (line 139-140).
func TestFlushTx_DecodeSupplierMsgError(t *testing.T) {
	decErr := errors.New("decoder: unknown type")
	h := New(&fakeRouter{dec: errorDecoder{decodeErr: decErr}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	// Build a valid tx with a message so the decode loop is entered.
	stakeMsg := &suppliergen.MsgStakeSupplier{Signer: "pokt1s", OperatorAddress: "pokt1o"}
	stakeBytes, err := stakeMsg.Marshal()
	if err != nil {
		t.Fatalf("marshal MsgStakeSupplier: %v", err)
	}
	msg := validTxEnv(t, "/pocket.supplier.MsgStakeSupplier", stakeBytes)
	err = h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected DecodeSupplierMsg error to be propagated")
	}
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decErr in chain, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — InsertMsgStakeSupplier error
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_InsertMsgStakeSupplierError verifies that a DB error from
// store.InsertMsgStakeSupplier is propagated (line 150-152).
func TestFlushTx_InsertMsgStakeSupplierError(t *testing.T) {
	dbErr := errors.New("db: connection closed")
	h := New(&fakeRouter{dec: stakeDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	// Build a valid tx; stakeDecoder returns a Stake record so the insert is attempted.
	stakeMsg := &suppliergen.MsgStakeSupplier{Signer: "pokt1s", OperatorAddress: "pokt1o"}
	stakeBytes, err := stakeMsg.Marshal()
	if err != nil {
		t.Fatalf("marshal MsgStakeSupplier: %v", err)
	}
	msg := validTxEnv(t, "/pocket.supplier.MsgStakeSupplier", stakeBytes)
	err = h.FlushHeight(context.Background(), errTx{err: dbErr}, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected InsertMsgStakeSupplier error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — InsertMsgUnstakeSupplier error
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_InsertMsgUnstakeSupplierError verifies that a DB error from
// store.InsertMsgUnstakeSupplier is propagated (line 155-157).
func TestFlushTx_InsertMsgUnstakeSupplierError(t *testing.T) {
	dbErr := errors.New("db: connection closed")
	h := New(&fakeRouter{dec: unstakeDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	stakeMsg := &suppliergen.MsgStakeSupplier{Signer: "pokt1u", OperatorAddress: "pokt1u"}
	stakeBytes, err := stakeMsg.Marshal()
	if err != nil {
		t.Fatalf("marshal MsgStakeSupplier: %v", err)
	}
	// The subject and message content don't matter to unstakeDecoder — it always
	// returns an Unstake record regardless of typeURL.
	msg := validTxEnv(t, "/pocket.supplier.MsgUnstakeSupplier", stakeBytes)
	err = h.FlushHeight(context.Background(), errTx{err: dbErr}, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected InsertMsgUnstakeSupplier error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushEvent — corrupt abci.Event (unmarshal error)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushEvent_CorruptAbciEvent verifies that corrupt abci.Event bytes inside
// a well-formed EventInBlock produce an error (line 169-170).
func TestFlushEvent_CorruptAbciEvent(t *testing.T) {
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	// Wrap corrupt abci.Event bytes in a valid EventInBlock.
	wrapped := psv1.EventInBlock{Event: []byte{0xff, 0xfe, 0xfd}, TxIndex: 0, EventIndex: 0}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal EventInBlock: %v", err)
	}
	msg := consumer.Message{
		Height:  999,
		Subject: "pokt.events.pocket.supplier.EventSupplierStaked.999",
		Data:    data,
	}
	err = h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected abci.Event unmarshal error to be propagated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushEvent — DecodeSupplierEvent error
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushEvent_DecodeError verifies that an error from DecodeSupplierEvent is
// propagated (line 177-178).
func TestFlushEvent_DecodeError(t *testing.T) {
	decErr := errors.New("decode: bad attribute")
	h := New(&fakeRouter{dec: errorDecoder{decodeErr: decErr}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	msg := validEventMsg(t, 999, "pokt.events.pocket.supplier.EventSupplierStaked.999")
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected DecodeSupplierEvent error to be propagated")
	}
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decErr in chain, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushEvent — non-nil decoded with no known sub-type (trailing return nil)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushEvent_NonNilEmptyDecoded verifies that when DecodeSupplierEvent
// returns a non-nil *SupplierEvent with no sub-type set, flushEvent returns nil
// without attempting any store call (line 202 — trailing return nil).
func TestFlushEvent_NonNilEmptyDecoded(t *testing.T) {
	h := New(&fakeRouter{dec: emptyEventDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	msg := validEventMsg(t, 999, "pokt.events.pocket.supplier.EventSupplierStaked.999")
	// nil tx is safe: emptyEventDecoder triggers the trailing return nil — no store call.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("expected nil for empty SupplierEvent, got: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushKV — DecodeSupplierKV error
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushKV_DecodeError verifies that an error from DecodeSupplierKV is
// propagated (line 211-212).
func TestFlushKV_DecodeError(t *testing.T) {
	decErr := errors.New("decode: malformed key")
	h := New(&fakeRouter{dec: errorDecoder{decodeErr: decErr}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	msg := validKVMsg(999)
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected DecodeSupplierKV error to be propagated")
	}
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decErr in chain, got %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushKV — non-nil decoded with no known sub-type (trailing return nil)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushKV_NonNilEmptyDecoded verifies that when DecodeSupplierKV returns a
// non-nil *SupplierKVRecord with no sub-type set, flushKV returns nil without
// attempting any store call (line 225 — trailing return nil).
func TestFlushKV_NonNilEmptyDecoded(t *testing.T) {
	h := New(&fakeRouter{dec: emptyKVDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnv999()

	msg := validKVMsg(999)
	// nil tx is safe: emptyKVDecoder triggers the trailing return nil — no store call.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("expected nil for empty SupplierKVRecord, got: %v", err)
	}
}
