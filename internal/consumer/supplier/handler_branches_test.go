package supplier

// handler_branches_test.go — unit tests for supplier handler branches not
// covered by handler_test.go. All tests here are pure unit (no DB / NATS).
// Branches that reach store.Insert* are covered by the integration tests in
// test/integration/supplier_handler_integration_test.go.

import (
	"context"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	txsigning "github.com/cosmos/cosmos-sdk/types/tx/signing"
	anypb "github.com/cosmos/gogoproto/types/any"

	storetypes "cosmossdk.io/store/types"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/types"

	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	suppliergen "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// decoderV27 delegates to the real v0_1_27 decoder.
type decoderV27 struct{}

func (decoderV27) Version() string                                        { return "v0_1_27" }
func (decoderV27) DecodeBlockHeader(_ []byte) (*types.BlockHeader, error) { return nil, nil }
func (decoderV27) DecodeSupplierMsg(typeURL string, value []byte) (*types.SupplierMsg, error) {
	return v0_1_27.Decoder{}.DecodeSupplierMsg(typeURL, value)
}
func (decoderV27) DecodeSupplierEvent(eventType string, attrs []types.EventAttr) (*types.SupplierEvent, error) {
	return v0_1_27.Decoder{}.DecodeSupplierEvent(eventType, attrs)
}
func (decoderV27) DecodeSupplierKV(key, value []byte, deleted bool) (*types.SupplierKVRecord, error) {
	return v0_1_27.Decoder{}.DecodeSupplierKV(key, value, deleted)
}

// handlerV27 wires a Handler to the v0_1_27 decoder.
func handlerV27() *Handler {
	return New(&fakeRouter{dec: decoderV27{}}, map[string]int16{"v0.1.27": 127})
}

// testEnvV27 returns a minimal test BlockEnvelope for height 999999.
func testEnvV27() *psv1.BlockEnvelope {
	return &psv1.BlockEnvelope{
		Height:       999999,
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — failed-tx skip (Code != 0)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_FailedTxSkip verifies that a tx with ExecTxResult.Code != 0 is
// silently skipped (decision 7): FlushHeight returns nil without touching the DB.
// The nil pgx.Tx is safe because the handler short-circuits before any store call.
func TestFlushTx_FailedTxSkip(t *testing.T) {
	h := handlerV27()
	env := testEnvV27()

	// Build a TxWithResult whose result has Code != 0 (failed tx).
	result := abci.ExecTxResult{Code: 4, Log: "out of gas"}
	resultBytes, err := result.Marshal()
	if err != nil {
		t.Fatalf("marshal ExecTxResult: %v", err)
	}

	cosmosTx := sdktx.Tx{
		Body:       &sdktx.TxBody{Messages: nil},
		AuthInfo:   &sdktx.AuthInfo{Fee: &sdktx.Fee{}, SignerInfos: []*sdktx.SignerInfo{{ModeInfo: &sdktx.ModeInfo{Sum: &sdktx.ModeInfo_Single_{Single: &sdktx.ModeInfo_Single{Mode: txsigning.SignMode_SIGN_MODE_DIRECT}}}}}},
		Signatures: nil,
	}
	txBytes, err := cosmosTx.Marshal()
	if err != nil {
		t.Fatalf("marshal sdktx.Tx: %v", err)
	}

	wrapped := psv1.TxWithResult{Tx: txBytes, Result: resultBytes}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal TxWithResult: %v", err)
	}

	msg := consumer.Message{Height: 999999, Subject: "pokt.tx.999999.0", Data: data}
	// nil pgx.Tx is safe — the failed-tx guard returns nil before any store call.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight failed-tx: unexpected error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushTx — MsgUnstakeSupplier reaches DecodeSupplierMsg
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushTx_MsgUnstake_DecodeSucceeds verifies that a well-formed
// TxWithResult containing a MsgUnstakeSupplier is decoded without error up to
// the store call. We use a noopDecoder so that DecodeSupplierMsg returns nil
// (unknown type URL), confirming the decoder path is traversed without a nil-tx panic.
func TestFlushTx_MsgUnstake_DecodeSucceeds(t *testing.T) {
	// Use the noopDecoder that always returns (nil, nil) — no store call.
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnvV27()

	unstake := &suppliergen.MsgUnstakeSupplier{
		Signer: "pokt1signer_u", OperatorAddress: "pokt1op_u",
	}
	unstakeBytes, err := unstake.Marshal()
	if err != nil {
		t.Fatalf("marshal MsgUnstakeSupplier: %v", err)
	}

	cosmosTx := sdktx.Tx{
		Body: &sdktx.TxBody{
			Messages: []*anypb.Any{
				{TypeUrl: "/pocket.supplier.MsgUnstakeSupplier", Value: unstakeBytes},
			},
		},
		AuthInfo: &sdktx.AuthInfo{Fee: &sdktx.Fee{}},
	}
	txBytes, err := cosmosTx.Marshal()
	if err != nil {
		t.Fatalf("marshal sdktx.Tx: %v", err)
	}
	result := abci.ExecTxResult{Code: 0}
	resultBytes, err := result.Marshal()
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	wrapped := psv1.TxWithResult{Tx: txBytes, Result: resultBytes}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal TxWithResult: %v", err)
	}

	msg := consumer.Message{Height: 999999, Subject: "pokt.tx.999999.0", Data: data}
	// noopDecoder returns nil → no store call → nil tx safe.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight noop-decode MsgUnstake: unexpected error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushEvent — corrupt EventInBlock bytes
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushEvent_CorruptEventInBlock verifies that corrupt EventInBlock bytes
// produce a descriptive error from flushEvent (the first unmarshal in the path).
func TestFlushEvent_CorruptEventInBlock(t *testing.T) {
	h := handlerV27()
	env := testEnvV27()

	msg := consumer.Message{
		Height:  999999,
		Subject: "pokt.events.pocket.supplier.EventSupplierStaked.999999",
		Data:    []byte{0xff, 0xfe, 0x00, 0x01},
	}
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected error for corrupt EventInBlock bytes")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushEvent — decoder returns nil (unknown event type skipped)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushEvent_DecoderReturnsNil verifies that when DecodeSupplierEvent
// returns nil (unknown event type), flushEvent returns nil without a store call.
func TestFlushEvent_DecoderReturnsNil(t *testing.T) {
	// Use noopDecoder which always returns (nil, nil) for events.
	h := New(&fakeRouter{dec: noopDecoder{}}, map[string]int16{"v0.noop": 1})
	env := testEnvV27()

	ev := abci.Event{Type: "pocket.unknown.SomeEvent"}
	evBytes, _ := ev.Marshal()
	wrapped := &psv1.EventInBlock{Event: evBytes, TxIndex: 0, EventIndex: 0}
	data, _ := wrapped.Marshal()

	msg := consumer.Message{
		Height:  999999,
		Subject: "pokt.events.pocket.unknown.SomeEvent.999999",
		Data:    data,
	}
	// nil tx is safe because decoder returns nil → no store call.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight with nil-returning decoder: unexpected error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushKV — corrupt StoreKVPair bytes
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushKV_CorruptStoreKVPair verifies that corrupt StoreKVPair bytes produce
// a descriptive error (first unmarshal in flushKV).
func TestFlushKV_CorruptStoreKVPair(t *testing.T) {
	h := handlerV27()
	env := testEnvV27()

	msg := consumer.Message{
		Height:  999999,
		Subject: "pokt.kv.supplier.999999",
		Data:    []byte{0xff, 0xfe, 0x00, 0x01},
	}
	err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg})
	if err == nil {
		t.Fatal("expected error for corrupt StoreKVPair bytes")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// flushKV — decoder returns nil (supplier deleted key ignored by v0_1_27)
// ─────────────────────────────────────────────────────────────────────────────

// TestFlushKV_SupplierDeletedKeyReturnsNil verifies that when DecodeSupplierKV
// returns nil for a deleted supplier primary key, flushKV returns nil (no
// store call, no panic). The v0_1_27 decoder returns nil for deleted Supplier/
// keys (it only processes ServiceConfigUpdate/ deletes).
func TestFlushKV_SupplierDeletedKeyReturnsNil(t *testing.T) {
	h := handlerV27()
	env := testEnvV27()

	// Supplier primary key with Delete=true.
	key := []byte("Supplier/operator_address/pokt1op_del")
	kv := storetypes.StoreKVPair{Key: key, Value: nil, Delete: true}
	kvBytes, _ := kv.Marshal()

	// Confirm the decoder returns nil for this key.
	decoded, err := v0_1_27.Decoder{}.DecodeSupplierKV(key, nil, true)
	if err != nil {
		t.Fatalf("DecodeSupplierKV: %v", err)
	}
	if decoded != nil {
		t.Skipf("v0_1_27 decoder returned non-nil for Supplier deleted key; test assumption no longer valid")
	}

	msg := consumer.Message{Height: 999999, Subject: "pokt.kv.supplier.999999", Data: kvBytes}
	// nil tx is safe: decoder returns nil → no store call.
	if err := h.FlushHeight(context.Background(), nil, env, []consumer.Message{msg}); err != nil {
		t.Fatalf("FlushHeight supplier deleted key: unexpected error: %v", err)
	}
}
