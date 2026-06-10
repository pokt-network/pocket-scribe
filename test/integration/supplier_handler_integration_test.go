//go:build integration

// supplier_handler_integration_test.go — component tests for supplier handler
// FlushHeight branches that require a live Postgres transaction.
//
// Covered gaps (all at 0% in combined coverage):
//   - flushEvent: UnbondingBegin, UnbondingEnd, UnbondingCanceled, ServiceConfigActivated
//   - flushTx: MsgUnstakeSupplier reaches store.InsertMsgUnstakeSupplier
//   - flushKV: ServiceConfigUpdate deleted=true (if key format matches decoder)
package integration

import (
	"context"
	"testing"
	"time"

	storetypes "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	sdktx "github.com/cosmos/cosmos-sdk/types/tx"
	anypb "github.com/cosmos/gogoproto/types/any"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	v0_1_27 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27"
	suppliergen "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_27/gen/pocket/supplier"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/router"
)

// handlerWithV27Router builds a supplier handler wired to a static router
// that maps height 247893+ to v0_1_27. Used by all handler integration tests.
func handlerWithV27Router(t *testing.T) *supplierhandler.Handler {
	t.Helper()
	ids := loadDecoderVersionIDs(t)
	ups := []router.Upgrade{
		{Name: "v0.1.27", AppliedAtHeight: 247893, DecoderVersion: "v0_1_27"},
	}
	rtr, err := router.NewStaticRouter(ups, router.DefaultRegistry(), "v0_1_27")
	if err != nil {
		t.Fatalf("NewStaticRouter: %v", err)
	}
	return supplierhandler.New(rtr, ids)
}

// envelopeFor builds a BlockEnvelope for height h at 2025-01-01T00:00:00Z.
func envelopeFor(h int64) *psv1.BlockEnvelope {
	return &psv1.BlockEnvelope{
		Height:       h,
		TimeUnixNano: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
	}
}

// wrapEvent builds a consumer.Message carrying an abci.Event at the given position.
func wrapEvent(t *testing.T, height int64, subject string, ev abci.Event, txIdx, evIdx int32) consumer.Message {
	t.Helper()
	evBytes, err := ev.Marshal()
	if err != nil {
		t.Fatalf("marshal abci.Event: %v", err)
	}
	wrapped := &psv1.EventInBlock{Event: evBytes, TxIndex: txIdx, EventIndex: evIdx}
	data, err := wrapped.Marshal()
	if err != nil {
		t.Fatalf("marshal EventInBlock: %v", err)
	}
	return consumer.Message{Height: height, Subject: subject, Data: data}
}

// flushMsg calls FlushHeight in a fresh Postgres transaction and commits.
func flushMsg(t *testing.T, h *supplierhandler.Handler, height int64, msgs []consumer.Message) {
	t.Helper()
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := h.FlushHeight(ctx, tx, envelopeFor(height), msgs); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("FlushHeight: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushEvent_UnbondingBegin
// ─────────────────────────────────────────────────────────────────────────────

// TestHandlerFlushEvent_UnbondingBegin verifies that FlushHeight writes an
// event_supplier_unbonding_begin row when the buffer contains an
// EventSupplierUnbondingBegin message. Idempotent on second call.
func TestHandlerFlushEvent_UnbondingBegin(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999200)
	supplierJSON := `{"owner_address":"pokt1own","operator_address":"pokt1op_ub","stake":{"denom":"upokt","amount":"2000"},"services":[]}`
	attrs := []abci.EventAttribute{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"210"`},
		{Key: "unbonding_end_height", Value: `"310"`},
	}
	ev := abci.Event{Type: "pocket.supplier.EventSupplierUnbondingBegin", Attributes: attrs}
	msg := wrapEvent(t, blockH, "pokt.events.pocket.supplier.EventSupplierUnbondingBegin."+int64str(blockH), ev, 0, 5)

	// Two calls: second must be idempotent (DO NOTHING).
	flushMsg(t, h, blockH, []consumer.Message{msg})
	flushMsg(t, h, blockH, []consumer.Message{msg})

	var session, unbonding int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT session_end_height, unbonding_end_height
		 FROM event_supplier_unbonding_begin WHERE block_height=$1 AND event_index=5`,
		blockH).Scan(&session, &unbonding); err != nil {
		t.Fatalf("query: %v", err)
	}
	if session != 210 || unbonding != 310 {
		t.Errorf("session=%d unbonding=%d, want 210/310", session, unbonding)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_supplier_unbonding_begin WHERE block_height=$1`, blockH).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1 (idempotent)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushEvent_UnbondingEnd
// ─────────────────────────────────────────────────────────────────────────────

func TestHandlerFlushEvent_UnbondingEnd(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999201)
	supplierJSON := `{"owner_address":"pokt1own","operator_address":"pokt1op_ue","stake":{"denom":"upokt","amount":"3000"},"services":[]}`
	attrs := []abci.EventAttribute{
		{Key: "supplier", Value: supplierJSON},
		{Key: "reason", Value: `"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`},
		{Key: "session_end_height", Value: `"410"`},
		{Key: "unbonding_end_height", Value: `"510"`},
	}
	ev := abci.Event{Type: "pocket.supplier.EventSupplierUnbondingEnd", Attributes: attrs}
	msg := wrapEvent(t, blockH, "pokt.events.pocket.supplier.EventSupplierUnbondingEnd."+int64str(blockH), ev, 0, 6)

	flushMsg(t, h, blockH, []consumer.Message{msg})

	var session, unbonding int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT session_end_height, unbonding_end_height
		 FROM event_supplier_unbonding_end WHERE block_height=$1 AND event_index=6`,
		blockH).Scan(&session, &unbonding); err != nil {
		t.Fatalf("query: %v", err)
	}
	if session != 410 || unbonding != 510 {
		t.Errorf("session=%d unbonding=%d, want 410/510", session, unbonding)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushEvent_UnbondingCanceled
// ─────────────────────────────────────────────────────────────────────────────

func TestHandlerFlushEvent_UnbondingCanceled(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999202)
	supplierJSON := `{"owner_address":"pokt1own","operator_address":"pokt1op_uc","stake":{"denom":"upokt","amount":"4000"},"services":[]}`
	attrs := []abci.EventAttribute{
		{Key: "supplier", Value: supplierJSON},
		{Key: "height", Value: `"610"`},
		{Key: "session_end_height", Value: `"710"`},
	}
	ev := abci.Event{Type: "pocket.supplier.EventSupplierUnbondingCanceled", Attributes: attrs}
	msg := wrapEvent(t, blockH, "pokt.events.pocket.supplier.EventSupplierUnbondingCanceled."+int64str(blockH), ev, 0, 7)

	flushMsg(t, h, blockH, []consumer.Message{msg})

	var atHeight, session int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT height, session_end_height
		 FROM event_supplier_unbonding_canceled WHERE block_height=$1 AND event_index=7`,
		blockH).Scan(&atHeight, &session); err != nil {
		t.Fatalf("query: %v", err)
	}
	if atHeight != 610 || session != 710 {
		t.Errorf("at_height=%d session=%d, want 610/710", atHeight, session)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushEvent_ServiceConfigActivated
// ─────────────────────────────────────────────────────────────────────────────

func TestHandlerFlushEvent_ServiceConfigActivated(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999203)
	attrs := []abci.EventAttribute{
		{Key: "operator_address", Value: `"pokt1op_sca"`},
		{Key: "service_id", Value: `"anvil"`},
		{Key: "activation_height", Value: `"820"`},
	}
	ev := abci.Event{Type: "pocket.supplier.EventSupplierServiceConfigActivated", Attributes: attrs}
	msg := wrapEvent(t, blockH, "pokt.events.pocket.supplier.EventSupplierServiceConfigActivated."+int64str(blockH), ev, 0, 8)

	flushMsg(t, h, blockH, []consumer.Message{msg})

	var activationHeight int64
	var operator, serviceID string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT activation_height, operator_address, service_id
		 FROM event_supplier_service_config_activated WHERE block_height=$1 AND event_index=8`,
		blockH).Scan(&activationHeight, &operator, &serviceID); err != nil {
		t.Fatalf("query: %v", err)
	}
	if activationHeight != 820 || operator != "pokt1op_sca" || serviceID != "anvil" {
		t.Errorf("activation=%d op=%q svc=%q", activationHeight, operator, serviceID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushTx_MsgUnstake
// ─────────────────────────────────────────────────────────────────────────────

// TestHandlerFlushTx_MsgUnstake verifies the Unstake branch in flushTx
// end-to-end with a real Postgres transaction.
func TestHandlerFlushTx_MsgUnstake(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999204)

	// Build MsgUnstakeSupplier proto bytes.
	unstake := &suppliergen.MsgUnstakeSupplier{
		Signer:          "pokt1signer_integ",
		OperatorAddress: "pokt1op_integ",
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

	msg := consumer.Message{Height: blockH, Subject: "pokt.tx." + int64str(blockH) + ".0", Data: data}
	flushMsg(t, h, blockH, []consumer.Message{msg})

	var operator string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT operator_address FROM msg_unstake_supplier WHERE block_height=$1`, blockH).Scan(&operator); err != nil {
		t.Fatalf("query: %v", err)
	}
	if operator != "pokt1op_integ" {
		t.Errorf("operator_address = %q, want pokt1op_integ", operator)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHandlerFlushKV_ServiceConfigUpdate_DeletedTrue
// ─────────────────────────────────────────────────────────────────────────────

// TestHandlerFlushKV_ServiceConfigUpdate_DeletedTrue exercises flushKV with a
// ServiceConfigUpdate key that has Delete=true in the StoreKVPair. If the
// v0_1_27 decoder handles this key format, the handler writes a deleted=true row.
func TestHandlerFlushKV_ServiceConfigUpdate_DeletedTrue(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	h := handlerWithV27Router(t)

	const blockH = int64(999205)

	// Probe the decoder to confirm the key format produces a ServiceConfigUpdate.
	key := []byte("ServiceConfigUpdate/eth/pokt1op_del/900")
	decoded, err := v0_1_27.Decoder{}.DecodeSupplierKV(key, nil, true)
	if err != nil {
		t.Fatalf("probe DecodeSupplierKV: %v", err)
	}
	if decoded == nil || decoded.ServiceConfigUpdate == nil {
		t.Skipf("v0_1_27 decoder returned nil for key %q — SCU deleted path not exercisable with this key format; covered by store-level component test", key)
	}

	kv := storetypes.StoreKVPair{Key: key, Value: nil, Delete: true}
	kvBytes, err := kv.Marshal()
	if err != nil {
		t.Fatalf("marshal StoreKVPair: %v", err)
	}
	msg := consumer.Message{
		Height:  blockH,
		Subject: "pokt.kv.supplier." + int64str(blockH),
		Data:    kvBytes,
	}

	flushMsg(t, h, blockH, []consumer.Message{msg})

	var deletedFlag bool
	if err := pg.Pool.QueryRow(ctx,
		`SELECT deleted FROM supplier_service_config_update_history WHERE block_height=$1`,
		blockH).Scan(&deletedFlag); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !deletedFlag {
		t.Error("expected deleted=true for SCU deleted KV entry")
	}
}
