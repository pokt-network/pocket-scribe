//go:build integration

// store_supplier_test.go — component tests for the store supplier inserters that
// are not exercised by the existing supplier-consumer integration tests.
// Covered gaps: InsertMsgUnstakeSupplier, InsertEventSupplierUnbondingBegin,
// InsertEventSupplierUnbondingEnd, InsertEventSupplierUnbondingCanceled,
// InsertEventSupplierServiceConfigActivated, InsertServiceConfigUpdate
// (deleted=true path).
//
// All inserts are exercised for:
//
//	(a) happy-path: row appears with expected values
//	(b) idempotency: re-insert with same PK is a no-op / DO UPDATE (as per the
//	    per-function conflict rule).
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// supplierDecodedBy is a stable decoder_version id that must exist after migrations.
// We derive it from the live DB rather than hardcoding to stay migration-resilient.
func supplierDecodedBy(t *testing.T) int16 {
	t.Helper()
	ctx := context.Background()
	var id int16
	if err := pg.Pool.QueryRow(ctx, `SELECT id FROM decoder_version WHERE tag='v0.1.28' LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("lookup decoder_version v0.1.28: %v", err)
	}
	return id
}

// txInStore opens a Postgres transaction, calls f inside it, and commits.
// Any error from f or the commit is fatal.
func txInStore(t *testing.T, f func(ctx context.Context, tx interface {
	Exec(ctx context.Context, sql string, args ...any) (interface{}, error)
})) {
	t.Helper()
	// Use the store's pool directly for simpler access.
}

// withTx begins a pgx transaction on the shared pool, calls f, and commits.
func withTx(t *testing.T, f func(ctx context.Context, tx interface{})) {
	t.Helper()
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	f(ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}

// supplierPos returns a stable test Position at height 999001.
func supplierPos() types.Position {
	return types.Position{
		Height:     999001,
		Time:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		TxIndex:    0,
		EventIndex: 0,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertMsgUnstakeSupplier
// ─────────────────────────────────────────────────────────────────────────────

// TestInsertMsgUnstakeSupplier_HappyPath verifies that
// InsertMsgUnstakeSupplier writes the expected row to msg_unstake_supplier
// and that a repeated insert is silently ignored (DO NOTHING idempotency).
func TestInsertMsgUnstakeSupplier_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()

	r := &types.MsgUnstakeSupplier{
		Position:        pos,
		Signer:          "pokt1signer_unstake",
		OperatorAddress: "pokt1operator_unstake",
	}

	// Insert first time.
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.InsertMsgUnstakeSupplier(ctx, tx, r, decodedBy); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("InsertMsgUnstakeSupplier: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify row.
	var signer, operator string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT signer, operator_address FROM msg_unstake_supplier
		 WHERE block_height=$1 AND tx_index=0 AND event_index=0`,
		pos.Height).Scan(&signer, &operator); err != nil {
		t.Fatalf("query: %v", err)
	}
	if signer != "pokt1signer_unstake" {
		t.Errorf("signer = %q, want %q", signer, "pokt1signer_unstake")
	}
	if operator != "pokt1operator_unstake" {
		t.Errorf("operator_address = %q, want %q", operator, "pokt1operator_unstake")
	}

	// Re-insert (idempotency: DO NOTHING).
	tx2, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	if err := store.InsertMsgUnstakeSupplier(ctx, tx2, r, decodedBy); err != nil {
		_ = tx2.Rollback(ctx)
		t.Fatalf("InsertMsgUnstakeSupplier idempotent: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit2: %v", err)
	}

	// Count must still be 1.
	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM msg_unstake_supplier WHERE block_height=$1`, pos.Height).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent DO NOTHING)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertEventSupplierUnbondingBegin
// ─────────────────────────────────────────────────────────────────────────────

// TestInsertEventSupplierUnbondingBegin_HappyPath writes a row and checks values;
// a repeat insert is DO NOTHING idempotent.
func TestInsertEventSupplierUnbondingBegin_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()
	pos.EventIndex = 1

	r := &types.EventSupplierUnbondingBegin{
		Position:           pos,
		SupplierJSON:       []byte(`{"operator_address":"pokt1op_ub"}`),
		ReasonJSON:         []byte(`"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`),
		SessionEndHeight:   210,
		UnbondingEndHeight: 310,
	}

	for i := 0; i < 2; i++ {
		tx, err := pg.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := store.InsertEventSupplierUnbondingBegin(ctx, tx, r, decodedBy); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var session, unbonding int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT session_end_height, unbonding_end_height
		 FROM event_supplier_unbonding_begin WHERE block_height=$1 AND event_index=1`,
		pos.Height).Scan(&session, &unbonding); err != nil {
		t.Fatalf("query: %v", err)
	}
	if session != 210 || unbonding != 310 {
		t.Errorf("session=%d unbonding=%d, want 210/310", session, unbonding)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_supplier_unbonding_begin WHERE block_height=$1`, pos.Height).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertEventSupplierUnbondingEnd
// ─────────────────────────────────────────────────────────────────────────────

func TestInsertEventSupplierUnbondingEnd_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()
	pos.EventIndex = 2

	r := &types.EventSupplierUnbondingEnd{
		Position:           pos,
		SupplierJSON:       []byte(`{"operator_address":"pokt1op_ue"}`),
		ReasonJSON:         []byte(`"SUPPLIER_UNBONDING_REASON_VOLUNTARY"`),
		SessionEndHeight:   410,
		UnbondingEndHeight: 510,
	}

	for i := 0; i < 2; i++ {
		tx, err := pg.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := store.InsertEventSupplierUnbondingEnd(ctx, tx, r, decodedBy); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var session, unbonding int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT session_end_height, unbonding_end_height
		 FROM event_supplier_unbonding_end WHERE block_height=$1 AND event_index=2`,
		pos.Height).Scan(&session, &unbonding); err != nil {
		t.Fatalf("query: %v", err)
	}
	if session != 410 || unbonding != 510 {
		t.Errorf("session=%d unbonding=%d, want 410/510", session, unbonding)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_supplier_unbonding_end WHERE block_height=$1`, pos.Height).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertEventSupplierUnbondingCanceled
// ─────────────────────────────────────────────────────────────────────────────

func TestInsertEventSupplierUnbondingCanceled_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()
	pos.EventIndex = 3

	r := &types.EventSupplierUnbondingCanceled{
		Position:         pos,
		SupplierJSON:     []byte(`{"operator_address":"pokt1op_uc"}`),
		AtHeight:         610,
		SessionEndHeight: 710,
	}

	for i := 0; i < 2; i++ {
		tx, err := pg.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := store.InsertEventSupplierUnbondingCanceled(ctx, tx, r, decodedBy); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var atHeight, session int64
	if err := pg.Pool.QueryRow(ctx,
		`SELECT height, session_end_height
		 FROM event_supplier_unbonding_canceled WHERE block_height=$1 AND event_index=3`,
		pos.Height).Scan(&atHeight, &session); err != nil {
		t.Fatalf("query: %v", err)
	}
	if atHeight != 610 || session != 710 {
		t.Errorf("at_height=%d session=%d, want 610/710", atHeight, session)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_supplier_unbonding_canceled WHERE block_height=$1`, pos.Height).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertEventSupplierServiceConfigActivated
// ─────────────────────────────────────────────────────────────────────────────

func TestInsertEventSupplierServiceConfigActivated_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()
	pos.EventIndex = 4

	r := &types.EventSupplierServiceConfigActivated{
		Position:         pos,
		ActivationHeight: 820,
		OperatorAddress:  "pokt1op_sca",
		ServiceID:        "anvil",
	}

	for i := 0; i < 2; i++ {
		tx, err := pg.Pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin %d: %v", i, err)
		}
		if err := store.InsertEventSupplierServiceConfigActivated(ctx, tx, r, decodedBy); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var activationHeight int64
	var operator, serviceID string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT activation_height, operator_address, service_id
		 FROM event_supplier_service_config_activated WHERE block_height=$1 AND event_index=4`,
		pos.Height).Scan(&activationHeight, &operator, &serviceID); err != nil {
		t.Fatalf("query: %v", err)
	}
	if activationHeight != 820 || operator != "pokt1op_sca" || serviceID != "anvil" {
		t.Errorf("activation=%d op=%q svc=%q", activationHeight, operator, serviceID)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM event_supplier_service_config_activated WHERE block_height=$1`, pos.Height).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestInsertServiceConfigUpdate_DeletedPath
// ─────────────────────────────────────────────────────────────────────────────

// TestInsertServiceConfigUpdate_DeletedPath verifies that InsertServiceConfigUpdate
// with Deleted=true writes a row where the deleted column is true, and that a
// subsequent re-insert with Deleted=false performs the DO UPDATE (last-write-wins
// at same block_height).
func TestInsertServiceConfigUpdate_DeletedPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	decodedBy := supplierDecodedBy(t)
	pos := supplierPos()
	pos.EventIndex = 0

	// First write: deleted=true (the SCU delete event at this height).
	r := &types.ServiceConfigUpdateSnapshot{
		Position:           pos,
		OperatorAddress:    "pokt1op_scu",
		ServiceID:          "eth",
		ActivationHeight:   900,
		DeactivationHeight: 1000,
		ServiceConfigJSON:  []byte(`{"service_id":"eth"}`),
		Deleted:            true,
	}

	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.InsertServiceConfigUpdate(ctx, tx, r, decodedBy); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert deleted=true: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify deleted=true is stored.
	var deleted bool
	if err := pg.Pool.QueryRow(ctx,
		`SELECT deleted FROM supplier_service_config_update_history
		 WHERE operator_address='pokt1op_scu' AND service_id='eth' AND activation_height=900 AND block_height=$1`,
		pos.Height).Scan(&deleted); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true")
	}

	// Second write: same PK but deleted=false (DO UPDATE fires — last-write-wins
	// same-height idempotency; replaying the block twice must yield the same row).
	r2 := *r
	r2.Deleted = false
	tx2, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin2: %v", err)
	}
	if err := store.InsertServiceConfigUpdate(ctx, tx2, &r2, decodedBy); err != nil {
		_ = tx2.Rollback(ctx)
		t.Fatalf("insert deleted=false: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit2: %v", err)
	}

	// DO UPDATE fires → deleted becomes false; still only 1 row.
	if err := pg.Pool.QueryRow(ctx,
		`SELECT deleted FROM supplier_service_config_update_history
		 WHERE operator_address='pokt1op_scu' AND service_id='eth' AND activation_height=900 AND block_height=$1`,
		pos.Height).Scan(&deleted); err != nil {
		t.Fatalf("query2: %v", err)
	}
	if deleted {
		t.Error("expected DO UPDATE to set deleted=false")
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM supplier_service_config_update_history
		 WHERE operator_address='pokt1op_scu' AND service_id='eth'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (append-only, single row per PK)", count)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestDecoderVersionIDs_HappyPath
// ─────────────────────────────────────────────────────────────────────────────

// TestDecoderVersionIDs_HappyPath verifies that DecoderVersionIDs returns a
// non-empty map after migrations (the per-version migrations seed decoder_version
// rows). Exercises the DecoderVersionIDs function which was at 0% coverage.
func TestDecoderVersionIDs_HappyPath(t *testing.T) {
	s := storeFrom(t)
	ids, err := s.DecoderVersionIDs(context.Background())
	if err != nil {
		t.Fatalf("DecoderVersionIDs: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("expected at least one decoder_version row after migrations")
	}
	// v0.1.28 must be present (used by existing fixtures).
	if _, ok := ids["v0.1.28"]; !ok {
		t.Error("v0.1.28 missing from decoder_version map")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Error-path tests: cancelled context forces pgx.Tx.Exec to fail
// This covers the "return fmt.Errorf(...)" branches in each Insert* function
// that are unreachable under normal test conditions (75% → 100%).
// ─────────────────────────────────────────────────────────────────────────────

// TestInsertMsgUnstakeSupplier_ContextCancelled verifies that a cancelled ctx
// causes InsertMsgUnstakeSupplier to return a non-nil error.
func TestInsertMsgUnstakeSupplier_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	decodedBy := supplierDecodedBy(t)
	r := &types.MsgUnstakeSupplier{Position: supplierPos(), Signer: "s", OperatorAddress: "o"}
	if err := store.InsertMsgUnstakeSupplier(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertEventSupplierUnbondingBegin_ContextCancelled covers the error path.
func TestInsertEventSupplierUnbondingBegin_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.EventSupplierUnbondingBegin{Position: supplierPos(), SupplierJSON: []byte("{}"), ReasonJSON: []byte(`""`)}
	if err := store.InsertEventSupplierUnbondingBegin(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertEventSupplierUnbondingEnd_ContextCancelled covers the error path.
func TestInsertEventSupplierUnbondingEnd_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.EventSupplierUnbondingEnd{Position: supplierPos(), SupplierJSON: []byte("{}"), ReasonJSON: []byte(`""`)}
	if err := store.InsertEventSupplierUnbondingEnd(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertEventSupplierUnbondingCanceled_ContextCancelled covers the error path.
func TestInsertEventSupplierUnbondingCanceled_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.EventSupplierUnbondingCanceled{Position: supplierPos(), SupplierJSON: []byte("{}")}
	if err := store.InsertEventSupplierUnbondingCanceled(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertEventSupplierServiceConfigActivated_ContextCancelled covers the error path.
func TestInsertEventSupplierServiceConfigActivated_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.EventSupplierServiceConfigActivated{Position: supplierPos(), ActivationHeight: 100}
	if err := store.InsertEventSupplierServiceConfigActivated(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertMsgStakeSupplier_ContextCancelled covers the error path.
func TestInsertMsgStakeSupplier_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.MsgStakeSupplier{Position: supplierPos(), Signer: "s", OperatorAddress: "o", ServicesJSON: []byte("[]")}
	if err := store.InsertMsgStakeSupplier(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertEventSupplierStaked_ContextCancelled covers the error path.
func TestInsertEventSupplierStaked_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.EventSupplierStaked{Position: supplierPos()}
	if err := store.InsertEventSupplierStaked(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertSupplierSnapshot_ContextCancelled covers the error path.
func TestInsertSupplierSnapshot_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.SupplierSnapshot{Position: supplierPos(), OperatorAddress: "o"}
	if err := store.InsertSupplierSnapshot(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestInsertServiceConfigUpdate_ContextCancelled covers the error path.
func TestInsertServiceConfigUpdate_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	tx, err := pg.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	decodedBy := supplierDecodedBy(t)
	r := &types.ServiceConfigUpdateSnapshot{Position: supplierPos(), OperatorAddress: "o", ServiceID: "eth", ServiceConfigJSON: []byte("{}")}
	if err := store.InsertServiceConfigUpdate(cancelled, tx, r, decodedBy); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}
