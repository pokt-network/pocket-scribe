package consumer

// consumer_test.go — covers the NewCmd() construction, the storeInserter adapter,
// and the envOr helper. The RunE bodies require live DB/NATS and are not exercised.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test double: minimal pgx.Tx implementation
// ─────────────────────────────────────────────────────────────────────────────

// noopTx is a pgx.Tx whose Exec always returns success; all other methods
// are unused in unit tests and left to the nil embedded interface.
type noopTx struct{ pgx.Tx }

func (noopTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// NewCmd
// ─────────────────────────────────────────────────────────────────────────────

// TestNewCmd_SubcommandsRegistered verifies that NewCmd registers the expected
// consumer sub-commands.
func TestNewCmd_SubcommandsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	want := map[string]bool{"block": false, "supplier": false}
	for _, sub := range cmd.Commands() {
		if _, ok := want[sub.Use]; ok {
			want[sub.Use] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("subcommand %q not registered", name)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// storeInserter.InsertBlock
// ─────────────────────────────────────────────────────────────────────────────

// TestStoreInserterInsertBlock verifies that storeInserter.InsertBlock delegates
// to store.InsertBlock via the tx.Exec call. A noopTx absorbs the Exec so no
// real DB connection is required.
func TestStoreInserterInsertBlock(t *testing.T) {
	si := storeInserter{}
	hdr := &types.BlockHeader{Height: 42}
	// noopTx.Exec returns success; store.InsertBlock should therefore return nil.
	if err := si.InsertBlock(context.Background(), noopTx{}, hdr); err != nil {
		t.Fatalf("storeInserter.InsertBlock: unexpected error: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// envOr
// ─────────────────────────────────────────────────────────────────────────────

// TestEnvOr_EnvVarSet verifies that envOr returns the env var value when set.
func TestEnvOr_EnvVarSet(t *testing.T) {
	const key = "PS_TEST_CONSUMER_DSN_UNIQUE"
	const want = "postgres-dsn-placeholder-value"
	t.Setenv(key, want)
	if got := envOr(key, "fallback"); got != want {
		t.Fatalf("envOr with set env: got %q, want %q", got, want)
	}
}

// TestEnvOr_EnvVarNotSet verifies that envOr returns the fallback when absent.
func TestEnvOr_EnvVarNotSet(t *testing.T) {
	const key = "PS_TEST_CONSUMER_DSN_UNIQUE"
	t.Setenv(key, "")
	const fallback = "fallback-value"
	if got := envOr(key, fallback); got != fallback {
		t.Fatalf("envOr with unset env: got %q, want %q", got, fallback)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// supplier subcommand: required-flag validation
// ─────────────────────────────────────────────────────────────────────────────

// TestSupplierCmd_ConfigRequired verifies that the supplier subcommand returns
// an error when --config is not provided (cobra required-flag enforcement).
func TestSupplierCmd_ConfigRequired(t *testing.T) {
	cmd := NewCmd()
	cmd.SetArgs([]string{"supplier"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error when required --config flag is missing for supplier")
	}
}
