package deregister

// cmd_test.go — covers NewCmd() construction and the envOr helper.
// Tests do NOT execute the RunE body (which requires a live Postgres connection).

import (
	"testing"
)

// TestNewCmd_FlagRegistered verifies that NewCmd registers the --dsn flag.
func TestNewCmd_FlagRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	if cmd.Flags().Lookup("dsn") == nil {
		t.Error("--dsn flag not registered on deregister-consumer command")
	}
}

// TestNewCmd_RequiresExactlyOneArg verifies that the command enforces ExactArgs(1).
func TestNewCmd_RequiresExactlyOneArg(t *testing.T) {
	cmd := NewCmd()
	cmd.SetArgs([]string{}) // no arg → should error

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error when no arg provided to deregister-consumer")
	}
}

// TestEnvOr_EnvVarSet verifies that envOr returns the env var value when set.
func TestEnvOr_EnvVarSet(t *testing.T) {
	const key = "PS_TEST_DEREGISTER_DSN_UNIQUE"
	const want = "postgres-dsn-placeholder-value"
	t.Setenv(key, want)
	if got := envOr(key, "fallback"); got != want {
		t.Fatalf("envOr with set env: got %q, want %q", got, want)
	}
}

// TestEnvOr_EnvVarNotSet verifies that envOr returns the fallback when absent.
func TestEnvOr_EnvVarNotSet(t *testing.T) {
	const key = "PS_TEST_DEREGISTER_DSN_UNIQUE"
	t.Setenv(key, "")
	const fallback = "fallback-value"
	if got := envOr(key, fallback); got != fallback {
		t.Fatalf("envOr with unset env: got %q, want %q", got, fallback)
	}
}
