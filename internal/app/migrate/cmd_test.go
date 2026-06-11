package migrate

// cmd_test.go — covers the migrate NewCmd() construction and the envOr helper.
// Tests do NOT execute the RunE body (which requires a live Postgres connection).

import (
	"testing"
)

// TestNewCmd_SubcommandsRegistered verifies that NewCmd registers the expected
// migrate subcommands and the DSN flag.
func TestNewCmd_SubcommandsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	want := map[string]bool{"up": false, "down": false, "status": false}
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
	if cmd.PersistentFlags().Lookup("dsn") == nil {
		t.Error("--dsn flag not registered on migrate command")
	}
}

// TestEnvOr_EnvVarSet verifies that envOr returns the environment variable
// value when the variable is set, covering the "v != empty" branch.
func TestEnvOr_EnvVarSet(t *testing.T) {
	const key = "PS_TEST_MIGRATE_DSN_ENVVAR_UNIQUE"
	const want = "postgres-dsn-placeholder-value"
	t.Setenv(key, want)
	if got := envOr(key, "fallback"); got != want {
		t.Fatalf("envOr with set env: got %q, want %q", got, want)
	}
}

// TestEnvOr_EnvVarNotSet verifies that envOr returns the fallback when the
// environment variable is absent, covering the "return fallback" branch.
func TestEnvOr_EnvVarNotSet(t *testing.T) {
	const key = "PS_TEST_MIGRATE_DSN_ENVVAR_UNIQUE"
	const fallback = "fallback-value"
	// Ensure the var is unset.
	t.Setenv(key, "")
	if got := envOr(key, fallback); got != fallback {
		t.Fatalf("envOr with unset env: got %q, want %q", got, fallback)
	}
}
