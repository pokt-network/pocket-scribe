package sync

// cmd_test.go — covers NewCmd() construction and the envOr helper.
// The RunE body requires live Postgres + HTTP LCD; those paths are not exercised here.

import (
	"testing"
)

// TestNewCmd_FlagsRegistered verifies that NewCmd registers all expected flags.
func TestNewCmd_FlagsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	for _, flag := range []string{"config", "dsn"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("--%s flag not registered on sync-upgrades command", flag)
		}
	}
}

// TestNewCmd_ConfigRequired verifies that --config is marked required.
func TestNewCmd_ConfigRequired(t *testing.T) {
	cmd := NewCmd()
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error when required --config flag is missing")
	}
}

// TestEnvOr_EnvVarSet verifies that envOr returns the env var value when set.
func TestEnvOr_EnvVarSet(t *testing.T) {
	const key = "PS_TEST_SYNC_DSN_UNIQUE"
	const want = "postgres-dsn-placeholder-value"
	t.Setenv(key, want)
	if got := envOr(key, "fallback"); got != want {
		t.Fatalf("envOr with set env: got %q, want %q", got, want)
	}
}

// TestEnvOr_EnvVarNotSet verifies that envOr returns the fallback when absent.
func TestEnvOr_EnvVarNotSet(t *testing.T) {
	const key = "PS_TEST_SYNC_DSN_UNIQUE"
	t.Setenv(key, "")
	const fallback = "fallback-value"
	if got := envOr(key, fallback); got != fallback {
		t.Fatalf("envOr with unset env: got %q, want %q", got, fallback)
	}
}
