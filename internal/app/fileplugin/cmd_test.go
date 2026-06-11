package fileplugin

// cmd_test.go — covers NewCmd() construction and the envOr helper.
// The RunE body requires live NATS; those paths are not exercised here.

import (
	"testing"
)

// TestNewCmd_FlagsRegistered verifies that NewCmd registers all expected flags.
func TestNewCmd_FlagsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	for _, flag := range []string{"bootstrap", "config", "input-dir", "max-height", "nats-url"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("--%s flag not registered on fileplugin command", flag)
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
	const key = "PS_TEST_FILEPLUGIN_NATS_UNIQUE"
	const want = "nats://custom-server:4222"
	t.Setenv(key, want)
	if got := envOr(key, "fallback"); got != want {
		t.Fatalf("envOr with set env: got %q, want %q", got, want)
	}
}

// TestEnvOr_EnvVarNotSet verifies that envOr returns the fallback when absent.
func TestEnvOr_EnvVarNotSet(t *testing.T) {
	const key = "PS_TEST_FILEPLUGIN_NATS_UNIQUE"
	t.Setenv(key, "")
	const fallback = "fallback-value"
	if got := envOr(key, fallback); got != fallback {
		t.Fatalf("envOr with unset env: got %q, want %q", got, fallback)
	}
}
