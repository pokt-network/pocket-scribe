package doctor

// cmd_test.go — covers NewCmd() construction and flag wiring.
// The RunE body requires live Postgres/NATS/LCD; not exercised here.

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestNewDoctorCmd_FlagsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	for _, flag := range []string{"config", "dsn", "nats-url"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("--%s flag not registered on doctor command", flag)
		}
	}
}

func TestNewDoctorCmd_NameAndUse(t *testing.T) {
	cmd := NewCmd()
	if cmd.Use != "doctor" {
		t.Errorf("expected Use=doctor, got %q", cmd.Use)
	}
}

func TestNewDoctorCmd_ConfigNotRequired(t *testing.T) {
	// doctor --config is optional (LCD check skipped when absent)
	cmd := NewCmd()
	f := cmd.Flags().Lookup("config")
	if f == nil {
		t.Fatal("--config not registered")
	}
	// Not required — annotations should not include "required"
	ann := f.Annotations
	if ann != nil {
		if _, ok := ann[cobra.BashCompOneRequiredFlag]; ok {
			t.Error("--config should not be marked required on doctor command")
		}
	}
}
