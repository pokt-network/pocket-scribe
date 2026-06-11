package inspect

// cmd_test.go — covers NewCmd() construction and flag wiring.
// RunE bodies require live Postgres/NATS; those paths are not exercised here.

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestNewInspectCmd_SubcommandsRegistered(t *testing.T) {
	cmd := NewCmd()
	if cmd == nil {
		t.Fatal("NewCmd returned nil")
	}
	subs := map[string]bool{}
	for _, sub := range cmd.Commands() {
		subs[sub.Name()] = true
	}
	for _, want := range []string{"cursors", "streams"} {
		if !subs[want] {
			t.Errorf("subcommand %q not registered on inspect command", want)
		}
	}
}

func TestNewInspectCmd_CursorsFlags(t *testing.T) {
	cmd := NewCmd()
	var cursors *cobra.Command
	for _, sub := range cmd.Commands() {
		if sub.Name() == "cursors" {
			cursors = sub
			break
		}
	}
	if cursors == nil {
		t.Fatal("cursors subcommand not found")
	}
	if cursors.Flags().Lookup("dsn") == nil {
		t.Error("--dsn flag not registered on inspect cursors")
	}
}

func TestNewInspectCmd_StreamsFlags(t *testing.T) {
	cmd := NewCmd()
	var streams *cobra.Command
	for _, sub := range cmd.Commands() {
		if sub.Name() == "streams" {
			streams = sub
			break
		}
	}
	if streams == nil {
		t.Fatal("streams subcommand not found")
	}
	if streams.Flags().Lookup("nats-url") == nil {
		t.Error("--nats-url flag not registered on inspect streams")
	}
}
