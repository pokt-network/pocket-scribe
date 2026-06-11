package app

// version_cmd_test.go — covers the newVersionCmd RunE body. The version
// command only prints a static string (no DB / NATS / config required).

import (
	"bytes"
	"strings"
	"testing"
)

// TestVersionCmdRun verifies that the `version` subcommand executes without
// error and writes a non-empty version string to its output.
func TestVersionCmdRun(t *testing.T) {
	root := NewRootCmd()

	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs([]string{"version"})

	if err := root.Execute(); err != nil {
		t.Fatalf("version command: unexpected error: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got == "" {
		t.Fatal("version command: expected non-empty output")
	}
}
