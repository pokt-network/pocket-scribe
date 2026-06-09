// Package main is the entry point for the ps CLI.
//
// The actual subcommand wiring lives in internal/app/* per CLAUDE.md
// quick-reference layout. This stub is a placeholder until Phase B brings
// cobra-based subcommands online.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "ps: command not yet implemented (Slice 1 Phase A skeleton)")
	os.Exit(1)
}
