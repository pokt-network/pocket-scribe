// Package main is the entry point for the ps CLI. All subcommand wiring lives
// in internal/app; this file stays thin.
package main

import (
	"fmt"
	"os"

	"github.com/pokt-network/pocketscribe/internal/app"
)

func main() {
	if err := app.NewRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ps:", err)
		os.Exit(1)
	}
}
