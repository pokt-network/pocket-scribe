// Package main is the entry point for the ps CLI. All subcommand wiring lives
// in internal/app; this file stays thin.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pokt-network/pocketscribe/internal/app"
)

func main() {
	// Propagate SIGINT/SIGTERM as context cancellation so long-running
	// subcommands (reconciler, consumer, fileplugin) exit cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	err := app.NewRootCmd().ExecuteContext(ctx)
	stop() // always release signal resources before process exit
	if err != nil {
		fmt.Fprintln(os.Stderr, "ps:", err)
		os.Exit(1)
	}
}
