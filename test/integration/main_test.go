//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	tc "github.com/pokt-network/pocketscribe/test/testcontainers"
)

// Package-shared harness, booted once in TestMain.
var (
	pg   *tc.PG
	nats *tc.NC
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	p, err := tc.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness postgres:", err)
		os.Exit(1)
	}
	n, err := tc.StartNATS(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness nats:", err)
		os.Exit(1)
	}
	pg, nats = p, n
	code := m.Run()
	pg.Pool.Close()
	_ = pg.Container.Terminate(ctx)
	nats.Client.Close()
	_ = nats.Container.Terminate(ctx)
	os.Exit(code)
}
