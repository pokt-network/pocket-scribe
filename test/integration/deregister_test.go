//go:build integration

package integration

import (
	"bytes"
	"context"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/app"
)

func TestDeregisterConsumerCLIUnblocksSeal(t *testing.T) { // spec test 13
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// Two required consumers; one (noop-b) is behind, so H=5 is not sealed.
	if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterConsumer(ctx, "noop-b", "v0.1.0"); err != nil {
		t.Fatal(err)
	}
	setConsolidation(t, "noop-a", 5)
	setConsolidation(t, "noop-b", 2)
	assertSealed(t, s, 5, false)

	// Decommission noop-b via the real `ps deregister-consumer` command.
	root := app.NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"deregister-consumer", "noop-b", "--dsn", pg.DSN})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("deregister-consumer: %v (output: %s)", err, out.String())
	}

	// noop-b is no longer in the required set → H=5 now seals on noop-a alone.
	active, err := s.RequiredSet(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0] != "noop-a" {
		t.Fatalf("RequiredSet = %v, want [noop-a]", active)
	}
	assertSealed(t, s, 5, true)
}
