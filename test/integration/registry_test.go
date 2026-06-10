//go:build integration

package integration

import (
	"context"
	"testing"
)

func TestSelfRegistrationIdempotent(t *testing.T) { // spec test 9
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for i := 0; i < 3; i++ {
		if err := s.RegisterConsumer(ctx, "noop-a", "v0.1.0"); err != nil {
			t.Fatalf("register #%d: %v", i, err)
		}
	}
	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM consumer_registry WHERE consumer_name = 'noop-a'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 registry row after 3 registrations, got %d", count)
	}
}

func TestDeregisterFlipsActive(t *testing.T) { // registry half of spec test 13
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")
	_ = s.RegisterConsumer(ctx, "noop-b", "v0.1.0")

	changed, err := s.DeregisterConsumer(ctx, "noop-b")
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	if !changed {
		t.Fatal("expected deregister to change a row")
	}
	active, err := s.RequiredSet(ctx, 100, genesisV0_1_0)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0] != "noop-a" {
		t.Fatalf("RequiredSet = %v, want [noop-a]", active)
	}

	// Deregistering again is a no-op.
	changed, _ = s.DeregisterConsumer(ctx, "noop-b")
	if changed {
		t.Fatal("second deregister should not change a row")
	}
}
