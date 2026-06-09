//go:build integration

package integration

import (
	"context"
	"testing"
)

// setConsolidation upserts a consumer's cursor directly (test scaffolding).
func setConsolidation(t *testing.T, name string, upTo int64) {
	t.Helper()
	_, err := pg.Pool.Exec(context.Background(),
		`INSERT INTO consumer_consolidation (consumer_name, consolidated_up_to, updated_at)
		 VALUES ($1,$2, now())
		 ON CONFLICT (consumer_name) DO UPDATE SET consolidated_up_to = EXCLUDED.consolidated_up_to`,
		name, upTo)
	if err != nil {
		t.Fatalf("set consolidation: %v", err)
	}
}

func TestSealOneConsumer(t *testing.T) { // spec test 7
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")

	setConsolidation(t, "noop-a", 4)
	assertSealed(t, s, 4, true)
	assertSealed(t, s, 5, false)

	setConsolidation(t, "noop-a", 5)
	assertSealed(t, s, 5, true)
}

func TestSealTwoConsumersAND(t *testing.T) { // spec test 8
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_ = s.RegisterConsumer(ctx, "noop-a", "v0.1.0")
	_ = s.RegisterConsumer(ctx, "noop-b", "v0.1.0")

	setConsolidation(t, "noop-a", 10)
	setConsolidation(t, "noop-b", 7)
	// H=8: a crossed it, b has not → NOT sealed.
	assertSealed(t, s, 8, false)
	// H=7: both crossed → sealed.
	assertSealed(t, s, 7, true)

	// b catches up to 10 → H=8..10 now sealed.
	setConsolidation(t, "noop-b", 10)
	assertSealed(t, s, 10, true)
}

func assertSealed(t *testing.T, s interface {
	IsSealed(context.Context, int64) (bool, error)
}, h int64, want bool) {
	t.Helper()
	got, err := s.IsSealed(context.Background(), h)
	if err != nil {
		t.Fatalf("IsSealed(%d): %v", h, err)
	}
	if got != want {
		t.Fatalf("IsSealed(%d) = %v, want %v", h, got, want)
	}
}
