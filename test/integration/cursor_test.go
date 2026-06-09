//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// noWrite is a handler body that writes no data rows (NoOp-equivalent).
func noWrite(_ context.Context, _ pgx.Tx) error { return nil }

func TestCursorAdvancesContiguously(t *testing.T) { // spec test 1
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for h := int64(1); h <= 5; h++ {
		cur, err := s.ProcessHeight(ctx, "noop-a", h, noWrite)
		if err != nil {
			t.Fatalf("ProcessHeight(%d): %v", h, err)
		}
		if cur != h {
			t.Fatalf("after processing %d, cursor = %d, want %d", h, cur, h)
		}
	}
}

func TestOutOfOrderFreezesAtGap(t *testing.T) { // spec test 10 + freeze half of test 2
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	// Process 1,2 then jump to 4 (gap at 3). Cursor must stay at 2.
	for _, h := range []int64{1, 2} {
		if _, err := s.ProcessHeight(ctx, "noop-a", h, noWrite); err != nil {
			t.Fatal(err)
		}
	}
	cur, err := s.ProcessHeight(ctx, "noop-a", 4, noWrite)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 2 {
		t.Fatalf("cursor after out-of-order 4 = %d, want 2 (gap at 3)", cur)
	}
	// processed_heights still recorded 4.
	if ok, _ := s.HasProcessed(ctx, "noop-a", 4); !ok {
		t.Fatal("height 4 should be recorded in processed_heights despite the gap")
	}
	// Now 3 arrives — cursor jumps to 4.
	cur, err = s.ProcessHeight(ctx, "noop-a", 3, noWrite)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 4 {
		t.Fatalf("cursor after gap fill = %d, want 4", cur)
	}
}

func TestDuplicateHeightIdempotent(t *testing.T) { // DB half of spec test 11
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)

	for i := 0; i < 2; i++ {
		if _, err := s.ProcessHeight(ctx, "noop-a", 7, noWrite); err != nil {
			t.Fatalf("ProcessHeight dup #%d: %v", i, err)
		}
	}
	var n int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=7`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed_heights rows for (noop-a,7) = %d, want 1", n)
	}
}
