//go:build integration

package integration

import (
	"context"
	"testing"
)

// Test (spec Phase B): existing + new migrations apply cleanly; consumer_registry exists.
func TestMigrationsApplyAndRegistryExists(t *testing.T) {
	ctx := context.Background()

	var regclass *string
	err := pg.Pool.QueryRow(ctx, `SELECT to_regclass('public.consumer_registry')::text`).Scan(&regclass)
	if err != nil {
		t.Fatalf("query regclass: %v", err)
	}
	if regclass == nil || *regclass != "consumer_registry" {
		t.Fatalf("consumer_registry table missing after migrate up (got %v)", regclass)
	}

	// Sanity: the Phase A coordination tables are present too.
	for _, tbl := range []string{"processed_heights", "consumer_consolidation", "block", "bucket_seal"} {
		var n int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, tbl).Scan(&n); err != nil {
			t.Fatalf("check %s: %v", tbl, err)
		}
		if n != 1 {
			t.Fatalf("expected table %s to exist", tbl)
		}
	}
}
