//go:build integration

// upgrades_sync_test.go — component tests for upgrades.Syncer.Sync, which
// requires a live store to call UpsertUpgrade. The HTTP layer is faked with
// net/http/httptest (same approach as the unit tests in internal/upgrades/).
// Covered gap: Sync at 0% (the method wraps Fetch + UpsertUpgrade).
package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/upgrades"
)

// TestSync_HappyPath verifies that Sync fetches an applied upgrade from the LCD
// stub, calls UpsertUpgrade, and returns the correct count (1 upserted).
// A second call with the same names is idempotent (count = 1 again, still 1 DB row).
func TestSync_HappyPath(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/v0.1.30",
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"height": "484473"})
		})
	mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/484473",
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"block": map[string]any{
					"header": map[string]string{
						"time": "2025-04-17T12:00:00Z",
					},
				},
			})
		})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := storeFrom(t)
	syncer := upgrades.New(srv.URL, srv.Client())

	// First sync: should upsert 1 row.
	n, err := syncer.Sync(ctx, s, []string{"v0.1.30"})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if n != 1 {
		t.Errorf("Sync count = %d, want 1", n)
	}

	// Verify row exists in DB.
	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM upgrades WHERE name='v0.1.30' AND applied_at_height=484473`).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("upgrades row count = %d, want 1", count)
	}

	// Second sync (idempotent): same result.
	n2, err := syncer.Sync(ctx, s, []string{"v0.1.30"})
	if err != nil {
		t.Fatalf("Sync idempotent: %v", err)
	}
	if n2 != 1 {
		t.Errorf("Sync idempotent count = %d, want 1", n2)
	}
	// Still exactly 1 row (ON CONFLICT DO UPDATE).
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM upgrades WHERE name='v0.1.30'`).Scan(&count); err != nil {
		t.Fatalf("query2: %v", err)
	}
	if count != 1 {
		t.Errorf("upgrades row count after second sync = %d, want 1", count)
	}
}

// TestSync_FetchError verifies that Sync propagates errors from Fetch (e.g. the
// LCD returns 500) without touching the database.
func TestSync_FetchError(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := storeFrom(t)
	syncer := upgrades.New(srv.URL, srv.Client())

	n, err := syncer.Sync(ctx, s, []string{"v0.1.30"})
	if err == nil {
		t.Fatal("expected error from Sync when Fetch fails")
	}
	if n != 0 {
		t.Errorf("Sync on error count = %d, want 0", n)
	}
}

// TestSync_SkipsHeight0 verifies that upgrades with height "0" (not yet
// applied on chain) are not upserted to the DB. Sync must return count=0.
func TestSync_SkipsHeight0(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/v0.1.32",
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"height": "0"})
		})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := storeFrom(t)
	syncer := upgrades.New(srv.URL, srv.Client())

	n, err := syncer.Sync(ctx, s, []string{"v0.1.32"})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if n != 0 {
		t.Errorf("Sync count for height=0 = %d, want 0", n)
	}

	var count int
	if err := pg.Pool.QueryRow(ctx,
		`SELECT count(*) FROM upgrades WHERE name='v0.1.32'`).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 0 {
		t.Errorf("upgrades row count for unapplied upgrade = %d, want 0", count)
	}
}
