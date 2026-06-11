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
	"os"
	"sort"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/fixturereport"
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

// fixtureEntry mirrors the shape in test/fixtures/sync-upgrades/mainnet-applied-plans.json.
// It is re-declared here (separate from the unit-test package) because integration tests
// are a different package and cannot share unexported types from internal/upgrades.
type syncFixtureEntry struct {
	AppliedPlan struct {
		Height string `json:"height"`
	} `json:"applied_plan"`
	BlockTime string `json:"block_time"`
}

// TestSync_AllMainnetUpgrades drives Sync end-to-end with an httptest server that
// replays the full mainnet golden fixture (all 34 upgrade_names from mainnet.yaml,
// including v0.1.1 and v0.1.32 which are height=0 and must be skipped). After the
// sync the DB rows are compared against fixturereport.MainnetUpgrades() — the
// chain-authoritative table — asserting an exact height match for every registered
// upgrade.
func TestSync_AllMainnetUpgrades(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	// Load the golden fixture.
	raw, err := os.ReadFile("../../test/fixtures/sync-upgrades/mainnet-applied-plans.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var fixture map[string]syncFixtureEntry
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}

	// Wire up an httptest server serving every entry in the fixture.
	mux := http.NewServeMux()
	for name, entry := range fixture {
		name, entry := name, entry // pin loop vars
		mux.HandleFunc("/cosmos/upgrade/v1beta1/applied_plan/"+name, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"height": entry.AppliedPlan.Height})
		})
		if entry.AppliedPlan.Height != "0" && entry.AppliedPlan.Height != "" {
			mux.HandleFunc("/cosmos/base/tendermint/v1beta1/blocks/"+entry.AppliedPlan.Height, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"block": map[string]any{
						"header": map[string]string{"time": entry.BlockTime},
					},
				})
			})
		}
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Build sorted name list from the fixture.
	allNames := make([]string, 0, len(fixture))
	for name := range fixture {
		allNames = append(allNames, name)
	}
	sort.Strings(allNames)

	// Run the full sync.
	s := storeFrom(t)
	syncer := upgrades.New(srv.URL, srv.Client())
	n, err := syncer.Sync(ctx, s, allNames)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Count expected: fixture entries with height != "0".
	expectedCount := 0
	for _, e := range fixture {
		if e.AppliedPlan.Height != "0" && e.AppliedPlan.Height != "" {
			expectedCount++
		}
	}
	if n != expectedCount {
		t.Errorf("Sync returned count=%d, want %d (applied entries)", n, expectedCount)
	}

	// Assert DB rows match MainnetUpgrades() (chain-authoritative).
	authoritative := fixturereport.MainnetUpgrades()
	for _, auth := range authoritative {
		var dbHeight int64
		if err := pg.Pool.QueryRow(ctx,
			`SELECT applied_at_height FROM upgrades WHERE name=$1`, auth.Name).Scan(&dbHeight); err != nil {
			t.Errorf("%s: expected row in DB but query failed: %v", auth.Name, err)
			continue
		}
		if dbHeight != auth.AppliedAtHeight {
			t.Errorf("%s: DB height=%d, MainnetUpgrades=%d — DISCREPANCY", auth.Name, dbHeight, auth.AppliedAtHeight)
		}
	}

	// Verify v0.1.1 and v0.1.32 (height=0) are absent from DB.
	for _, skip := range []string{"v0.1.1", "v0.1.32"} {
		var cnt int
		if err := pg.Pool.QueryRow(ctx,
			`SELECT count(*) FROM upgrades WHERE name=$1`, skip).Scan(&cnt); err != nil {
			t.Fatalf("count %s: %v", skip, err)
		}
		if cnt != 0 {
			t.Errorf("%s (height=0) should not appear in DB, found %d row(s)", skip, cnt)
		}
	}

	// Second sync must be idempotent: same count, same rows.
	n2, err := syncer.Sync(ctx, s, allNames)
	if err != nil {
		t.Fatalf("Sync idempotent: %v", err)
	}
	if n2 != n {
		t.Errorf("Sync idempotent count=%d, want %d", n2, n)
	}
	// Total DB rows after idempotent sync must still equal expected applied count.
	var totalRows int
	if err := pg.Pool.QueryRow(ctx, `SELECT count(*) FROM upgrades`).Scan(&totalRows); err != nil {
		t.Fatalf("count upgrades: %v", err)
	}
	if totalRows != expectedCount {
		t.Errorf("DB total upgrades=%d, want %d after idempotent sync", totalRows, expectedCount)
	}
}
