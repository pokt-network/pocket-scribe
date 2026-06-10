//go:build integration

// store_router_test.go — component tests for router.NewDBRouter and
// router.DBRouter.Refresh, which require a live Postgres + seeded upgrades table.
// These are the only tests that exercise the zero-coverage db.go code paths.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	v0_1_0 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_0"
	v0_1_28 "github.com/pokt-network/pocketscribe/internal/decoders/v0_1_28"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// minRegistry is a small registry sufficient for DBRouter tests.
func minRegistry() map[string]decoders.Decoder {
	return map[string]decoders.Decoder{
		"v0_1_0":  v0_1_0.Decoder{},
		"v0_1_28": v0_1_28.Decoder{},
	}
}

// seedUpgrade upserts one upgrade row directly via pool (bypasses store to keep
// test helpers self-contained).
func seedUpgrade(t *testing.T, name string, height int64, decoderVersion string) {
	t.Helper()
	ctx := context.Background()
	_, err := pg.Pool.Exec(ctx,
		`INSERT INTO upgrades (name, applied_at_height, applied_at_time, decoder_version)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (name) DO UPDATE SET
		   applied_at_height=EXCLUDED.applied_at_height,
		   applied_at_time=EXCLUDED.applied_at_time,
		   decoder_version=EXCLUDED.decoder_version`,
		name, height, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), decoderVersion)
	if err != nil {
		t.Fatalf("seedUpgrade %s: %v", name, err)
	}
}

// TestNewDBRouter_ConstructsFromSeededUpgrades verifies that NewDBRouter:
//  1. loads the upgrades table on construction,
//  2. returns the genesis decoder for heights before the first upgrade,
//  3. returns the correct registered decoder for heights after a known upgrade.
func TestNewDBRouter_ConstructsFromSeededUpgrades(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	seedUpgrade(t, "v0.1.28", 287932, "v0_1_28")

	s := storeFrom(t)
	reg := minRegistry()

	r, err := router.NewDBRouter(ctx, s, reg, "v0_1_0")
	if err != nil {
		t.Fatalf("NewDBRouter: %v", err)
	}

	// Height before the upgrade → genesis decoder v0_1_0.
	d0, err := r.DecoderFor(1)
	if err != nil {
		t.Fatalf("DecoderFor(1): %v", err)
	}
	if d0.Version() != "v0_1_0" {
		t.Errorf("DecoderFor(1) = %s, want v0_1_0", d0.Version())
	}

	// Height at the upgrade boundary → v0_1_28.
	d28, err := r.DecoderFor(287932)
	if err != nil {
		t.Fatalf("DecoderFor(287932): %v", err)
	}
	if d28.Version() != "v0_1_28" {
		t.Errorf("DecoderFor(287932) = %s, want v0_1_28", d28.Version())
	}

	// Well above the upgrade → still v0_1_28.
	d28b, err := r.DecoderFor(999999)
	if err != nil {
		t.Fatalf("DecoderFor(999999): %v", err)
	}
	if d28b.Version() != "v0_1_28" {
		t.Errorf("DecoderFor(999999) = %s, want v0_1_28", d28b.Version())
	}
}

// TestDBRouter_RefreshPicksUpNewRows verifies that calling Refresh after adding
// a new upgrade to the DB makes the router aware of the new boundary.
func TestDBRouter_RefreshPicksUpNewRows(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	// Start with only the genesis decoder (no upgrade rows).
	s := storeFrom(t)
	reg := minRegistry()

	r, err := router.NewDBRouter(ctx, s, reg, "v0_1_0")
	if err != nil {
		t.Fatalf("NewDBRouter: %v", err)
	}

	// Before Refresh: height 287932 returns genesis decoder.
	dBefore, err := r.DecoderFor(287932)
	if err != nil {
		t.Fatalf("DecoderFor before refresh: %v", err)
	}
	if dBefore.Version() != "v0_1_0" {
		t.Errorf("before refresh: DecoderFor(287932) = %s, want v0_1_0", dBefore.Version())
	}

	// Seed a new upgrade row and call Refresh.
	seedUpgrade(t, "v0.1.28", 287932, "v0_1_28")
	if err := r.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// After Refresh: height 287932 returns the new decoder.
	dAfter, err := r.DecoderFor(287932)
	if err != nil {
		t.Fatalf("DecoderFor after refresh: %v", err)
	}
	if dAfter.Version() != "v0_1_28" {
		t.Errorf("after refresh: DecoderFor(287932) = %s, want v0_1_28", dAfter.Version())
	}
}

// TestNewDBRouter_DBReadFailure verifies that NewDBRouter propagates a Postgres
// error. We simulate this by closing the store's pool before construction.
func TestNewDBRouter_DBReadFailure(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()

	// Build a store that has already been closed — any DB call will fail.
	s, err := store.New(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	s.Close() // pool closed; ListUpgrades will fail

	reg := minRegistry()
	_, err = router.NewDBRouter(ctx, s, reg, "v0_1_0")
	if err == nil {
		t.Fatal("expected error from NewDBRouter when DB is unavailable")
	}
}

// TestStoreNew_BadDSN verifies that store.New returns an error for an
// unreachable DSN (covers the pgxpool.New / ping error branches in store.go:18).
func TestStoreNew_BadDSN(t *testing.T) {
	ctx := context.Background()
	// Non-existent host → pgx connection fails.
	_, err := store.New(ctx, "postgres://user:pass@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err == nil {
		t.Fatal("expected error for unreachable DSN")
	}
}

// TestUpsertUpgrade_ContextCancelled covers the UpsertUpgrade error path.
func TestUpsertUpgrade_ContextCancelled(t *testing.T) {
	pg.Reset(t)
	s := storeFrom(t)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	u := store.Upgrade{
		Name:            "v0.1.x",
		AppliedAtHeight: 1,
		DecoderVersion:  "v0_1_x",
	}
	if err := s.UpsertUpgrade(cancelled, u); err == nil {
		t.Fatal("expected error with cancelled context")
	}
}
