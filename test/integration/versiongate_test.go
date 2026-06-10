//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pokt-network/pocketscribe/internal/store"
)

func seedUpgrade(t *testing.T, s *store.Store, name string, height int64, decoderVersion string) {
	t.Helper()
	err := s.UpsertUpgrade(context.Background(), store.Upgrade{
		Name:            name,
		AppliedAtHeight: height,
		AppliedAtTime:   time.Date(2025, 6, 17, 16, 15, 0, 0, time.UTC),
		DecoderVersion:  decoderVersion,
	})
	if err != nil {
		t.Fatalf("seed upgrade %s: %v", name, err)
	}
}

func TestConsumerFirstValidHeight(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")

	h, err := s.ConsumerFirstValidHeight(ctx, "v0.1.20", "v0_1_0")
	if err != nil || h != 135297 {
		t.Fatalf("got %d, %v; want 135297", h, err)
	}
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.1.0", "v0_1_0")
	if err != nil || h != 1 {
		t.Fatalf("got %d, %v; want 1", h, err)
	}
	h, err = s.ConsumerFirstValidHeight(ctx, "v0.2.0", "v0_1_0")
	if err != nil || h != store.DormantHeight {
		t.Fatalf("got %d, %v; want DormantHeight", h, err)
	}
	// Error path: garbage genesis from a broken config must fail loud.
	if _, err := s.ConsumerFirstValidHeight(ctx, "v0.1.0", "garbage"); err == nil {
		t.Fatal("want error on invalid genesis version")
	}
}

func TestFirstValidHeightsMap(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	seedUpgrade(t, s, "v0.1.20", 135297, "v0_1_20")
	mustRegister(t, s, "blocklike", "v0.1.0")
	mustRegister(t, s, "late", "v0.1.20")
	mustRegister(t, s, "phantom", "v0.2.0")
	// Deregistered consumers are excluded.
	mustRegister(t, s, "gone", "v0.1.0")
	if _, err := s.DeregisterConsumer(ctx, "gone"); err != nil {
		t.Fatal(err)
	}

	m, err := s.FirstValidHeights(ctx, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"blocklike": 1, "late": 135297, "phantom": store.DormantHeight}
	if len(m) != len(want) {
		t.Fatalf("got %v, want %v", m, want)
	}
	for k, v := range want {
		if m[k] != v {
			t.Fatalf("m[%s] = %d, want %d", k, m[k], v)
		}
	}
}

func TestRegisterConsumerNormalizesVersion(t *testing.T) {
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	// Underscored input is normalized to canonical dotted form at the boundary.
	if err := s.RegisterConsumer(ctx, "u", "v0_1_20"); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := pg.Pool.QueryRow(ctx,
		`SELECT first_valid_version FROM consumer_registry WHERE consumer_name='u'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "v0.1.20" {
		t.Fatalf("stored %q, want v0.1.20", stored)
	}
	// Garbage is rejected before it reaches the table.
	if err := s.RegisterConsumer(ctx, "bad", "not-a-version"); err == nil {
		t.Fatal("want error registering invalid version")
	}
}

func TestLegacyUnderscoredRowStillResolves(t *testing.T) {
	// Rows written before write-side normalization existed must keep working:
	// the read path normalizes again (versiongate.go).
	pg.Reset(t)
	ctx := context.Background()
	s := storeFrom(t)
	_, err := pg.Pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version) VALUES ('legacy', 'v0_1_0')`)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.FirstValidHeights(ctx, "v0_1_0")
	if err != nil {
		t.Fatal(err)
	}
	if m["legacy"] != 1 {
		t.Fatalf("legacy underscored row resolved to %d, want 1", m["legacy"])
	}
}

func mustRegister(t *testing.T, s *store.Store, name, v string) {
	t.Helper()
	if err := s.RegisterConsumer(context.Background(), name, v); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}
