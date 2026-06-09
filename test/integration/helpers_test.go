//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// storeFrom wraps the shared pool in a *store.Store for tests that exercise the
// store API. (Store.New pings; the shared pool is already live, so we build the
// Store directly from the shared DSN.)
func storeFrom(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(t.Context(), pg.DSN)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// newStoreFromDSN builds a Store against an arbitrary DSN (used by the
// fixed-port Postgres-restart test).
func newStoreFromDSN(t *testing.T, dsn string) (*store.Store, error) {
	t.Helper()
	s, err := store.New(t.Context(), dsn)
	if err != nil {
		return nil, err
	}
	t.Cleanup(s.Close)
	return s, nil
}

// processedCount4 counts processed_heights rows at height 4 via the given store.
func processedCount4(t *testing.T, s *store.Store) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM processed_heights WHERE consumer_name='noop-a' AND height=4`).Scan(&n); err != nil {
		t.Fatalf("processedCount4: %v", err)
	}
	return n
}
