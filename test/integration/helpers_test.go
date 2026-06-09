//go:build integration

package integration

import (
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
