package router

import (
	"context"
	"fmt"

	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// DBRouter loads the upgrades table into an in-memory staticRouter snapshot and
// can Refresh it (ADR-018: DB-driven, no hardcoded heights).
type DBRouter struct {
	store          *store.Store
	registry       map[string]decoders.Decoder
	genesisVersion string
	current        Router
}

// NewDBRouter loads the upgrades table once and returns a ready router. The
// router is LENIENT: upgrade rows whose decoder_version is not in the registry do
// NOT error — DecoderFor falls back to the nearest registered earlier version
// (correct for the version-invariant block header). It errors only on an empty
// registry (via NewStaticRouter) or a DB read failure.
func NewDBRouter(ctx context.Context, st *store.Store, registry map[string]decoders.Decoder, genesisVersion string) (*DBRouter, error) {
	r := &DBRouter{store: st, registry: registry, genesisVersion: genesisVersion}
	if err := r.Refresh(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Refresh reloads the upgrades table into a fresh snapshot.
func (r *DBRouter) Refresh(ctx context.Context) error {
	rows, err := r.store.ListUpgrades(ctx)
	if err != nil {
		return err
	}
	ups := make([]Upgrade, 0, len(rows))
	for _, u := range rows {
		ups = append(ups, Upgrade{Name: u.Name, AppliedAtHeight: u.AppliedAtHeight, DecoderVersion: u.DecoderVersion})
	}
	snap, err := NewStaticRouter(ups, r.registry, r.genesisVersion)
	if err != nil {
		return fmt.Errorf("router refresh: %w", err)
	}
	r.current = snap
	return nil
}

// DecoderFor delegates to the current snapshot.
func (r *DBRouter) DecoderFor(height int64) (decoders.Decoder, error) {
	return r.current.DecoderFor(height)
}
