package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/pokt-network/pocketscribe/internal/protover"
)

// RegisterConsumer idempotently records a consumer in consumer_registry. Called
// on consumer startup; re-running never duplicates a row (ON CONFLICT DO NOTHING).
// firstValidVersion is normalized to canonical dotted form before storage; rows
// written before this normalization existed may hold non-canonical spellings —
// that is safe because every read path (FirstValidHeights via firstValidHeight)
// normalizes again; write-side normalization is hygiene, not a correctness dependency.
func (s *Store) RegisterConsumer(ctx context.Context, name, firstValidVersion string) error {
	v, err := protover.Normalize(firstValidVersion)
	if err != nil {
		return fmt.Errorf("register consumer %q: %w", name, err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name) DO NOTHING`,
		name, v)
	if err != nil {
		return fmt.Errorf("register consumer %q: %w", name, err)
	}
	return nil
}

// DeregisterConsumer flips active=false for an explicit decommission. Returns
// true if a currently-active row was changed. This UPDATE touches registry
// metadata only (allowed exception to append-only).
func (s *Store) DeregisterConsumer(ctx context.Context, name string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE consumer_registry
		 SET active = false, deregistered_at = now()
		 WHERE consumer_name = $1 AND active = true`,
		name)
	if err != nil {
		return false, fmt.Errorf("deregister consumer %q: %w", name, err)
	}
	return tag.RowsAffected() == 1, nil
}

// RequiredSet returns the consumers whose sign-off height H must wait on:
// active consumers whose consumer_first_valid_height (spec §4.10) is <= H.
// genesisVersion is network.genesis_decoder_version from the network config.
// Sorted by name for deterministic output.
func (s *Store) RequiredSet(ctx context.Context, height int64, genesisVersion string) ([]string, error) {
	fvh, err := s.FirstValidHeights(ctx, genesisVersion)
	if err != nil {
		return nil, fmt.Errorf("required_set(%d): %w", height, err)
	}
	var names []string
	for name, h := range fvh {
		if h <= height {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}
