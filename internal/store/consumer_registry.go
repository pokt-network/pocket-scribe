package store

import (
	"context"
	"fmt"
)

// RegisterConsumer idempotently records a consumer in consumer_registry. Called
// on consumer startup; re-running never duplicates a row (ON CONFLICT DO NOTHING).
func (s *Store) RegisterConsumer(ctx context.Context, name, firstValidVersion string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO consumer_registry (consumer_name, first_valid_version)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name) DO NOTHING`,
		name, firstValidVersion)
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

// RequiredSet returns the consumers whose sign-off height H must wait on.
//
// Phase B: required_set == the set of currently-active consumers; the height
// argument is accepted but not yet used. Phase F adds semver-gated membership
// (FirstValidVersion vs network.genesis_decoder_version / upgrades), at which
// point height becomes significant.
func (s *Store) RequiredSet(ctx context.Context, _ int64) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT consumer_name FROM consumer_registry WHERE active = true ORDER BY consumer_name`)
	if err != nil {
		return nil, fmt.Errorf("query required set: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan consumer name: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}
