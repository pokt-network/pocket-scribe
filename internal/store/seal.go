package store

import (
	"context"
	"fmt"
)

// IsSealed reports whether height is sealed: every consumer in
// required_set(height) has consolidated_up_to >= height, and the required set
// is non-empty. The non-empty guard is kept from Phase B deliberately (a
// height nobody is required to process must not read as "sealed" on a fresh
// database) — a divergence from a vacuous-truth reading of spec §4.10,
// matching spec tests 7/8 behavior. Derived at query time — no materialized
// seal row in Slice 1.
func (s *Store) IsSealed(ctx context.Context, height int64, genesisVersion string) (bool, error) {
	required, err := s.RequiredSet(ctx, height, genesisVersion)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	if len(required) == 0 {
		return false, nil
	}
	var lagging int
	err = s.pool.QueryRow(ctx,
		`SELECT count(*)
		 FROM unnest($1::text[]) AS r(consumer_name)
		 LEFT JOIN consumer_consolidation c USING (consumer_name)
		 WHERE COALESCE(c.consolidated_up_to, 0) < $2`,
		required, height).Scan(&lagging)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	return lagging == 0, nil
}
