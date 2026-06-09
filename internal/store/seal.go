package store

import (
	"context"
	"fmt"
)

// IsSealed reports whether height is sealed: every consumer in the required set
// (Phase B: all active consumers) has consolidated_up_to >= height, and the
// required set is non-empty. Derived at query time — no materialized seal row
// in Slice 1 (a materialized block_seal is deferred to Slice 2).
func (s *Store) IsSealed(ctx context.Context, height int64) (bool, error) {
	var sealed bool
	err := s.pool.QueryRow(ctx,
		`SELECT
		     count(*) FILTER (WHERE r.active) > 0
		     AND count(*) FILTER (WHERE r.active AND COALESCE(c.consolidated_up_to, 0) < $1) = 0
		 FROM consumer_registry r
		 LEFT JOIN consumer_consolidation c ON c.consumer_name = r.consumer_name`,
		height).Scan(&sealed)
	if err != nil {
		return false, fmt.Errorf("is_sealed(%d): %w", height, err)
	}
	return sealed, nil
}
