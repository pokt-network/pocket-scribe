package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ConsolidatedUpTo returns the consumer's contiguous high-water mark, or 0 if
// it has never consolidated.
func (s *Store) ConsolidatedUpTo(ctx context.Context, consumer string) (int64, error) {
	var cur int64
	err := s.pool.QueryRow(ctx,
		`SELECT consolidated_up_to FROM consumer_consolidation WHERE consumer_name=$1`,
		consumer).Scan(&cur)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("read consolidation %q: %w", consumer, err)
	}
	return cur, nil
}

// readConsolidationTx reads the cursor inside an open transaction.
func readConsolidationTx(ctx context.Context, tx pgx.Tx, consumer string) (int64, error) {
	var cur int64
	err := tx.QueryRow(ctx,
		`SELECT consolidated_up_to FROM consumer_consolidation WHERE consumer_name=$1`,
		consumer).Scan(&cur)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("read consolidation (tx) %q: %w", consumer, err)
	}
	return cur, nil
}

// advanceConsolidation computes the new contiguous high-water mark for consumer
// starting from current, then upserts consumer_consolidation. The window query
// returns the largest H such that every height in (current, H] is present in
// processed_heights (i.e. the unbroken run starting at current+1). If the very
// next height is missing, it returns current unchanged. Commutative: the result
// depends only on the set of processed heights, not arrival order.
func advanceConsolidation(ctx context.Context, tx pgx.Tx, consumer string, current int64) (int64, error) {
	var next int64
	err := tx.QueryRow(ctx,
		`WITH run AS (
		     SELECT height, ROW_NUMBER() OVER (ORDER BY height) AS rn
		     FROM processed_heights
		     WHERE consumer_name = $1 AND height > $2
		 )
		 SELECT COALESCE(MAX(height), $2) FROM run WHERE height = $2 + rn`,
		consumer, current).Scan(&next)
	if err != nil {
		return 0, fmt.Errorf("compute consolidation for %q: %w", consumer, err)
	}

	// GREATEST keeps the cursor monotonic even if two writers ever race (the
	// ADR-007 multi-instance case) — consolidated_up_to can never regress.
	if _, err := tx.Exec(ctx,
		`INSERT INTO consumer_consolidation (consumer_name, consolidated_up_to, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (consumer_name) DO UPDATE
		 SET consolidated_up_to = GREATEST(consumer_consolidation.consolidated_up_to, EXCLUDED.consolidated_up_to),
		     updated_at = now()`,
		consumer, next); err != nil {
		return 0, fmt.Errorf("upsert consolidation for %q: %w", consumer, err)
	}
	return next, nil
}
