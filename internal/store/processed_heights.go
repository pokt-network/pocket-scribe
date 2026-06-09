package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// insertProcessedHeight records that consumer processed height, idempotently.
// Runs inside the caller's transaction.
func insertProcessedHeight(ctx context.Context, tx pgx.Tx, consumer string, height int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO processed_heights (consumer_name, height)
		 VALUES ($1, $2)
		 ON CONFLICT (consumer_name, height) DO NOTHING`,
		consumer, height)
	if err != nil {
		return fmt.Errorf("insert processed height (%s,%d): %w", consumer, height, err)
	}
	return nil
}

// HasProcessed reports whether consumer has a processed_heights row for height.
func (s *Store) HasProcessed(ctx context.Context, consumer string, height int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_heights WHERE consumer_name=$1 AND height=$2)`,
		consumer, height).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has processed (%s,%d): %w", consumer, height, err)
	}
	return exists, nil
}
