package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ProcessHeight runs the ack-after-commit transaction body for one message
// (invariant #5): BEGIN → write(tx) → INSERT processed_heights → advance
// consolidation → COMMIT. It returns the consumer's new consolidated_up_to.
//
// write performs the handler's data inserts within the same transaction; pass a
// no-op for NoOp consumers. The caller acks the NATS message only after this
// returns nil.
func (s *Store) ProcessHeight(
	ctx context.Context,
	consumer string,
	height int64,
	write func(ctx context.Context, tx pgx.Tx) error,
) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if err := write(ctx, tx); err != nil {
		return 0, fmt.Errorf("handler write at height %d: %w", height, err)
	}
	if err := insertProcessedHeight(ctx, tx, consumer, height); err != nil {
		return 0, err
	}
	current, err := readConsolidationTx(ctx, tx, consumer)
	if err != nil {
		return 0, err
	}
	next, err := advanceConsolidation(ctx, tx, consumer, current)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit at height %d: %w", height, err)
	}
	return next, nil
}
