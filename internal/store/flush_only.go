package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// FlushOnly runs write inside one transaction WITHOUT advancing any cursor or
// recording processed_heights (ADR-024 triggers 2-3: partial flush). The
// caller must NOT ack NATS messages on success — only the block-boundary
// fence advances the cursor and acks.
func (s *Store) FlushOnly(ctx context.Context, write func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if err := write(ctx, tx); err != nil {
		return fmt.Errorf("partial flush write: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("partial flush commit: %w", err)
	}
	return nil
}
