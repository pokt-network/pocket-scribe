package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/types"
)

// InsertBlock writes one row to the block table inside the caller's transaction.
// Idempotent: ON CONFLICT (height) DO NOTHING — replaying a height is a no-op
// (the sidecar produces deterministic bytes for a given height; invariant 4).
func InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO block (height, time, hash, proposer_address, tx_count)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (height) DO NOTHING`,
		h.Height, h.Time, h.Hash, h.ProposerAddress, h.TxCount)
	if err != nil {
		return fmt.Errorf("insert block at height %d: %w", h.Height, err)
	}
	return nil
}
