// Package block implements the consumer.Handler that maps BlockEnvelope
// messages to block table rows. The sidecar decoded the version-invariant
// header and embedded it in the envelope (ADR-022 amendment); this handler
// only needs to unmarshal the envelope and insert the row — no router, no
// poktroll decode (spec §4.8, Phase E).
package block

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Inserter is the store surface the handler writes through (real: store.InsertBlock).
type Inserter interface {
	InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error
}

// Handler maps sidecar BlockEnvelope messages to block rows.
type Handler struct {
	inserter Inserter
}

// New constructs the block handler.
func New(inserter Inserter) *Handler { return &Handler{inserter: inserter} }

// ID returns the stable consumer name used as the JetStream durable and DB key.
func (h *Handler) ID() string { return "block" }

// FirstValidVersion is the earliest poktroll semver at which this consumer applies.
func (h *Handler) FirstValidVersion() string { return "v0.1.0" }

// Handle maps the sidecar's BlockEnvelope to the canonical BlockHeader and
// inserts the block row. No decoding and no router: the sidecar already
// decoded the version-invariant header (ADR-022 amendment).
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, msg consumer.Message) error {
	var env psv1.BlockEnvelope
	if err := env.Unmarshal(msg.Data); err != nil {
		return fmt.Errorf("block envelope at height %d: %w", msg.Height, err)
	}
	return h.inserter.InsertBlock(ctx, tx, &types.BlockHeader{
		Height:          env.Height,
		Time:            time.Unix(0, env.TimeUnixNano).UTC(),
		Hash:            env.Hash,
		ProposerAddress: env.ProposerAddress,
		TxCount:         int(env.TxCount),
	})
}
