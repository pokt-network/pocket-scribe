// Package block implements the consumer.Handler that decodes block headers and
// writes the block table. It decodes consumer-side via the router (ADR-008): the
// sidecar publishes raw block-{H}-meta bytes; this handler version-dispatches and
// maps to the canonical types.BlockHeader.
package block

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/pokt-network/pocketscribe/internal/consumer"
	"github.com/pokt-network/pocketscribe/internal/decoders"
	"github.com/pokt-network/pocketscribe/internal/types"
)

// Router is the subset of router.Router the block handler needs.
type Router interface {
	DecoderFor(height int64) (decoders.Decoder, error)
}

// Inserter is the store surface the handler writes through (real: store.InsertBlock).
type Inserter interface {
	InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error
}

// Handler decodes block-{H}-meta payloads and writes the block table.
type Handler struct {
	router   Router
	inserter Inserter
}

// New constructs the block handler.
func New(r Router, inserter Inserter) *Handler { return &Handler{router: r, inserter: inserter} }

// ID returns the stable consumer name used as the JetStream durable and DB key.
func (h *Handler) ID() string { return "block" }

// FirstValidVersion is the earliest poktroll semver at which this consumer applies.
func (h *Handler) FirstValidVersion() string { return "v0.1.0" }

// Handle decodes the raw meta bytes via the height-selected decoder and inserts
// the block row inside the runtime-managed transaction (invariant 5).
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, msg consumer.Message) error {
	dec, err := h.router.DecoderFor(msg.Height)
	if err != nil {
		return err
	}
	header, err := dec.DecodeBlockHeader(msg.Data)
	if err != nil {
		return err
	}
	return h.inserter.InsertBlock(ctx, tx, header)
}
