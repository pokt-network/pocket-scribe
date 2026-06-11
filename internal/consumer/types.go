package consumer // package comment lives in internal/consumer/doc.go (do not repeat it — revive package-comments)

import (
	"context"

	"github.com/jackc/pgx/v5"

	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
)

// Message is the runtime's view of one block-level NATS message.
type Message struct {
	Height       int64
	Subject      string
	MsgID        string
	TimeUnixNano int64 // Pocket-Block-Time header; 0 when absent (pre-Phase-G streams)
	Data         []byte
}

// Handler is the per-module business logic invoked inside the ack-after-commit
// transaction. In Phase B the only implementation is NoOpHandler; real handlers
// (block, supplier, …) arrive in Phase D+.
type Handler interface {
	// ID is the stable consumer name (also the consumer_registry PK and the
	// JetStream durable name).
	ID() string
	// FirstValidVersion is the semver tag at which this consumer becomes
	// applicable (e.g. "v0.1.0"). Stored in consumer_registry; height-gating of
	// the required set is deferred to Phase F.
	FirstValidVersion() string
	// Handle writes this consumer's data rows for msg within tx. It MUST NOT
	// commit or roll back — the runtime owns the transaction.
	Handle(ctx context.Context, tx pgx.Tx, msg Message) error
}

// BatchHandler is the per-module logic for fan-out consumers (ADR-024): the
// runtime buffers a height's messages and calls FlushHeight ONCE inside the
// ack-after-commit transaction when the BlockEnvelope (the fence) arrives.
// msgs is every buffered fan-out message for the height in arrival order
// (deduplicated by Nats-Msg-Id); it is EMPTY for quiet heights — the handler
// must succeed writing nothing so the cursor still advances.
type BatchHandler interface {
	ID() string
	FirstValidVersion() string
	FlushHeight(ctx context.Context, tx pgx.Tx, env *psv1.BlockEnvelope, msgs []Message) error
}
