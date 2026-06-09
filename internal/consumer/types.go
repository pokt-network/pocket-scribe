package consumer // package comment lives in internal/consumer/doc.go (do not repeat it — revive package-comments)

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Message is the runtime's view of one block-level NATS message.
type Message struct {
	Height  int64
	Subject string
	MsgID   string
	Data    []byte
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
