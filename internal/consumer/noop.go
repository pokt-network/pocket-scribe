package consumer

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// NoOpHandler is a consumer that records progress (processed_heights +
// consolidation via the runtime) but writes no data rows. Used to validate the
// orchestration and AND-seal logic without any chain decoding.
type NoOpHandler struct {
	id                string
	firstValidVersion string
}

// NewNoOpHandler builds a NoOpHandler with the given id and first-valid version.
func NewNoOpHandler(id, firstValidVersion string) NoOpHandler {
	return NoOpHandler{id: id, firstValidVersion: firstValidVersion}
}

// ID returns the consumer's stable name.
func (h NoOpHandler) ID() string { return h.id }

// FirstValidVersion returns the semver tag recorded in consumer_registry.
func (h NoOpHandler) FirstValidVersion() string { return h.firstValidVersion }

// Handle writes nothing.
func (h NoOpHandler) Handle(_ context.Context, _ pgx.Tx, _ Message) error { return nil }
