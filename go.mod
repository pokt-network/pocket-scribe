module github.com/pokt-network/pocketscribe

go 1.26

// Direct dependencies (will be populated as code is written).
// The version pins below reflect the latest stable releases at project start.
// Update deliberately, on a feature branch, with tests.

require (
	// CLI
	github.com/spf13/cobra v1.10.1
	github.com/spf13/viper v1.21.0

	// Database
	github.com/jackc/pgx/v5 v5.8.0
	github.com/pressly/goose/v3 v3.27.0

	// Messaging
	github.com/nats-io/nats.go v1.46.1

	// Cosmos / poktroll
	cosmossdk.io/store v1.5.2
	github.com/cosmos/cosmos-sdk v0.53.0
	github.com/cometbft/cometbft v0.38.19
	google.golang.org/protobuf v1.40.0
	google.golang.org/grpc v1.78.0

	// Observability
	github.com/prometheus/client_golang v1.24.0
	go.opentelemetry.io/otel v1.40.0
	go.opentelemetry.io/otel/sdk v1.40.0

	// Testing
	github.com/stretchr/testify v1.13.0
	github.com/testcontainers/testcontainers-go v0.40.0
	github.com/testcontainers/testcontainers-go/modules/postgres v0.40.0
	github.com/testcontainers/testcontainers-go/modules/nats v0.40.0
	github.com/sebdah/goldie/v2 v2.7.0

	// Utility
	github.com/google/uuid v1.10.0
)

// Apply poktroll's cometbft fork (see poktroll/go.mod).
replace github.com/cometbft/cometbft => github.com/pokt-network/cometbft v0.38.19-0.20260116111103-1d2c8fa1e75a
