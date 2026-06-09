module github.com/pokt-network/pocketscribe

go 1.26

// Dependencies are added per-task via `go get <module>@<version>` as imports
// land (Hard rule 9), then finalized with `go mod tidy` in Task 17. The
// decoder/proto/observability deps removed here (cosmos-sdk, cometbft,
// cosmossdk.io/store, protobuf, grpc, otel) and the cometbft `replace` return
// in Phase C with the codegen pipeline.

require (
	github.com/jackc/pgx/v5 v5.8.0
	github.com/pressly/goose/v3 v3.27.0
	github.com/spf13/cobra v1.10.1
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/sethvargo/go-retry v0.3.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.36.0 // indirect
)
