module github.com/pokt-network/pocketscribe

go 1.26

// Dependencies are added per-task via `go get <module>@<version>` as imports
// land (Hard rule 9), then finalized with `go mod tidy` in Task 17. The
// decoder/proto/observability deps removed here (cosmos-sdk, cometbft,
// cosmossdk.io/store, protobuf, grpc, otel) and the cometbft `replace` return
// in Phase C with the codegen pipeline.

require github.com/spf13/cobra v1.10.1

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
)
