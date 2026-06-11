# internal/decoders

Version-aware proto decoders for Pocket Network's Shannon protocol. The root package defines the `Decoder` interface and shared utility functions (block header parsing, record splitting, KV store key extraction). Each sub-directory `v{X}_{Y}_{Z}/` corresponds to a specific poktroll git tag and implements `Decoder` for that version's proto shapes — the directory is immutable once committed.

## Invariants honored

- **One directory per version** — `v0_1_0`, `v0_1_8`, … are sealed; bugs are fixed in-place but the directory is never repurposed for a different version.
- **Generated code is read-only** — `v*/gen/` is produced by `buf`; never hand-edited.
- **100% decoder coverage required** — every `Decode*KV` method must have a golden test using real captured fixture data (`sebdah/goldie/v2`).
- **No shape-stability assumptions** — proto shapes are non-monotonic; every new module must expand the shape-guard seed list in `internal/router/shapeguard_test.go` before any decoder package is written (machine-derived shape proof required).
- **Errors returned, never logged** — decode failures propagate to the consumer; the consumer decides not to ack.
- **`time.Now()` forbidden** — decoders produce canonical types from bytes only (invariant 1).

## Entry points

- `Decoder` interface — `Version()`, `DecodeBlockHeader()`, `DecodeSupplierMsg()`, `DecodeSupplierEvent()`, `DecodeSupplierKV()`.
- `DecodeBlockHeader(metaBytes []byte) (*types.BlockHeader, error)` — shared implementation used by all version adapters.
- `SplitMeta(data []byte) ([][]byte, error)` — splits a FilePlugin meta payload into its length-prefixed records.
- `ReadDelimited(buf []byte) (payload []byte, consumed int, err error)` — reads one varint-length-prefixed record from a data file.
- `StoreKeyOf(payload []byte) (string, error)` — extracts the Cosmos store key from a raw `StoreKVPair`.

## Testing

- **Golden/contract** — `internal/decoders/v*/` tests use `sebdah/goldie/v2` against `test/fixtures/` snapshots; 100% coverage on all `Decode*` methods is mandatory.
- **Unit** — `blockheader_test.go`, `meta_test.go`, `supplierevents_test.go`, `supplierkv_test.go` in the root package test shared utilities.
- **Integration** — `test/integration/store_router_test.go` cross-checks canonical type equivalence across version decoder implementations.
