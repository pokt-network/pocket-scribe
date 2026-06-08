# FilePlugin Specification (Cosmos SDK v0.53.0)

> **Source caveat**: Some findings below were synthesized from cosmos-sdk documentation (ADR-038), `pkg.go.dev` API docs, and GitHub issue summaries, rather than direct source inspection. Re-verify exact line numbers and field names against `/tmp/cosmos-sdk/store/streaming/file/service.go` and a `poktrolld init`-generated `app.toml` before relying on byte-level details.

## Where the code lives

| Path in cosmos-sdk | Purpose |
|---|---|
| `store/streaming/streaming.go` | Top-level `NewStreamingService(...)` factory + `ServiceConstructorLookupTable`. Where the string `"file"` resolves. |
| `store/streaming/file/service.go` | `FileStreamingService` — implements `baseapp.ABCIListener`. Owns write directory, prefix, optional fsync, per-block file handles. |
| `store/streaming/file/service_test.go` | Tests (see Issue #14302 — flaky/skipped). |
| `baseapp/streaming.go` | `RegisterStreamingServices(appOpts, keys)` — reads `[streaming.abci]`, instantiates services. |
| `baseapp/abci_listener.go` | Defines `ABCIListener` interface. |
| `store/types/listening.pb.go` | Generated protobuf — including `StoreKVPair`. |

poktroll's `app/app.go:283-285` calls `app.RegisterStreamingServices(appOpts, app.kvStoreKeys())`. Single seam, fully driven by `app.toml`.

## File output format

**Two files per block** in the configured `write_dir`:

```
{prefix}block-{height}-meta
{prefix}block-{height}-data
```

- `block-{height}-meta` — length-prefixed protobuf with the ABCI **request/response pair**. In v0.53 (post-ABCI 2.0) that's `RequestFinalizeBlock` / `ResponseFinalizeBlock` plus the `Commit` response. (v0.47 had the separate `BeginBlock`/`DeliverTx`/`EndBlock`/`Commit`.)
- `block-{height}-data` — length-prefixed protobuf-encoded `StoreKVPair` messages, one per Set/Delete on any listened KVStore during the block.

**Encoding**: every record is `varint-length + proto-encoded bytes`. No JSON mode.

**Prefix**: configurable, prepended verbatim. E.g. `prefix = "pokt-"` → `pokt-block-42-data`.

**Atomicity (important)**: files are created at the **start** of `ListenFinalizeBlock` and closed at end of `ListenCommit`. **No rename-on-close.** A crashed node can leave partially-written `block-N-*` pairs.

- If `fsync = true`, `Sync()` is called before close (durable but slow).
- If `fsync = false` (default), kernel page cache only — data can vanish on power loss.

**Implication for PocketScribe sidecar**: must tolerate truncated trailing records. Treat unexpected EOF on the trailing record as "block not yet complete, retry later."

## `app.toml` schema (v0.53.0)

```toml
###############################################################################
###                        Streaming                                        ###
###############################################################################

[streaming]

  [streaming.abci]
    # Store keys to stream. MUST match the store key names from app.go.
    # Empty array = stream NOTHING (not "all"). No wildcard.
    # To stream all stores, enumerate every key explicitly.
    keys = []
    
    # Plugin name. Built-in: "file". External: name of a go-plugin binary.
    # Strings like "abci", "abci_v1", "grpc" are NOT recognized.
    plugin = ""
    
    # Halt the node if the listener returns an error from
    # ListenFinalizeBlock or ListenCommit.
    # DEFAULT: true (preserves correctness; we WANT this for an indexer).
    stop-node-on-err = true
```

The **file-plugin-specific options** typically live in a companion `[streamers.file]` block (kept for backward compatibility from v0.46/v0.47):

```toml
[streamers.file]
keys = ["acc", "bank", "gov", "staking", "mint", "distribution", "slashing",
        "ibc", "upgrade", "evidence", "transfer", "feegrant", "authz", "capability",
        "application", "supplier", "session", "service", "proof", "tokenomics",
        "shared", "gateway"]
write_dir = "/var/lib/poktroll/streaming"
prefix = ""
output-metadata = "true"
stop-node-on-error = "true"
fsync = "false"
```

Note **string-typed booleans** (`"true"`, `"false"`) — parsed via `cast.ToBool`, so `true`/`"true"`/`1` all work.

**`output-metadata = "false"`** would skip the `block-{height}-meta` file. **Don't do this for PocketScribe** — without metadata you lose access to txs, events, validator updates, and `app_hash`.

## The `ABCIListener` interface

In v0.53 (post-ABCI 2.0):

```go
type ABCIListener interface {
    ListenFinalizeBlock(ctx context.Context,
        req abci.RequestFinalizeBlock,
        res abci.ResponseFinalizeBlock) error

    ListenCommit(ctx context.Context,
        res abci.ResponseCommit,
        changeSet []*storetypes.StoreKVPair) error
}
```

`StoreKVPair`:

```go
type StoreKVPair struct {
    StoreKey string  // module store key, e.g. "supplier", "bank"
    Delete   bool    // true => deletion; false => set
    Key      []byte  // raw KV key (module-encoded)
    Value    []byte  // raw KV value (nil/empty for Delete)
}
```

**Ordering**: `changeSet` is ordered by write sequence within the block — txs in block order, then EndBlocker writes. Deterministic across non-Byzantine nodes.

**Error semantics**: returning a non-nil error → panic + halt node (if `stop-node-on-err=true`). Otherwise logged and discarded. **No built-in retry, no buffering, no backpressure.**

## Known gotchas

1. **#14302 — `TestFileStreamingService` flaky/skipped in CI.** Regressions can land unnoticed. PocketScribe should add its own integration test that ingests a real `block-N-*` pair and asserts proto round-trip.
2. **Halted node on full disk.** With `stop-node-on-err=true` + `fsync=true`, full disk → consensus halts. Mount `write_dir` on a separate disk; alert on free space.
3. **Partial file on crash.** No rename-on-close. Sidecar must tolerate truncated final record.
4. **`keys = []` indexes NOTHING.** No warning logged. First-time gotcha.
5. **No file rotation.** One file pair per block forever (~17k files/day at 5s blocks; ~1.4k/day at 60s Pocket blocks). Sidecar must delete processed files.
6. **`output-metadata = "false"` breaks height correlation.** Don't disable.
7. **`StoreKey` is the module key string** (`"bank"`), not the namespace prefix (`"\x02"`). Raw key/value bytes still need module-side codec helpers to decode.
8. **ABCI 2.0 changed metadata.** v0.50+ has `RequestFinalizeBlock`/`ResponseFinalizeBlock`. Any v0.47-era decoder fails on v0.53 metadata.

## v0.50.x vs v0.53.x

**Interface unchanged.** Same `ListenFinalizeBlock` / `ListenCommit`, same `StoreKVPair`. File format on disk identical.

What changed (none breaks a FilePlugin consumer):
- Import path consolidated under `cosmossdk.io/store/types`.
- `baseapp/streaming.go` now reads `[streaming.abci]` exclusively for listener selection; `[streamers.file]` keys are read by the file plugin itself.
- x/auth `PreBlocker` requirement (unrelated).

**A FilePlugin reader written against v0.50 works on v0.53 unchanged.**

## Plugin string values

Two resolution paths:

1. **Built-in lookup.** `ServiceConstructorLookupTable` in `store/streaming/streaming.go`. Only upstream entry: `"file"` → `file.NewStreamingService`.
2. **External go-plugin binary.** Any string not in the table is treated as a binary name (must be on `$PATH` or absolute path). Handshake cookie: `grpc_abci_v1.Handshake`.

**For PocketScribe: use `plugin = "file"`.** Built-in, zero external dependency, on-disk format documented above.

## PocketScribe-specific recommendations

Based on the above:

1. **Use `plugin = "file"`** with explicit `keys` enumerating all poktroll stores we care about: `supplier`, `application`, `gateway`, `service`, `session`, `proof`, `tokenomics`, `shared`, `bank`, `auth`.
2. **Set `stop-node-on-err = true`** — we want consensus halts over silent drift.
3. **Set `fsync = "true"`** in production — pay the durability cost. Loss of recent KV changes during power failure is unacceptable for an indexer.
4. **Set `output-metadata = "true"`** — we need block metadata for `block_time`, tx context, events.
5. **Choose a `write_dir`** on a dedicated disk volume.
6. **The sidecar (`ps fileplugin`)** must:
   - Use `inotify` (on same host) to detect new files.
   - Ignore `.tmp` and partial trailing records.
   - Process **both** the `-meta` and `-data` files for a height as an atomic unit (don't publish meta without data or vice versa).
   - Tolerate the gotchas above (truncated final record on crash, missing meta, etc.).
7. **Integration test** that:
   - Generates a real block file pair from a local poktroll devnet.
   - Reads them with the sidecar's parser.
   - Asserts the decoded `RequestFinalizeBlock` + `[]StoreKVPair` are non-empty and round-trip cleanly.

## Sources

- [ADR 038: KVStore state listening (v0.50 docs)](https://docs.cosmos.network/v0.50/build/architecture/adr-038-state-listening)
- [ADR 038 source (main branch)](https://github.com/cosmos/cosmos-sdk/blob/main/docs/architecture/adr-038-state-listening.md)
- [`store/streaming` on pkg.go.dev](https://pkg.go.dev/github.com/cosmos/cosmos-sdk/store/streaming)
- [`cosmossdk.io/store/types` (StoreKVPair)](https://pkg.go.dev/cosmossdk.io/store/types)
- [Issue #14302 — TestFileStreamingService flaky/skipped](https://github.com/cosmos/cosmos-sdk/issues/14302)
- [Issue #10096 — ADR-038 Implementation tracking](https://github.com/cosmos/cosmos-sdk/issues/10096)
- [v0.47 app.toml template](https://github.com/cosmos/cosmos-sdk/blob/main/tools/confix/data/v0.47-app.toml)
- [Release v0.53.0](https://github.com/cosmos/cosmos-sdk/releases/tag/v0.53.0)
- [mtps/cosmos-streaming-plugin (external plugin reference)](https://github.com/mtps/cosmos-streaming-plugin)
