# ADR-003: Official Cosmos SDK FilePlugin + Go sidecar (over custom in-process plugin)

**Status**: Accepted
**Date**: 2026-05-21
**Authors**: Jorge Cuesta, Claude

## Context

The indexer needs to capture every block and every KV-store write from poktroll, reliably, without blocking consensus.

Cosmos SDK provides `RegisterStreamingServices(appOpts, keys)` which loads streaming plugins per `app.toml`. Plugin choices:
- **`plugin = "file"`** (built-in): writes binary protobuf files per block to a directory.
- **`plugin = "<grpc>"`** (built-in, external go-plugin): streams to a gRPC endpoint.
- **Custom Go plugin** compiled into poktroll binary.

Verified in `poktroll/app/app.go:283-285` that `RegisterStreamingServices` is called. `app.toml` already has the `[streaming.abci]` section ready.

## Decision

Use the **official FilePlugin** (`plugin = "file"`) in poktroll, with a separate **Go sidecar** that tails the output directory and publishes to NATS JetStream.

## Consequences

### Positive

- **Zero risk to consensus.** The node only runs official, well-tested SDK code. Any bug in the indexer logic can't crash the node.
- **Crash safety via filesystem.** If NATS is down or the sidecar crashes, files accumulate on disk. The sidecar resumes from cursor on restart — no data loss as long as disk space is monitored.
- **Decoupled lifecycle.** Deploy the sidecar or consumer code 1000 times; the node binary stays untouched.
- **`stop-node-on-err = true`** + full disk = node halts (good for indexer correctness). Operator alerted via disk monitoring.
- **Simple sidecar (~300 LoC).** Easy to test, easy to reason about.

### Negative

- **Two-process model.** Slightly more moving parts than an in-process plugin.
- **Disk I/O overhead.** The file write step costs latency vs. direct in-process publish (acceptable: blocks are 60s, not microseconds).
- **Disk space management.** Sidecar must rotate (delete after publish + safety window).
- **One file per block** means lots of small files. Filesystems with poor large-directory performance (ext4 without `dir_index`, old NFS) degrade.

### Neutral

- File format is `varint-length + protobuf` for each record (see `docs/research/file-plugin-spec.md`).
- Two files per block: `block-{H}-meta` (FinalizeBlock req/res) + `block-{H}-data` (StoreKVPair changes).

## Alternatives considered

### Option A: Custom Go plugin compiled into poktroll
- Pro: Lowest latency, no disk hop.
- Con: **A bug in the plugin can halt or corrupt the node.** Unacceptable risk.
- Con: Couples indexer lifecycle to node binary releases.
- **Rejected because**: consensus liveness > indexer convenience.

### Option B: gRPC plugin
- Pro: No disk I/O.
- Con: NOT crash-safe — if the gRPC endpoint is down when the node tries to push, that block is lost. Node would have to halt to avoid silent loss.
- Con: Tightly couples node uptime to indexer uptime.
- **Rejected because**: violates "indexer crash must not affect node" principle.

### Option C: Polling RPC (Pocketdex today)
- Pro: No plugin needed.
- Con: Polling latency, no KV-store granularity, doesn't scale to bulk-op blocks (3k suppliers restaking = 3k RPC calls).
- **Rejected because**: this is the root cause of current pain.

## Implementation notes

### poktroll `app.toml`

```toml
[streaming]
  [streaming.abci]
    keys = ["supplier","application","gateway","service","session","proof","tokenomics","bank","auth"]
    plugin = "file"
    stop-node-on-err = true

[streamers.file]
write_dir = "/var/lib/poktroll/streaming"
prefix = ""
output-metadata = "true"
stop-node-on-error = "true"
fsync = "true"      # production: pay durability cost
```

### Sidecar pattern (cmd/ps fileplugin)

```
Loop:
  files = list dir, sorted by height, excluding .tmp
  for each file with height > cursor:
    payload = read file (tolerate truncated trailing record)
    publish to NATS with Nats-Msg-Id = "block-{H}"
    wait for ack
    update cursor on disk
    if height < (chain_head - safety_window):
      delete file
  wait for inotify or 1s tick
```

Same-host deployment preferred. NFS works as fallback (loses inotify, must poll).

### Failure modes

| Failure | Behavior |
|---|---|
| Sidecar crashes | Files persist. Restart resumes from cursor. Zero loss. |
| NATS down | Sidecar stops deleting. Disk fills. Node halts via `stop-node-on-err`. |
| Sidecar slow | Files accumulate. Bounded by safety window. |
| Partial file (node crash mid-write) | Sidecar treats truncated trailing record as "not finalized, retry". |

## References

- Full session transcript: Topic 7 (verified poktroll support), Topic 8 (sidecar pattern).
- `docs/research/file-plugin-spec.md` — detailed format and gotchas.
- ADR-004 (NATS JetStream) is the downstream of this pipeline.
