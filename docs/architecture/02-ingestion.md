# 02 — Ingestion: from chain to consumers

## Why this design

Three options were evaluated for getting chain data into the indexer:

1. **Polling RPC** (Pocketdex today): polls block-by-block via gRPC. Single-threaded, latency-bound, doesn't see KV-level state changes. Rejected.
2. **Custom in-process plugin** in poktroll node: writes protobuf events directly into NATS or Postgres from inside the consensus binary. Powerful but **any bug kills consensus**. Rejected.
3. **Official Cosmos SDK FilePlugin + sidecar** (chosen): the SDK's official plugin writes one binary file per block to a directory. A separate sidecar process tails the directory and publishes to NATS. Node and indexer are fully decoupled.

The "FilePlugin + sidecar" design is sometimes called the **outbox pattern** in distributed systems. It uses the filesystem as a durable, append-only queue between the node and the message bus. If NATS is down, files accumulate on disk; the node never blocks (until disk fills, at which point `stop-node-on-err=true` halts the node — better than silent drift).

## poktroll configuration

poktroll uses Cosmos SDK v0.53.0 and natively supports `RegisterStreamingServices` (verified in `app/app.go:283`). The operator enables it via `app.toml`:

```toml
[streaming]

  [streaming.abci]
    keys = ["supplier","application","gateway","service","session","proof","tokenomics","bank","auth"]
    plugin = "file"
    stop-node-on-err = true
```

- `keys` selects which KV stores emit changes. Empty array = all keys, but explicit list is safer (avoids noise from internal stores).
- `plugin = "file"` uses the SDK's built-in FilePlugin. Other valid values exist for gRPC/in-process variants but we don't use those.
- `stop-node-on-err = true` is critical: if the plugin can't write (disk full, permission error, fs corruption), the node halts. We **want** this — better halt than silent drift.

Plus the file-plugin-specific options that go in the same file (exact key names verified in our research doc at `docs/research/file-plugin-spec.md`):

```toml
  [streaming.abci.file]
    output-dir = "/var/lib/poktroll/streaming"
    # one binary file per block: block-{height}.{ts}
    # contains FinalizeBlock req/res + KV changes for selected stores
```

## What the FilePlugin emits per block

Per the SDK's `store/streaming/file/file.go`, one file per block is written with a protobuf-encoded payload containing:

- `req` — the `FinalizeBlock` request (block header, txs, time)
- `res` — the `FinalizeBlock` response (events, results)
- `state_changes` — the `[]StoreKVPair` representing all KV writes during block processing, scoped to the configured `keys`

The naming convention is `{prefix}block-{height}-meta` (FinalizeBlock req/res) and `{prefix}block-{height}-data` (StoreKVPair changes). **No `.bin` suffix.** Verified in `docs/research/file-plugin-spec.md`.

**Atomicity**: the plugin writes to a temporary file (`.tmp` suffix) and renames atomically to the final name. Consumers must ignore `.tmp` files.

## Sidecar publisher

The sidecar (`cmd/ps (ps fileplugin)`) is a small Go binary that runs **on the same host as the node** (or, less ideally, on a separate host via NFS). Its job:

1. Tail the streaming output directory (`inotify` on local fs, polling on NFS).
2. For each new finalized file (not `.tmp`):
   - Read it.
   - Parse just enough to know the height and to extract per-store fan-out.
   - Publish to NATS subjects with `Nats-Msg-Id = "block-{height}"` (or `"kv-{height}-{store}-{keyhash}"` for finer-grained subjects).
   - Wait for NATS ack (synchronous publish).
   - Advance the on-disk cursor (`/var/lib/pocketscribe/cursor`).
   - Delete the file **only if** `block_height < (chain_head - safety_window)`. Keep recent files for emergency resync.

Pseudocode in `CLAUDE.md` and full reference implementation will live in `internal/app/fileplugin/` (invoked via `ps fileplugin`).

### Sidecar invariants

- **Ack from NATS before delete**: never delete a file that hasn't been confirmed by the JetStream stream.
- **Cursor advance after publish**: the cursor (`/var/lib/pocketscribe/cursor`) is a single integer (the latest successfully published height). Writing it atomically uses `rename(tmpfile, cursor)`.
- **Crash recovery**: on startup, read cursor → list all files in directory → process files with `height > cursor`. Files for `height <= cursor` are safe to delete on encounter (already published; NATS dedups duplicates anyway).
- **Backpressure**: if NATS is slow/down, the sidecar stops deleting. Disk fills. `stop-node-on-err=true` eventually halts the node. Operator alerted.

### Same-host vs network (NFS, MinIO, etc.)

| Option | When to choose |
|---|---|
| **Sidecar on same host** | Default. Best latency (inotify, zero network), simplest ops. Recommended unless node host has zero headroom. |
| **NFS** | Sidecar on separate host. Works but loses inotify (polling adds 100-500ms). Atomic rename respected on same mount. Stale handles on NFS server reboot — use `hard,intr` mount. |
| **MinIO sidecar on node + pull on indexer host** | Multi-consumer firehose, geo-distributed reads, durable archive. Adds two components but useful at scale. |
| **rsync/lsyncd** | Quick hack, not production. |

Default: same-host. See `docs/operations/deployment.md` for sizing.

## NATS JetStream layout

Stream definition (configured at provisioning):

```
Stream: POKT_CHAIN
  Subjects: pokt.>
  Storage: file
  Retention: limits
  MaxAge: 30d
  Replicas: 3
  Discard: old
  Duplicate window: 24h    # dedup window on Nats-Msg-Id
  MaxConsumers: -1
```

Subjects (hierarchical, supports filtering):

- `pokt.block.{height}` — the full block payload (used by consumers that need everything)
- `pokt.kv.{store}.{height}` — per-store KV writes (most consumers subscribe here)
- `pokt.events.{event_type}.{height}` — fan-out by event type for easy filtering

The sidecar publishes to **all three** for each block — JetStream dedupes per-subject via Msg-Id, so storage is bounded.

Optional partition fan-out by entity hash (for horizontal scaling, see `07-ha-scaling.md`):

```
pokt.kv.supplier.{partition}.{height}
  where partition = sha256(address) % N
```

We start without partitioning — append-only commutativity lets us use queue groups with non-ordered delivery and add partitioning later if needed.

## Consumer pattern

Each consumer (`ps consumer <module>`, e.g. `ps consumer supplier`):

1. Connects to NATS, subscribes to a durable consumer on filtered subjects.
2. For each message:
   - `BEGIN` Postgres tx.
   - Decode the payload using the appropriate decoder version (looked up via `internal/router` based on the block height).
   - For each affected entity: upsert into `*_history` and the relevant event hypertables.
   - Insert `processed_heights (consumer_name, block_height)` in the same tx.
   - `COMMIT`.
   - `Ack` the NATS message.
3. On crash: NATS redelivers unacked messages; idempotent upserts make this a no-op.

JetStream consumer config:

```
Consumer: supplier-indexer
  Durable: true
  FilterSubject: pokt.kv.supplier.>
  AckPolicy: explicit
  MaxAckPending: 1            # serial within consumer; multiple workers via queue group
  DeliverPolicy: from-sequence (last-acked) on restart
```

For horizontal scale: multiple processes with the same `Durable` name form a queue group; NATS load-balances. For strict ordering per entity, use partitioned subjects + one consumer per partition.

## Failure modes covered

| Failure | Behavior |
|---|---|
| Sidecar crashes | Files accumulate. Sidecar restarts, reads cursor, resumes. No data loss. |
| Sidecar OOM during heavy backfill | Same as crash. Tune `GOGC` or use `pull-batch-size`. |
| NATS unavailable | Sidecar stops deleting files. Disk fills. Node halts (configurable). Operator alerted. |
| Consumer crashes | Unacked msgs redelivered on restart. Idempotent upserts handle replay. |
| Postgres unavailable | Consumer fails to commit; doesn't ack; NATS retries. Backoff with jitter. |
| Disk full on archive node | `stop-node-on-err=true` halts node. **Better than silent drift.** Operator alerted. |
| Network partition between sidecar and NATS | Sidecar stops deleting. Files accumulate. NATS dedup eats duplicates on reconnect. |
| Double publish (HA scenario) | Two archive nodes both publishing → JetStream dedup by Msg-Id eats duplicate. |

See `docs/operations/disaster-recovery.md` for procedures.
