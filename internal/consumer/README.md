# internal/consumer

Generic consumer runtime for PocketScribe. Subscribes to NATS JetStream, buffers messages per block height, and writes snapshots to Postgres inside a single ack-after-commit transaction (invariant 5). Provides two runtimes: `Runtime` for one-message-per-height consumers (block metadata) and `BatchRuntime` for fan-out consumers (tx/event/KV) that buffer until the `pokt.block.{H}` envelope arrives as a completeness fence (ADR-024).

## Invariants honored

- **Invariant 1** — `time.Now()` never touches chain-data rows; the valve/eviction clock (`BatchConfig.Now`) is for operational decisions only.
- **Invariant 2 & 3** — handlers receive a `pgx.Tx` and must only append rows; no UPDATE on history tables.
- **Invariant 4** — `Nats-Msg-Id` dedup via the `seen` map absorbs AckWait redeliveries without double-inserting.
- **Invariant 5** — NATS ack fires strictly after `tx.Commit`; on crash the message is redelivered and the upsert is a no-op.
- **ADR-024** — `BatchRuntime` implements the block-boundary fence, size valve (trigger 2, default 5000 rows), time valve (trigger 3, default 5 s), and orphaned-buffer eviction (Phase G amendment).

## Entry points

- `Runtime` — single-message consumer; wraps one `Handler` implementation.
- `NewRuntime(Config) *Runtime` — constructs and wires collaborators.
- `BatchRuntime` — fan-out consumer; buffers per height, flushes atomically on the envelope.
- `NewBatchRuntime(BatchConfig) *BatchRuntime` — constructs with valve knobs.
- `Handler` / `BatchHandler` — interfaces implemented by module packages (`consumer/block`, `consumer/supplier`, …).
- `Message` — runtime's view of one NATS message (height, subject, MsgID, `TimeUnixNano`, data).

## Testing

- **Unit** — `internal/consumer/batch_test.go` covers valve triggers, eviction, and dedup with injected clocks and fake `flushFn`/`processFn`.
- **Integration** — `test/integration/runtime_test.go`, `batch_valves_test.go`, `cursor_test.go` use testcontainers (NATS + Postgres) to verify ack-after-commit, gap detection, and reconnect behaviour under real JetStream.
