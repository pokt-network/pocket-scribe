# internal/fileplugin

Sidecar that reads Cosmos SDK FilePlugin output (pairs of `block-{H}-meta` and `block-{H}-data` files) and publishes every block's contents as the ADR-022 fan-out onto NATS JetStream. Each block produces per-tx (`pokt.tx.{H}.{i}`), per-event (`pokt.events.{type}.{H}`), and per-KV (`pokt.kv.{store}.{H}`) messages, followed **last** by the metadata-only `BlockEnvelope` on `pokt.block.{H}` — the envelope ordering guarantees consumers can treat it as a completeness fence.

## Invariants honored

- **ADR-022** — fan-out subject taxonomy and envelope-last ordering contract.
- **ADR-027** — both meta and data files are always expected together; absence of the data file is a hard error, never silently skipped.
- Payload caps: 256 KiB soft cap (WARN + metric, still published); 1 MiB hard cap (refused, height left un-acked so no silent message loss).
- `Nats-Msg-Id` derived from `(subject, height, intra-block index)` — deterministic and replay-safe (invariant 4).
- `Pocket-Block-Time` header stamped from the consensus header on every fan-out message (required by `BatchRuntime` partial flushes — ADR-022 Phase G amendment).

## Entry points

- `Bootstrap(ctx, client, dir, maxHeight, chainID, fpm) (heights, messages int, err error)` — publishes all heights found in `dir` up to `maxHeight`; the main entry point for `ps fileplugin`.
- `SoftCapBytes = 256 KiB` / `HardCapBytes = 1 MiB` — exported payload cap thresholds.

## Testing

- **Unit** — `internal/fileplugin/bootstrap_test.go` uses captured real fixtures in `test/fixtures/` (golden/contract layer) to verify subject grammar, envelope ordering, and `Nats-Msg-Id` determinism. `sizecap_test.go` covers the 256 KiB/1 MiB policy.
- **Integration** — `test/integration/fileplugin_test.go`, `sidecar_caps_test.go` spin real NATS JetStream via testcontainers and assert published message counts and hard-cap refusal.
