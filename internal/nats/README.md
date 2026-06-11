# internal/nats

Single source of truth for JetStream subject naming and NATS client wiring (CLAUDE.md DRY invariant; ADR-022). All subject strings, stream names, and helper functions live exclusively here — consumers and the fileplugin sidecar import from this package instead of hard-coding strings.

## Invariants honored

- **DRY** — `subjects.go` is the only place subject patterns are defined; no other file may redeclare them.
- **ADR-022** — the four subject grammars (`pokt.block.{H}`, `pokt.tx.{H}.{i}`, `pokt.events.{type}.{H}`, `pokt.kv.{store}.{H}`) are encoded here as constants and constructor functions.
- **Deterministic `Nats-Msg-Id`** — `MsgID(subject, height, index)` produces a stable dedup key (invariant 4); replaying the same block always yields the same IDs.
- **`Pocket-Block-Time` header** — `HeaderBlockTime` constant ensures the header name is consistent between the sidecar (writer) and consumer runtimes (reader).

## Entry points

- `StreamName = "POKT"` / `StreamSubjects` — JetStream stream config constants.
- `HeaderBlockTime = "Pocket-Block-Time"` — consensus block time header (unix nanoseconds).
- `BlockSubject(h) / TxSubject(h, idx) / EventSubject(type, h) / KVSubject(store, h)` — subject constructors.
- `MsgID(subject, height, index) string` — deterministic dedup key.
- `HeightFromSubject(subject) (int64, error)` — universal height extractor dispatching all four grammars.
- `IsBlockSubject / IsTxSubject / IsEventSubject / IsKVSubject` — fast classification helpers.
- `Client` — thin JetStream client wrapper (`client.go`).

## Testing

- **Unit** — `internal/nats/subjects_test.go` round-trips every subject grammar (construct → parse → compare) and verifies `MsgID` stability.
