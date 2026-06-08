---
paths:
  - "internal/consumer/**/*.go"
---

# Consumer rules

When editing consumer code (the modules that read NATS and write to Postgres):

1. **Transaction order is sacred**: `BEGIN` → upsert data → insert into `processed_heights` → `COMMIT` → `Ack` NATS. Never ack before commit.
2. **Idempotent upserts**: every INSERT uses `ON CONFLICT (pk) DO UPDATE/NOTHING`. Same input replayed N times = same DB state.
3. **Deterministic IDs**: `(block_height, tx_index, event_index)` or `(address, block_height)` — chain-derived only.
4. **Append-only history**: never UPDATE rows in `*_history` tables. Insert new rows with `(id, block_height)` PK.
5. **No event-derived state**: never `UPDATE supplier SET stake = stake + delta`. Snapshot from chain (KV write or gRPC).
6. **Use canonical types** from `internal/types/`. Decode via the router-selected decoder.
7. **Use subjects from `internal/nats/subjects.go`** — never hard-code subject strings.
8. **Metrics**: increment `pocketscribe_consumer_processed_blocks_total{consumer=X}` per block processed. Record durations in histograms.
9. **Logging**: structured (`slog`) with `service`, `module`, `block_height`, `block_time` fields.

Anti-patterns:
- ❌ `db.Exec("UPDATE supplier_history ...")`.
- ❌ Caching entity state in memory (Redis OK as performance optimization, but DB is truth).
- ❌ `INSERT INTO ...` without ON CONFLICT.
- ❌ Calling `time.Now()` in mappings (Invariant 1).
- ❌ Hard-coded subject strings (`"pokt.kv.supplier.123"`).

See `docs/architecture/02-ingestion.md`, ADR-005, ADR-006, ADR-007.
