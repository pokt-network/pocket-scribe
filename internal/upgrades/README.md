# internal/upgrades

Discovers applied chain upgrade plans from a Cosmos LCD endpoint and records them in the `upgrades` table (ADR-018). Powers `ps sync-upgrades` and the reconciler's periodic refresh. The `Syncer` makes two LCD calls per upgrade name — `/cosmos/upgrade/v1beta1/applied_plan/{name}` for the applied height and `/cosmos/base/tendermint/v1beta1/blocks/{h}` for the block time — then upserts the result idempotently.

## Invariants honored

- **ADR-018** — upgrade metadata (height, block time, decoder version) flows from the chain, never hardcoded; the `upgrades` table is the DB-side source of truth consumed by the router.
- **Idempotent upsert** — `store.UpsertUpgrade` uses `ON CONFLICT … DO UPDATE`; running `ps sync-upgrades` multiple times is safe (invariant 4).
- **Version name normalization** — upgrade plan names follow dotted semver ("v0.1.30"); `versionToDecoder` converts them to underscored decoder-dir form ("v0_1_30") before storing.
- **Invariant 1** — `AppliedAtTime` is the consensus block time from the LCD response, not `time.Now()`.

## Entry points

- `New(lcd, client) *Syncer` — constructs a Syncer; `client` is injectable for tests (httptest).
- `Syncer.Fetch(ctx, names) ([]store.Upgrade, error)` — pure HTTP → structs; no DB side effects.
- `Syncer.Sync(ctx, store, names) (int, error)` — Fetch + upsert; returns count written.

## Testing

- **Unit** — `internal/upgrades/upgrades_test.go` uses an `httptest.Server` to verify LCD parsing and decoder-version name conversion.
- **Integration** — `test/integration/upgrades_sync_test.go` exercises the full path against a real Postgres using a mock LCD server.
