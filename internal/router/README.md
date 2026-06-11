# internal/router

Resolves a block height to the `decoders.Decoder` implementation active at that height. Resolution is purely version-based: the only per-network input is the contents of the `upgrades` table (which protocol version was applied at which height); the logic never branches on network name. A `DBRouter` loads the table into an in-memory `staticRouter` snapshot and supports periodic `Refresh` as new upgrades land (ADR-018).

## Invariants honored

- **Version-based, never network-based** — decoders are keyed on protocol version strings (e.g. `"v0_1_30"`); the only per-network data flows through the `upgrades` table.
- **ADR-018** — upgrade boundaries come from the DB; no heights are hardcoded.
- **Lenient fallback** — if an upgrade's `decoder_version` is not yet registered, `DecoderFor` falls back to the nearest earlier registered version (correct for the version-invariant block header during incremental decoder rollout).
- **Shape-guard** — `internal/router/shapeguard_test.go` maintains the transitive proto shape closure per entity; new modules must be seeded here before their decoders are written.

## Entry points

- `Router` interface — `DecoderFor(height int64) (decoders.Decoder, error)`.
- `NewStaticRouter(upgrades, registry, genesisVersion) (Router, error)` — in-memory router from a pre-loaded upgrade set.
- `DBRouter` — wraps `staticRouter`; refreshes from `store.ListUpgrades`.
- `NewDBRouter(ctx, store, registry, genesisVersion) (*DBRouter, error)` — constructs and loads in one step.
- `DefaultRegistry() map[string]decoders.Decoder` — maps canonical version strings to registered decoder singletons.

## Testing

- **Unit** — `internal/router/router_test.go` verifies height→version resolution including gaps and lenient fallback; `mainnet_boundaries_test.go` pins known production upgrade heights; `shapeguard_test.go` verifies shape closure per module/version pair.
- **Integration** — `test/integration/store_router_test.go` exercises `DBRouter.Refresh` against a live Postgres.
