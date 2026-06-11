# internal/app

Composition roots for every `ps` subcommand. Each sub-package owns its own Cobra command construction, dependency wiring (config → store → NATS → domain package), and process lifecycle. `cmd/ps/main.go` is intentionally thin: it builds the cobra root via `app.Root()` and exits. Business logic lives in domain packages (`internal/consumer`, `internal/fileplugin`, `internal/store`, etc.), not here.

## Invariants honored

- **Thin CLI layer** — cobra commands parse flags and wire dependencies; no business logic or DB queries reside in `app/*`.
- **One composition root per subcommand** — `fileplugin/`, `consumer/`, `indexer/`, `reconciler/`, `migrate/`, `inspect/`, `sync/`, `deregister/` each own their own `doc.go` and `New*Cmd` constructor.
- **Config via `internal/config`** — all runtime parameters (DSN, NATS URL, LCD URL, network name) are loaded through the shared config package, not parsed inline.

## Entry points

- `app.Root() *cobra.Command` — constructs the root command with all subcommands registered; called once from `main`.
- `app/fileplugin.NewCmd()` — wires `fileplugin.Bootstrap` with config, NATS client, and metrics.
- `app/consumer.NewCmd(module)` — wires `consumer.Runtime` or `consumer.BatchRuntime` for one module.
- `app/indexer.NewCmd()` — runs all enabled consumers in a single process (fan-out of `consumer.NewCmd`).
- `app/migrate.NewCmd()` — delegates to `store.Migrate` for goose up/down/status.
- `app/sync.NewCmd()` — runs `upgrades.Syncer.Sync` once (used by `ps sync-upgrades`).

## Testing

- Composition roots are thin by design; coverage comes from integration tests in `test/integration/` that bring up the full stack via testcontainers rather than testing wiring code directly.
- **Coverage gate exclusion**: `internal/app/*` is excluded from the per-package coverage gate (`scripts/covgate`). These packages are thin cobra wiring over domain logic — their real coverage comes from integration/E2E layers (4-5), and unit-faking their store/NATS/LCD connections would be happy-path padding, which the project coverage policy forbids. The domain logic they call is gated normally. Unit tests that exist here (flag parsing, command tree shape) are kept as executable documentation even though the gate does not require them.
