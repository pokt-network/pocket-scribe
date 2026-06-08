# Contributing to PocketScribe

Workflows for common tasks. Each one has a Claude Code slash command in `.claude/commands/` that automates the scaffolding.

## Add a new consumer (new module to index)

1. Run `/scaffold-consumer <module_name>` (e.g. `supplier`, `gateway`, `application`).
2. The command generates:
   - `internal/consumers/<module>/consumer.go` — main loop, NATS subscribe, batch handler
   - `internal/consumers/<module>/handler.go` — per-event processing
   - `internal/consumers/<module>/handler_test.go` — unit test skeleton
   - SQL migration in `schema/migrations/` for the entity's history table
   - Test golden file directory in `test/golden/`
3. Fill in the entity schema (must respect [invariants](./CLAUDE.md)):
   - PK is `(address_or_id, block_height)` (NEVER `valid_from_height`)
   - `block_height`, `block_time` columns mandatory
   - No `valid_to_*` column
   - All fields nullable except IDs and core invariants (to support multi-version protos)
4. Write a golden test: capture a real block from testnet, decode, verify expected rows.
5. Update `docs/architecture/03-data-model.md` with the new entity.

## Add a new aggregate

1. Run `/scaffold-aggregate <name> <bucket_size>` (e.g. `rewards_hourly 1h`).
2. The command generates:
   - SQL migration with the `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)`
   - `INSERT INTO aggregate_registry` row with metadata
   - Test in `test/integration/` that seeds known data and asserts the aggregate values
3. Set initial `status = 'shadow'`. Promote to `'public'` only after spot-check validation.
4. Document in `docs/architecture/04-aggregates.md`.

**Constraints**:
- All `time_bucket(...)` calls use `block_time` only.
- Sealing depends on `consumers_needed` array — list the consumers whose data feeds this aggregate.

## Onboard a new poktroll version

1. Run `/generate-decoder <version_tag>` (e.g. `v0.1.6`).
2. The command:
   - Clones `pokt-network/poktroll@<version_tag>` into a temp dir
   - Copies `proto/**` into `internal/decoders/v{X}_{Y}_{Z}/proto/`
   - Runs `buf generate` to produce Go types
   - Creates a stub `decoder.go` implementing the `Decoder` interface
3. Compare proto diffs against the previous version with `buf breaking`. If breaking:
   - Document the breaking change in `docs/decisions/` as an ADR
   - Update affected canonical types in `internal/types/`
   - Add a test in cross-version test suite
4. Update `internal/router/upgrades.go` with the new upgrade height (query `x/upgrade applied-plan` from a mainnet node).
5. Add this version to the CI matrix in `.github/workflows/proto-matrix.yml`.

## Modify an existing entity schema

**Additive changes only**. Removing or renaming columns is forbidden — old rows must remain queryable.

1. Write a new migration that `ALTER TABLE ... ADD COLUMN ... NULL`.
2. The new column is `NULL` for all heights before the change. Document the height of introduction in the comment.
3. Update consumer to populate the new field for new rows.
4. Add a test for the boundary (last row without the field, first row with it).

If you really need to "remove" a field: stop populating it for new rows, leave existing rows untouched, document the deprecation in `docs/decisions/`.

## Add a hot fix to existing mappings

1. Identify the height range affected by the bug.
2. Add a unit test that reproduces the bug at a known height.
3. Fix the mapping code. Verify the test passes.
4. Run partial reindex: `ps replay --module=<name> --from=<H1> --to=<H2>`.
5. Reconciler should report green within one cycle.

## Run tests

```bash
make test              # unit tests only (fast, no containers)
make test-component    # spins testcontainers for NATS + Postgres
make test-golden       # contract tests against versioned golden files
make test-integration  # full stack, no real poktroll node
make test-e2e          # spins a local poktroll node, slowest
```

## Commit conventions

- Imperative mood: "Add supplier consumer", not "Added".
- Reference ADR or issue: "Add supplier consumer (ADR-007)".
- One logical change per commit. Refactors separate from features.
- Pre-commit hook runs `golangci-lint`, `gofumpt`, and `buf format`.

## Pull request checklist

- [ ] Tests pass locally
- [ ] No invariant violations (see `CLAUDE.md`)
- [ ] Migration is forward-compatible (additive only for existing tables)
- [ ] If schema change: docs updated in `docs/architecture/03-data-model.md`
- [ ] If new aggregate: docs updated in `docs/architecture/04-aggregates.md`
- [ ] If new decoder version: CI matrix updated
- [ ] Commit messages reference relevant ADR if applicable
- [ ] CHANGELOG updated if user-facing
