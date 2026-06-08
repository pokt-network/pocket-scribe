---
name: generate-migration-from-diff
description: Generate a goose SQL migration for a specific poktroll version from its shape snapshot. Reads docs/research/.shapes/<vX_Y_Z>.json (produced by /generate-decoder), compares against the immediately-previous snapshot, and emits schema/migrations/NNNN_decoder_<vX_Y_Z>.sql with CREATE TABLE IF NOT EXISTS (first appearance) and ALTER TABLE ADD COLUMN IF NOT EXISTS (additive changes). NO Go decoder code (those live in internal/decoders/<v>/ and are scaffolded separately).
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# /generate-migration-from-diff

Convert a shape snapshot into an idempotent goose migration.

This skill is the deterministic bridge between **shape capture** (`/generate-decoder` → JSON snapshot) and **schema evolution** (goose migrations). It does NOT generate Go decoder code — that's wire→logical adapter work that lives in `internal/decoders/<v>/`.

## Design contract

### Schema = logical, decoder = wire-to-logical adapter

The SQL schema represents the **logical** shape of an entity (e.g. `claimed_amount BIGINT, claimed_denom TEXT`), not the wire shape (`Coin` message vs `string` representation). When poktroll changes the wire representation across versions (e.g. v0.1.27 went from `Coin claimed_upokt` to `string claimed_upokt`), the **schema does NOT change**. The decoder for each version reads its wire format and writes the same logical columns.

That keeps Hasura/PostgREST contracts stable for downstream clients and isolates wire turbulence inside the decoder package.

### No "version suffix" on column names

We tried that. It's noise. Field renames or genuine semantic-different fields → new column with a new name. Old data stays in old columns (append-only, ADR-005). Whatever ambiguity remains is resolved by `decoded_by_version` per row.

### NOT a declarative DSL

We are NOT building a versioned-YAML-entity-mapper. That's the SubQuery trap (declarative-everything → can't escape when chain breaks). The YAML in this skill is the **minimum** explicit configuration humans need to declare. Per-version parsing logic stays in Go (per-version decoder packages), where it's type-safe, testable, and debuggable.

## Inputs

1. **Version tag** — e.g. `v0.1.0`, `v0.1.27`.

The skill reads everything else:
- `docs/research/.shapes/<vX_Y_Z>.json` — must exist (run `/generate-decoder` first).
- `.claude/skills/generate-migration-from-diff/config.yaml` — entity mappings + denom aliases.

## Steps

### 1. Validate + setup

- Confirm tag matches `^v\d+\.\d+\.\d+$`.
- Confirm `docs/research/.shapes/<vX_Y_Z>.json` exists.
- Detect next migration number by scanning `schema/migrations/NNNN_*.sql`.

### 2. Load shapes + config

- Read current snapshot.
- Find previous snapshot (immediately-prior semver). May be absent (genesis).
- Read `config.yaml`:

```yaml
entities:
  pocket.shared.Supplier:
    pattern: stateful
    table: supplier_history
    id_field: operator_address
  pocket.tokenomics.EventClaimSettled:
    pattern: stateless
    table: event_claim_settled
  pocket.supplier.MsgStakeSupplier:
    pattern: stateless
    table: msg_stake_supplier

coin_denom_aliases:
  - upokt
  - pokt

coin_string_recognition: true   # treat `string <name>_<alias>` as Coin lookalike
```

### 3. Per-entity DDL generation

For each entity in `config.yaml`:

#### Compute logical columns from current shape

For each proto field of the entity in the current snapshot, derive SQL columns by these rules:

| Proto type | SQL columns |
|---|---|
| `string` | `<name>` TEXT NULL |
| `bytes` | `<name>` BYTEA NULL (all observed uses are opaque crypto/payloads) |
| `uint64`, `int64`, `sint64`, `fixed64`, `sfixed64` | `<name>` BIGINT NULL |
| `uint32`, `int32`, `sint32`, `fixed32`, `sfixed32` | `<name>` INTEGER NULL |
| `bool` | `<name>` BOOLEAN NULL |
| `double`, `float` | `<name>` DOUBLE PRECISION NULL |
| `cosmos.base.v1beta1.Coin` | `<base>_amount` BIGINT NULL, `<base>_denom` TEXT NULL where `<base>` = `<name>` minus a known denom suffix |
| `string <name>_<denom_alias>` (when `coin_string_recognition`) | same as Coin — `<base>_amount`, `<base>_denom` |
| `repeated <scalar>` | `<name>` `<sql_type>[]` NULL |
| `repeated <message>` | `<name>` JSONB NULL |
| Other message (nested) | `<name>` JSONB NULL |
| Enum | `<name>` SMALLINT NULL |

#### Stateful entities (`pattern: stateful`)

Add framework columns:
- `block_height` BIGINT NOT NULL
- `block_time` TIMESTAMPTZ NOT NULL
- `decoded_by_version` SMALLINT NOT NULL REFERENCES decoder_version(id)
- `indexed_at` TIMESTAMPTZ NOT NULL DEFAULT now()

PK: `(<id_field>, block_height)`.

Index: `(<id_field>, block_height DESC)` for "current value" queries.

#### Stateless entities (`pattern: stateless`)

Add framework columns:
- `block_height` BIGINT NOT NULL
- `block_time` TIMESTAMPTZ NOT NULL
- `tx_index` INTEGER NOT NULL DEFAULT 0
- `event_index` INTEGER NOT NULL DEFAULT 0
- `decoded_by_version` SMALLINT NOT NULL REFERENCES decoder_version(id)
- `indexed_at` TIMESTAMPTZ NOT NULL DEFAULT now()

PK: `(block_height, tx_index, event_index)`.

Hypertable: `SELECT create_hypertable('<table>', 'block_time', if_not_exists => TRUE);` after CREATE TABLE.

#### CREATE vs ALTER

- **First appearance of entity** (entity is in `added_messages` OR no previous snapshot exists): emit `CREATE TABLE IF NOT EXISTS` with all derived columns + framework columns + PK + hypertable (if stateless).
- **Existing entity with added fields**: for each new logical column the field would produce, emit `ALTER TABLE <t> ADD COLUMN IF NOT EXISTS <col> <type> NULL`.
- **Existing entity with removed fields**: emit SQL comment `-- REMOVED in <version>: <field>. Column kept (append-only).` Do NOT drop.
- **Existing entity with type-changed fields**: with consistent decomposition rules, this almost never produces a schema change (e.g. Coin → string with same denom alias maps to same columns). If a genuine schema-visible type change occurs, emit `-- TYPE CHANGE FLAG: <field> <old>→<new>. Verify decoder handles this.` and continue. NO destructive ALTER.

### 4. Register the decoder version

Always emit (at the top of the migration):

```sql
CREATE TABLE IF NOT EXISTS decoder_version (
    id        SMALLINT PRIMARY KEY,
    tag       TEXT UNIQUE NOT NULL,
    added_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE decoder_version IS 'Lookup of decoder packages. Populated by /generate-migration-from-diff. Referenced by every entity row via decoded_by_version.';

INSERT INTO decoder_version (id, tag) VALUES (<N>, '<tag>')
ON CONFLICT (tag) DO NOTHING;
```

The `<N>` is derived from the version: `vX.Y.Z` → `X*10000 + Y*100 + Z` (e.g. `v0.1.27` → `127`). Predictable, ordered, fits in SMALLINT for all foreseeable versions.

### 5. Down migration

Emit a `-- +goose Down` block that's the inverse, but with `IF EXISTS` guards. Removed-field comments don't have a down counterpart (they were no-ops). Generated CREATE TABLE produces a corresponding `DROP TABLE IF EXISTS`. Generated ALTER ADD COLUMN produces `ALTER TABLE ... DROP COLUMN IF EXISTS`.

**WARNING**: Down migrations are dangerous in production for an append-only indexer. The skill emits them for goose compliance, but the operator MUST consider whether to actually run them.

### 6. Write file

`schema/migrations/NNNN_decoder_<vX_Y_Z>.sql` with:
```
-- +goose Up
-- +goose StatementBegin
... DDL ...
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
... inverse DDL ...
-- +goose StatementEnd
```

Each statement on its own line, with leading comment describing what it does. Migration is **fully idempotent** (CREATE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, INSERT ON CONFLICT).

### 7. Report

```
OK v0.1.27 migration written.
   File: schema/migrations/0042_decoder_v0_1_27.sql
   CREATE TABLE: 0 (all entities pre-existed)
   ALTER TABLE: 1 entity (event_claim_settled +5 cols, -3 commented)
   decoder_version row: id=127, tag=v0.1.27
   Lines: 87
```

## Idempotency

- Re-running for the same tag REPLACES the existing migration file (operator can re-edit + re-run safely).
- Applied + un-applied migrations are NOT touched in the DB — the operator decides when to `goose up`.

## Out of scope

- Go decoder code generation — separate skill (Paso 3).
- Hypertable retention / compression policies — separate ADR.
- COMMENT ON for new columns — generated as best-effort using the proto field comment if extracted; otherwise placeholder.
- Reconciler invariants — separate ADR.

## References

- ADR-005 — append-only pure (no destructive ALTER).
- ADR-008 — versioned decoders (Go, not YAML).
- ADR-016 — pgx + sqlc + goose (the toolchain).
- ADR-018 — no hardcoded upgrades (upgrades table chain-driven, NOT this skill).
- `docs/research/.shapes/v*.json` — input data.
- `.claude/skills/generate-decoder/` — produces the snapshots this skill consumes.
