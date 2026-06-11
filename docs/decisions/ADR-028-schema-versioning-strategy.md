# ADR-028: Schema versioning strategy — nullable superset, decoder-as-adapter

**Status**: Accepted (implemented in Slice 1; status updated 2026-06-11)
**Date**: 2026-05-30
**Authors**: jorge.s.cuesta@gmail.com (with Claude)
**Related**: ADR-005 (append-only pure), ADR-006 (chain as source of truth), ADR-008 (versioned decoders), ADR-016 (pgx + sqlc + goose), ADR-018 (no hardcoded upgrades)

## Context

PocketScribe must index a chain whose protobuf shapes evolve across releases. Across 33 vendored poktroll versions (v0.1.0 → v0.1.33) plus their dependency cosmos-sdk (v0.50.13 then v0.53.0), the same logical entity (e.g. `EventClaimSettled`) appears in several distinct wire shapes — fields added, removed, and (in v0.1.27) renamed and re-typed. A naive "rebuild the table every time the chain changes" strategy makes append-only impossible, breaks reproducibility, and burns the reconciler's ability to validate history.

We also rejected SubQuery-style declarative-version-mapping YAML up front — that path leads to a parser DSL that grows hair until it becomes a worse Go.

We need a strategy that:
1. Lets a single SQL schema serve every chain version forever.
2. Keeps schema = stable contract (Hasura/PostgREST clients unaffected by chain wire shape).
3. Stays auditable per row (which decoder produced this row?).
4. Is automated end-to-end via two skills (`/generate-decoder` and `/generate-migration-from-diff`).
5. Survives the wire turbulence we observed in v0.1.27 without manual surgery.

## Decision

We adopt the following strategy, codified in `.claude/skills/generate-migration-from-diff/`:

### 1. Schema = logical, decoder = wire-to-logical adapter

The SQL schema represents the **logical** shape of an entity (e.g. `claimed_amount BIGINT, claimed_denom TEXT`), not the wire shape (`Coin` message vs `string` representation). When poktroll changes wire (e.g. v0.1.27 went from `Coin claimed_upokt` to `string claimed_upokt`), the **schema does NOT change**. The per-version decoder package reads its wire format and writes the same logical columns.

Downstream APIs (Hasura, PostgREST) see a stable contract regardless of chain version.

### 2. Nullable superset of fields, never destructive ALTER

- New fields → `ALTER TABLE ADD COLUMN IF NOT EXISTS <col> <type> NULL`.
- Removed fields → SQL comment, column stays forever (append-only, ADR-005).
- Type-changed fields → with the atomic decomposition rules below, this almost never produces a schema-visible change. If a genuine shape break occurs, the skill emits a `-- TYPE CHANGE FLAG` comment and continues; no destructive ALTER.
- No version-suffixed columns (`claimed_upokt_v27_amount`). The skill emitted them once during early prototyping; the noise outweighed any value. Provenance is captured by `decoded_by_version` per row.

### 3. `decoded_by_version` column + `decoder_version` table

Every entity row carries `decoded_by_version SMALLINT NOT NULL REFERENCES decoder_version(id)`. The `decoder_version` lookup table maps each onboarded decoder package to a stable integer id (`vX.Y.Z` → `X*10000 + Y*100 + Z`).

This enables:
- Debug: "why is this column NULL?" — check decoder version, verify whether that wire shape carried the field at all.
- Reconciler: filter rows by decoder version to isolate bugs to a specific decoder package.
- Auditable provenance: every row knows which Go code interpreted its bytes.

### 4. Atomic decomposition of complex types

The skill maps proto types to SQL columns by these rules:

| Proto type | SQL columns |
|---|---|
| `string` | `<name>` TEXT NULL |
| `bytes` | `<name>` BYTEA NULL |
| `uint64` / `int64` / `*64` | `<name>` BIGINT NULL |
| `uint32` / `int32` / `*32` | `<name>` INTEGER NULL |
| `bool` | `<name>` BOOLEAN NULL |
| `cosmos.base.v1beta1.Coin` | `<base>_amount` BIGINT + `<base>_denom` TEXT |
| `string` with denom-alias suffix (e.g. `claimed_upokt: string` post-v0.1.27) | same as Coin |
| `repeated <scalar>` | `<name> <type>[]` |
| `repeated <message>` | `<name>` JSONB |
| Other message (nested) | `<name>` JSONB |
| Enum | `<name>` SMALLINT |

Coin split convention: `<base>` strips known denom aliases (`upokt`, `pokt`) from field name. Exception: when base = `amount`, the numeric column is `amount` (not `amount_amount`) to avoid redundancy.

### 5. COMMENT ON every column, derived from proto docstring

The `/generate-decoder` extractor captures the `//` comment above each field. The migration skill emits `COMMENT ON COLUMN ...` for every column with a non-empty comment. When the proto docstring changes between versions, the skill emits a refresh `COMMENT ON COLUMN` for the affected columns. This keeps the Hasura/PostgREST auto-generated docs in sync with upstream improvements.

### 6. Three patterns per entity

- **stateful**: state entity with lifecycle. PK = `(id_fields..., block_height)`. `id_fields` is a list — supports composite keys (e.g. `Delegation` has `[delegator_address, validator_address]`).
- **singleton**: state entity without a key (`FeePool`, `Plan`). PK = `(block_height)`.
- **stateless**: append-only event/msg. PK = `(block_height, tx_index, event_index)`. Hypertable on `block_time`.

### 7. Auto-include patterns + explicit overrides

Explicit `entities:` map in `config.yaml` for state entities (we have to declare `id_fields`). Auto-include rules catch all events and messages by FQN regex:

```yaml
auto_include:
  - regex: '^pocket\.[\w.]+\.Event[A-Z]\w*$'
    pattern: stateless
    table_template: "{snake}"
  - regex: '^pocket\.[\w.]+\.Msg[A-Z]\w*$'
    pattern: stateless
    table_template: "{snake}"
    exclude_suffix: Response
  - regex: '^cosmos\.[\w.]+\.Event[A-Z]\w*$'
    pattern: stateless
    table_template: "cosmos_{module}_{ver}_{snake}"
  - regex: '^cosmos\.[\w.]+\.Msg[A-Z]\w*$'
    pattern: stateless
    table_template: "cosmos_{module}_{ver}_{snake}"
    exclude_suffix: Response
```

Table-name tokens: `{snake}` (snake_case of last name segment), `{module}` (second segment of FQN), `{ver}` (version segment for cosmos protos).

### 8. Idempotent migrations (`IF NOT EXISTS` everywhere)

Every generated DDL uses idempotent forms (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ADD COLUMN IF NOT EXISTS`, `INSERT ... ON CONFLICT DO NOTHING`). The skill can be re-run for any version without harm; goose can re-apply a migration; the operator can reorder for local testing.

### 9. Cosmos-sdk versioning via go.mod detection + dedup vendoring

For each poktroll release, `/generate-decoder` reads its `go.mod`, extracts the `github.com/cosmos/cosmos-sdk` version, and vendors that cosmos-sdk release into `third_party/proto/cosmos-sdk/<v>/` (skipping if already vendored). The shape snapshot records `cosmos_sdk_version` alongside `version`.

The extractor scans both namespaces (`pocket.*` from poktroll and `cosmos.*` + `tendermint.*` from cosmos-sdk). The diff and migration logic treat both uniformly — a single migration may cover both poktroll and cosmos-sdk shape changes when a poktroll release bumps its dependency.

Observed: across 33 poktroll releases, only 2 distinct cosmos-sdk versions were vendored (v0.50.13 for v0.1.0–v0.1.11, v0.53.0 for v0.1.12–v0.1.33). Dedup works in practice.

## Consequences

### Positive

- One stable SQL schema serves all chain versions, present and future. Hasura/PostgREST clients never break on chain changes.
- Reproducibility preserved: old rows decoded with old decoders stay correctly interpreted; no retroactive data migration ever.
- Schema doubles as documentation (COMMENT ON for every column, refreshed on proto update).
- Onboarding a new poktroll release is `/generate-decoder vX.Y.Z` + `/generate-migration-from-diff vX.Y.Z` + manual decoder write. The skills do the boilerplate; humans write only the wire-to-logical Go code.
- decoded_by_version per row gives an indelible audit trail.
- Cosmos-sdk dedup means we don't vendor the same cosmos-sdk version twice across 33 poktroll releases.

### Negative

- Schema column count grows monotonically over time. v0.1.27 + v0.1.33 alone added ~25 columns to `event_claim_settled`. Storage is cheap; query writers must select carefully.
- Per-row overhead: 1 BIGINT (decoded_by_version FK) + 1 TIMESTAMPTZ (indexed_at) on every row. Acceptable.
- The skills are Python (extract.py, generate.py); a small dependency for tooling. Not in the production binary path.
- For entities never wired to a Go consumer, tables stay empty forever. Schema-as-contract is valuable but storage is real (~30 idle hypertables for cosmos msgs we may never decode).

### Neutral

- The skill is non-magical: it produces SQL that the operator reviews before `goose up`. Auto-include patterns are explicit and inspectable.
- Adding a new entity to the explicit `entities:` map (e.g. when wireamos `Application`) is one config edit + re-run of the skill for the relevant versions.

## Alternatives considered

### Option A: Versioned YAML entity map (SubQuery-style)

Define each entity's shape per version range in YAML, runtime reads YAML to decide what to do.

- Pro: Single declarative source of truth.
- Con: YAML becomes a parser DSL. Every weird case adds a feature. When the runtime can't capture a corner case, you can't escape — the framework is the boundary.
- **Rejected because**: Exactly the SubQuery trap PocketScribe's whole existence is a reaction against (CLAUDE.md banned list, ADR-007).

### Option B: One table per chain version

`supplier_history_v0_1_0`, `supplier_history_v0_1_27`, etc.

- Pro: Each table has the exact shape of its version.
- Con: Anti-DRY. Queries explode. Aggregates cross-version impossible.
- **Rejected because**: makes append-only across versions meaningless; reconciler can't compare; Hasura/PostgREST exposes 33 endpoints per entity.

### Option C: Version-suffix columns for type changes

`claimed_upokt_v27_amount` and `claimed_upokt_amount` side-by-side.

- Pro: Explicit which version produced what data.
- Con: Schema clutter. Refresh-storm in migrations every wire change. Same provenance already captured by `decoded_by_version`.
- **Rejected because**: noise outweighs the value. Tried it in early iteration, hated it.

### Option D: All-JSONB-payload table per entity

One big `event_claim_settled_data JSONB` column, decoder writes a JSON blob.

- Pro: Schema never changes.
- Con: Loses native indexability, type fidelity, Hasura auto-introspection. Defeats the purpose of using a relational DB.
- **Rejected because**: contradicts ADR-002 (Postgres + Timescale choice motivated by relational queries).

## Implementation notes

The two skills implement this strategy end-to-end:

- `.claude/skills/generate-decoder/` — vendor + extract.
  - `run.sh` clones poktroll vX.Y.Z, parses go.mod for cosmos-sdk version, vendors cosmos-sdk (dedup), runs extractor.
  - `scripts/extract.py` parses .proto files (regex parser handling messages, fields, oneof, nested, comments), produces snapshot JSON.
  - `scripts/diff.py` compares vs previous snapshot (semver-sorted).
  - `scripts/update_evolution.py` appends curated rows to `docs/research/spine-shape-evolution.md`.

- `.claude/skills/generate-migration-from-diff/` — emit SQL.
  - `run.sh` thin wrapper.
  - `scripts/generate.py` reads snapshot, applies entity config + auto-include rules, emits idempotent goose migration.
  - `config.yaml` declares: explicit state entities with id_fields, auto-include regexes, denom aliases, coin_string_recognition.

Validation evidence:
- 33 snapshots produced (`docs/research/.shapes/v*.json`), each with ~1000 messages from both namespaces.
- 33 migrations produced (deleted after validation pending ADR approval).
- Genesis migration for v0.1.0 creates 168 tables covering full chain data model.
- v0.1.12 migration captures the cosmos-sdk v0.50.13 → v0.53.0 bump (11 new tables + 10 ALTERs).
- v0.1.27 migration captures the EventClaimSettled refactor (1 CREATE + 120 ALTERs + 72 COMMENT updates) without any destructive operation.

## References

- ADR-005 — append-only pure (no destructive ALTER).
- ADR-006 — chain as source of truth (no derived state, no retroactive migration).
- ADR-007 — per-module consumers in Go (rejected the declarative-everything trap).
- ADR-008 — one decoder package per chain version (Go code per version, not YAML).
- ADR-016 — pgx v5 + sqlc + goose (the toolchain).
- ADR-018 — upgrades table chain-driven (not generated by this skill).
- `.claude/skills/generate-decoder/SKILL.md`
- `.claude/skills/generate-migration-from-diff/SKILL.md`
- `docs/research/spine-shape-evolution.md` — human-readable curated evolution log.
- `docs/research/.shapes/v*.json` — full snapshots, archaeological record.
