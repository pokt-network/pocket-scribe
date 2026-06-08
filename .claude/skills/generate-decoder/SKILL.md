---
name: generate-decoder
description: Onboard a poktroll chain version. Vendors protos at the given tag, extracts a structural snapshot of every poktroll-owned message, and computes the diff against the previous vendored version. Produces docs/research/.shapes/<vX_Y_Z>.json (full snapshot) and appends to docs/research/spine-shape-evolution.md (curated human view). Idempotent — re-running for an already-vendored version refreshes both outputs. NO codegen, NO migration generation (those are separate skills).
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# /generate-decoder

Onboard ONE poktroll chain version into PocketScribe's shape catalog.

This skill is the canonical entry point for "a new poktroll version exists, capture its shape". It vendors the protos, snapshots every message, and explains what changed since the previous version. It does NOT generate Go code (that needs the `Decoder` interface stable, deferred until Paso 3) and does NOT write schema migrations (that's `/generate-migration-from-diff`).

## Inputs

1. **Version tag** — exact git tag on `pokt-network/poktroll`, e.g. `v0.1.0`, `v0.1.34`.

That's the only input. Everything else is inferred from the project state.

## Steps

### 1. Validate input + setup

- Confirm the tag matches the regex `^v\d+\.\d+\.\d+$`.
- Compute `VERSION_DIR = $(echo $TAG | tr '.' '_')` (e.g. `v0.1.0` → `v0_1_0`).
- Ensure `third_party/proto/poktroll/`, `docs/research/.shapes/`, `docs/research/` exist.

### 2. Vendor protos

```bash
TMPDIR=/tmp/poktroll-clone-$TAG
DEST=third_party/proto/poktroll/$VERSION_DIR
rm -rf $TMPDIR $DEST
mkdir -p $DEST
git clone --quiet --depth=1 --branch $TAG https://github.com/pokt-network/poktroll.git $TMPDIR
cp -r $TMPDIR/proto/* $DEST/
rm -rf $TMPDIR
```

If clone fails, abort. Report tag + error.

### 3. Extract shape snapshot

Run `scripts/proto-shape/extract.py $DEST > docs/research/.shapes/${VERSION_DIR}.json`.

The script parses every `*.proto` under `$DEST/pocket/**` (poktroll-owned, not cosmos/gogo deps) and produces a JSON document of this shape:

```json
{
  "version": "v0.1.0",
  "vendored_at": "<ISO timestamp passed via env>",
  "messages": {
    "pocket.shared.Supplier": {
      "file": "pocket/shared/supplier.proto",
      "fields": [
        {"name": "owner_address", "type": "string", "tag": 1, "repeated": false},
        {"name": "operator_address", "type": "string", "tag": 2, "repeated": false}
      ]
    }
  }
}
```

Messages are keyed by fully-qualified name (`<package>.<MessageName>`). Field order = proto declaration order.

### 4. Compute diff vs previous

Run `scripts/proto-shape/diff.py docs/research/.shapes/${VERSION_DIR}.json` which:
- Finds the immediately-previous snapshot by semver-sorting existing `docs/research/.shapes/*.json`.
- If no previous → emits `{previous_version: null, ...}`.
- Otherwise → emits structured diff:

```json
{
  "version": "v0.1.5",
  "previous_version": "v0.1.4",
  "added_messages": ["pocket.tokenomics.NewMessage"],
  "removed_messages": [],
  "changed_messages": {
    "pocket.shared.Supplier": {
      "added_fields": [{"name": "rev_share", "type": "...", "tag": 7}],
      "removed_fields": [],
      "type_changed_fields": []
    }
  },
  "unchanged_messages_count": 142
}
```

The diff is printed to stdout. It is NOT persisted — it is recomputed on demand from snapshots, so any two versions can be diff'd later.

### 5. Append to evolution doc (curated)

Read `.claude/skills/generate-decoder/tracked-entities.txt` (one fully-qualified message name per line; the curated subset we care about).

For each tracked message in the current version, append a one-line entry to `docs/research/spine-shape-evolution.md` describing what changed vs previous. Always append, even if no change ("unchanged" is a valid explicit record).

### 6. Report to stdout

```
✅ v0.1.5 vendored.
   Messages: 145 total (+1, -0, ~3 changed vs v0.1.4).
   Tracked entities changed: pocket.shared.Supplier (+rev_share), pocket.tokenomics.EventClaimSettled (unchanged).
   Snapshot: docs/research/.shapes/v0_1_5.json
   Evolution doc: docs/research/spine-shape-evolution.md (3 lines appended)
```

## Idempotency

Re-running for an already-vendored version overwrites the vendored protos, snapshot JSON, and the corresponding line(s) in the evolution doc. No state carries over from prior runs except what's on disk.

## Failure modes

| Failure | Recovery |
|---|---|
| Clone fails (tag doesn't exist, network) | Abort, report. Operator retries with correct tag. |
| `proto/` dir missing in cloned repo | Abort. Some early tags may not have it — flag for operator. |
| Extract script can't parse a `.proto` file | Print warning, skip that file, continue. List skipped files at end. |
| No previous version exists | Skip diff step, emit "genesis snapshot" message. |

## Out of scope (explicit non-goals)

- Codegen of Go types — `internal/decoders/<v>/gen/` not touched. Deferred until ADR-028 + Paso 3.
- Schema migrations — handled by `/generate-migration-from-diff`.
- Registering in `upgrades` table — that comes from `ps sync-upgrades` (ADR-018, chain-driven).
- CI matrix update — separate concern.

## References

- ADR-008 — versioned decoders (the why).
- ADR-018 — no hardcoded upgrades (the constraint).
- `docs/research/spine-shape-evolution.md` — the human-readable output.
- `scripts/proto-shape/` — implementation of extract + diff.
