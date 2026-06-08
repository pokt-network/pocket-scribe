# ADR-027: FilePlugin output integrity validation

**Status**: Accepted
**Date**: 2026-05-27
**Authors**: jorge.s.cuesta
**Supersedes**: —

## Context

FilePlugin output (per-block `block-{H}-meta` + `block-{H}-data` files) is the
canonical KV-state archive that downstream consumers (PocketScribe indexers,
ad-hoc tooling, future ecosystem) treat as source of truth alongside the chain.

Failure modes observed during the genesis-sync archeology (ADR-021 chapter 4):

1. **Process crash mid-block**: node panics between `ListenFinalizeBlock` and
   `ListenCommit` → orphan meta with 2 protos and no data file.
2. **OOM-throttled compression**: snapshot script collided with a memory-limited
   `xz`, produced a truncated `.tar.xz` that passed file-size but failed
   `xz -t` later (chapter 4, v0.1.25 incident).
3. **Restore-before-sync**: stale `fileplugin-output/` from a previous run
   contained blocks not produced by the current run; without filtering, those
   were tarred and shipped as if they belonged.
4. **Bit-flip in transit**: cross-DC upload corrupts a byte; sha256 sidecar
   catches the tar but not whether the protos inside are parseable.
5. **Format drift across SDK / proto-version boundaries**: hypothetical, but
   nothing currently asserts the file format is what consumers expect.

Up to this ADR, the only integrity check applied was a sha256 sidecar on the
`.tar.xz`. That proves the tar wasn't tampered with end-to-end; it does **not**
prove the bytes inside parse as the expected protos, that heights are
contiguous, or that the block count matches the version's `runs_until` range.

## Decision

Validate FilePlugin output at **three checkpoints**, fail-closed at each.

### 1. Producer-side, pre-snapshot

Immediately after the node exits successfully for a version and before
`40-snapshot-version.sh` runs, the orchestrator MUST run a validator against
the local `fileplugin-output/` directory.

The validator MUST assert, for every block in the directory:

- `block-{H}-meta` contains exactly 3 varint-length-prefixed proto messages,
  which decode in order as `abci.RequestFinalizeBlock`,
  `abci.ResponseFinalizeBlock`, `abci.ResponseCommit`.
- `meta.RequestFinalizeBlock.Height == H` (filename matches encoded height).
- `block-{H}-data` exists and every varint-framed payload decodes as
  `storetypes.StoreKVPair`.
- No trailing bytes in either file.

And across the directory:

- Heights are contiguous from `previous_version.runs_until + 1` to
  `current_version.runs_until`.
- File count equals `2 * (max_height − min_height + 1)`.

On failure: orchestrator aborts the version with FATAL. The bucket is not
touched. A human investigates.

### 2. Consumer-side, post-upload spot-check

After upload completes for a version, the orchestrator MUST re-download a
random sample of N blocks (default `N=5`, configurable; always includes
first and last block of the range) and re-run the same validator against
them. This guards against in-flight corruption that the tar's outer sha256
already protects against, and additionally exercises the bucket-→-disk path
that downstream consumers will use.

On failure: orchestrator marks the upload as **SUSPECT** in the orchestrator
log, aborts before advancing to the next version. The version remains
flagged until manually re-snapped and re-uploaded.

### 3. Downstream-consumer-side, on-ingest

Any consumer that reads from the FilePlugin archive (PocketScribe indexers,
backfill tools, ecosystem) MUST validate per-block before consuming the
payload. The validator binary is shipped as a reusable Go library
(`pkg/fileplugin/validate`) and a CLI (`ps validate-fileplugin <dir>`).

Consumers that bypass validation are operating outside the supported usage
pattern.

## Consequences

### Positive

- The bucket cannot contain unparseable data. Once a version's artifacts are
  in `mainnet/v0.1.X/` they are guaranteed parseable and structurally
  complete.
- Producer failures (crash mid-block, memory-throttled xz, stale leftover
  files) become loud at the point they occur, not silent until weeks later
  when an indexer hits the corrupt file.
- Consumers have a single, canonical validator they can call before
  ingesting any block — no per-consumer drift in interpretation.
- The validator itself is testable in isolation against fixture files, so
  format expectations are pinned in code.

### Negative

- Adds ~3 minutes of CPU per 100K blocks at producer-side. Acceptable: the
  full archeology run for chapter 4 is on the order of days; validation is
  <1% of that budget.
- Spot-check downloads 5 small files (≪ 1 MB total) per version from the
  bucket. Negligible egress cost intra-Hetzner.
- The validator binary becomes a build-time dependency of the orchestrator
  pipeline. It must be kept in lockstep with the proto contract (it
  imports `abci` and `storetypes` types).

### Neutral

- Validator failure modes are reportable as structured output (file path,
  block height, expected vs actual) — feeds into future observability if
  the indexer team wants metrics on validation outcomes.

## Alternatives considered

### Option A: Trust the sha256 sidecar only

- Pro: zero additional code, zero CPU overhead.
- Con: sha256 only proves the tar is byte-identical to what the producer
  hashed. It cannot detect: empty/truncated proto messages, height/filename
  mismatches, gaps in the block range, or any structural problem that the
  producer never noticed.
- **Rejected because**: the OOM-throttled-xz incident in chapter 4 v0.1.25
  produced a file that passed `xz -t` initially but failed validation later.
  Sha256 alone would have shipped that to the bucket.

### Option B: Validate only at consumer side (lazy)

- Pro: producer pipeline stays simple.
- Con: every consumer pays the validation cost on every ingest, and a bad
  version sits in the bucket indefinitely until *some* consumer hits it.
  No clear ownership of "is this version healthy?".
- **Rejected because**: integrity should be enforced by the writer at write
  time, not delegated to every reader. Matches invariant #3 (chain is
  source of truth — *for the chain*; for the archive, the writer is the
  source of truth and must self-verify).

### Option C: Full re-parse during snapshot (replace `xz` with parse-then-rewrite)

- Pro: catches everything in one pass.
- Con: rewriting the archive removes byte-for-byte determinism with what
  the node wrote — defeats the purpose of having a canonical archive.
- **Rejected because**: bit-identical preservation of node output is a
  hard property we want to keep.

## Implementation notes

- Validator lives at `pkg/fileplugin/validate` (library) and
  `cmd/ps-validate-fileplugin` (binary). Initial implementation at
  `experiments/sync-from-genesis/cmd/validate-fileplugin/` will be lifted
  into `pkg/` once the API stabilises.
- Orchestrator integration: see `experiments/sync-from-genesis/scripts/orchestrator.sh`
  v3 — validation hook between `run_version` and `snap_and_upload`,
  spot-check hook after `snap_and_upload`.
- The validator decodes only the meta protos (3 per block) and verifies
  framing on the data file. It does NOT decode every `StoreKVPair` — that
  would be O(N) in chain TX volume and unnecessary for integrity. Framing
  + parseability of the first/last KV is sufficient signal; consumers
  decode the full payload during their own ingest.

## References

- ADR-003: FilePlugin and sidecar
- ADR-026: Sidecar optional archive upload (where the bucket layout is
  defined)
- ADR-021: Shannon history discontinuity (which made archeology necessary
  in the first place)
- HANDOFF for chapter 4, lessons #2–#5 (validate-on-write motivation)
