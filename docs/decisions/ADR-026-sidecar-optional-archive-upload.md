# ADR-026: Sidecar optional archive upload (operator-driven, async)

**Status**: Proposed
**Date**: 2026-05-23
**Authors**: Jorge Cuesta, Claude

## Context

The current bootstrap story ([ADR-023](ADR-023-live-vs-bootstrap-boundary.md)) requires a canonical FilePlugin archive packaged out-of-band (today, manually via `experiments/sync-from-genesis/scripts/*`). This works fine for the project team producing the initial archive but doesn't scale to:

- An operator running PocketScribe in production who wants their own off-site backup of FilePlugin output.
- Community-driven archive replication (anyone with a sidecar can contribute a canonical bundle).
- Operators who want to enable a fresh bootstrap from a recent height (not from genesis) without depending on the project's archive cadence.

The sidecar already sees every FilePlugin file as it lands; the natural extension is an OPTIONAL goroutine that bundles + uploads to a configured object store.

Risks if naively added:

- **Coupling to NATS publish path**: a slow / failing S3 must not stall consensus indexing. The archiver MUST be failure-isolated.
- **Scope creep**: the sidecar grows responsibilities. Bounded if archive is clearly an OPTIONAL, opt-in feature, not a default.
- **Bucket churn**: per-block PUTs are expensive (LIST/HEAD/PUT costs scale with object count). MUST chunk and compress.
- **Coordination with `keep_local`**: deleting local files after upload risks data loss if the archiver was wrong about success. MUST be opt-in and gated by sha256 verification.

## Decision

The sidecar gains an OPTIONAL, async, failure-isolated archive uploader. Disabled by default. When enabled, runs as a separate goroutine that does NOT block the NATS publish path.

### Config (TOML)

```toml
[sidecar.s3-archive]
enabled            = true
bucket             = "my-pocketscribe-mainnet"
prefix             = "mainnet/"            # object key prefix
chunk_blocks       = 1000                  # tar.xz every N blocks
trigger            = "blocks"              # "blocks" | "daily" | "size"
size_threshold_mb  = 100                   # only if trigger="size"
keep_local         = false                 # delete local files after verified upload
endpoint           = ""                    # S3-compatible (Hetzner, MinIO, Backblaze); empty = AWS
region             = "us-east-1"
access_key_env     = "S3_ACCESS_KEY"       # name of env var holding credentials
secret_key_env     = "S3_SECRET_KEY"
manifest_object    = "mainnet/MANIFEST.md" # central manifest object updated on each chunk
```

When `enabled = false` (default), the goroutine is not started — zero cost.

### Behavior

1. Sidecar maintains a watermark `last_archived_height` persisted to a small `archiver_state` table in Postgres (or a flat file when Postgres isn't shared with the indexer — TBD).
2. The archiver goroutine watches the FilePlugin output directory. When the trigger fires (N blocks accumulated / daily timer / cumulative size exceeded), it:
   - Picks the next `chunk_blocks` worth of fully-written FilePlugin files (gap-free, contiguous).
   - Verifies the range against `last_archived_height + 1 .. last_archived_height + chunk_blocks`.
   - Tars + xz-compresses (matches existing experiment format).
   - Computes sha256 of the tarball.
   - Uploads `{prefix}/chunks/blocks-{H_lo}-{H_hi}.tar.xz` + sidecar `.sha256`.
   - Re-downloads HEAD + verifies remote size; downloads sha256 and verifies match.
   - Updates `archiver_state.last_archived_height = H_hi` in the sidecar's local store.
   - If `keep_local = true`: nothing else.
   - If `keep_local = false`: deletes the local files for `[H_lo, H_hi]`.
   - Appends a line to the central `MANIFEST.md` object (read, append, PUT — eventual-consistency tolerant).
3. On failure at ANY step: log, increment failure metric, sleep `archiver_retry_backoff_ms` (exponential cap 5 min), retry the SAME chunk. Never advance the watermark without a verified upload.
4. SIGTERM handling: drain in-flight uploads cleanly (don't half-leave a chunk uploaded but not recorded). On hard SIGKILL: the watermark is the source of truth on restart, so the chunk will be retried (idempotent — same sha256 = remote object already there, just re-record).

### Why not push from a separate process

Was considered. Rejected because:
- The sidecar already knows when a FilePlugin file is fully written (it reads the `.tmp → final rename` signal proposed for future sidecar work). A separate watcher would re-implement the same logic.
- Failure isolation between archiver and NATS publish is achievable inside the same process via a separate goroutine + cancel context.
- Operationally simpler: one binary, one log stream, one Prometheus exporter.

### Why mandatory verification (sha256 round-trip)

S3 PUT does not guarantee object integrity by default. A truncated upload can return 200 OK. We verify by downloading the sha256 sidecar object and comparing. Cost: one tiny GET per chunk. Worth it given the chunk = ~10 MB compressed represents thousands of blocks of canonical state.

### Failure modes (and what the indexer does)

| Failure | Indexer impact | Archiver behavior |
|---|---|---|
| S3 unreachable | None — NATS publish path keeps going | Retry with exponential backoff |
| Bucket credentials expired | None | Loud error metric; alert via Prometheus |
| sha256 mismatch on round-trip | None | Refuse to advance watermark; retry |
| Local disk full (when `keep_local=true`) | EVENTUALLY blocks NATS publish if FilePlugin writer fails. Operator concern. | Same |
| Chunk gap detected | None | Refuse to upload (gap means missing FilePlugin file — bigger problem); alert |

## Consequences

### Positive

- Operators can self-host backups without touching upstream tooling.
- Community-replicable archives — if 3 operators enable this against different buckets, you have natural redundancy.
- Local disk doesn't grow unboundedly when `keep_local=false`.
- Same code path as our current experiment scripts will eventually replace `experiments/sync-from-genesis/scripts/40-snapshot-version.sh` + `50-upload-hetzner.sh` for ongoing per-version cuts.

### Negative

- Sidecar binary grows. ~500 LOC for the archiver + tests.
- One more config block for operators to understand. Mitigated by being opt-out by default (`enabled=false`).
- Sha256 verification round-trip adds ~1 GET per chunk; negligible.

## Open questions

- Object naming convention beyond `chunks/blocks-{H_lo}-{H_hi}.tar.xz`: do we want per-version subdirs (matching `experiments/.../mainnet/v0.1.X/`)? Provisional: no — live operators don't track poktroll version boundaries, just heights.
- Manifest format: markdown like the experiment, or JSON for machine consumption? Provisional: dual — generate both.
- Should the archiver also upload the meta-only files separately (`events/` style) for indexer consumers that want to do partial bootstrap by event-type filter without downloading state? Defer; this is ADR-022 territory at the consumer level, not the archiver level.

## References

- [ADR-003](ADR-003-fileplugin-and-sidecar.md) — sidecar architecture
- [ADR-019](ADR-019-partial-history-from-height-x.md) — partial history bootstrap
- [ADR-022](ADR-022-nats-payload-discipline.md) — NATS payload discipline
- [ADR-023](ADR-023-live-vs-bootstrap-boundary.md) — live vs bootstrap boundary
- `experiments/sync-from-genesis/scripts/40-snapshot-version.sh` — current manual snapshot script (proto-archiver)
- `experiments/sync-from-genesis/scripts/50-upload-hetzner.sh` — current manual upload script
