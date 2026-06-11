# ADR-023: Live ingest vs bootstrap boundary — indexer never reaches out of NATS

**Status**: Accepted (implemented in Slice 1; status updated 2026-06-11)
**Date**: 2026-05-23
**Authors**: Jorge Cuesta, Claude

## Context

PocketScribe has two distinct ingest paths:

- **Live ingest**: a running poktroll node produces FilePlugin output → `ps fileplugin` sidecar publishes to NATS → consumers materialize Postgres.
- **Bootstrap**: a fresh deployment needs the chain from genesis (or from height N) without re-syncing the chain locally. The canonical FilePlugin archive (published per version to object storage, see `experiments/sync-from-genesis/`) is the source.

A tempting shortcut for live ingest is to put references back to the archive in NATS payloads ("the body is at `s3://.../block-H-data`, go fetch it"). This was rejected for the following reasons:

1. **Coupling**: the indexer's hot path would depend on S3/Hetzner availability and credentials. A bucket region outage stalls indexing.
2. **Scope creep**: PocketScribe is the indexer producer. Bucket distribution is a different concern (operations / packaging), and the indexer should not be a client of it.
3. **Failure semantics drift**: with a reference, "the message arrived in NATS" no longer means "the data is available to consumers". Retries, idempotency, and dedup all become harder to reason about.
4. **Restart symmetry**: bootstrap and live should run the same consumer code. Mixing in a fetch-from-bucket fallback at runtime makes the consumer code conditional on which path produced the message.

## Decision

**The indexer's live path consumes only what flows through NATS.** No NATS payload carries a reference to external storage (S3, HTTP, object stores outside JetStream). Bootstrap and live diverge at the producer end only.

### Live path

```
poktroll node ─→ FilePlugin output dir ─→ ps fileplugin (publish) ─→ NATS ─→ consumers ─→ Postgres
```

### Bootstrap path

```
canonical archive (Hetzner) ─→ download + verify ─→ local FilePlugin replica dir ─→ ps fileplugin (publish, --bootstrap) ─→ NATS ─→ consumers ─→ Postgres
```

The sidecar gains a `--bootstrap` flag that:

- Reads from a configured local directory of FilePlugin output (`--input-dir`).
- Publishes at maximum rate (no live-tail wait between blocks).
- Refuses to publish blocks beyond a configured `--max-height` (so a bootstrap from height 0 to N can be cleanly handed off to a live sidecar starting at N+1).
- Otherwise behaves identically: same subjects, same payloads, same `Nats-Msg-Id` derivation. Idempotency means a hand-off block processed by both is a no-op.

Consumers don't know which produced the message. Their cursor advancement, batching, and Postgres writes are identical.

### Out of scope for the indexer

- Downloading the canonical archive (separate operator tool / playbook).
- Verifying archive integrity (separate tool consuming `*.sha256` sidecars from the bucket).
- Re-emitting from on-chain replay against a live node (would require keeping a poktroll node hot; not a bootstrap path PocketScribe owns).

## Consequences

### Positive

- Live indexer has zero dependencies on object storage. Its only externals are NATS and Postgres.
- Bootstrap is reproducible: anyone with the archive bundle + a NATS + Postgres can replay deterministically.
- Same consumer code paths everywhere → fewer integration test combinations.
- Failure isolation: bucket outage doesn't affect a running indexer.

### Negative

- Operator must manually orchestrate bootstrap: download archive, point a sidecar at it, wait for completion, swap to live sidecar. (Documented in operations runbook; not automated yet.)
- The sidecar binary grows a second mode. Bounded complexity; same publishing logic, different reader.

## References

- [ADR-003](ADR-003-fileplugin-and-sidecar.md) — sidecar architecture
- [ADR-019](ADR-019-partial-history-from-height-x.md) — partial history
- [ADR-022](ADR-022-nats-payload-discipline.md) — NATS payload discipline
- `experiments/sync-from-genesis/` — current bootstrap artifact generation
