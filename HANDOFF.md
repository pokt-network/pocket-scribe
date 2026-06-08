# Handoff — 2026-06-06

> Snapshot at end of session. Use [RESUME.md](./RESUME.md) to retake.

## What got done today

- **Schema generation pipeline** end-to-end: 4 skills (`generate-decoder`,
  `generate-migration-from-diff`, `verify-migrations`, `add-decoder-version`),
  244 tables validated against TimescaleDB via `make verify-migrations`.
- **Archeology cleanup**: distilled `experiments/sync-from-genesis/` (168 GB)
  into a clean `archeology/` directory with 32 patched binaries (Git LFS),
  consolidated docs (README + FINDINGS + VERSIONS), and the tip-mode patched
  orchestrator.
- **Repo hygiene**: removed all empty scaffolding (`internal/*`, `cmd/`,
  `test/`, `deploy/`), Makefile rewritten to only tested targets,
  `Tiltfile` stubbed, `STATUS.md` rewritten to reflect reality.
- **High-level docs**: `docs/architecture/00-system-flow.md` with 4 mermaid
  diagrams (live ingestion, schema generation, decoder routing, archeology
  pipeline).
- **Public repo created**: pushed to `git@github.com:pokt-network/pocket-scribe.git`
  as a single clean `Initial commit` (150fcd1, 32 LFS files, 5.3 GB).

## ⚠️ Operational status — orchestrator FATAL (UNREVIEWED)

The poktroll archeology orchestrator running on `pnf@65.108.199.125` has
**died for the third time today** at v0.1.30:

| Run | Started | Died at | Final height | Reason |
|---|---|---|---|---|
| Run 1 | yesterday | 03:51 | h=595475 | 15 retries exhausted |
| Run 2 | this morning | (restart with `MAX_RETRIES=60`) | h=599435 | **60 retries exhausted** |
| Status | dead | 07:23 today | — | bucket has v0.1.28 + v0.1.29 only |

The progression is real but **glacial** (~60 blocks per retry cycle, ~4 min
per cycle). Bumping `MAX_RETRIES` does not solve the root cause — each retry
makes negligible progress. We need to investigate **why** the binary stalls
at this rate.

**Hypotheses to test next session** (in priority order):

1. **FilePlugin back-pressure** — the per-block file writer might be
   blocking when output disk IO can't keep up. Check
   `iostat -x 5` while running; look for `fileplugin-output/` IO bottleneck.
2. **Peer discovery flaky** — the binary may be cycling through bad peers.
   Inspect `run-v0.1.30.log` for "peer dropped", "no peers", etc.
3. **Memory pressure** — `pocketd` could be hitting OOM-soft and getting
   rate-limited. Check `dmesg | grep -i "memory\|oom"` on the host.
4. **Bug in patched v0.1.30 binary** — check if running stock v0.1.30 with
   FilePlugin disabled progresses faster. If yes → our patch is the
   culprit.

**Workaround** if root cause is intractable: capture v0.1.30 via a different
strategy:
- (a) Snapshot the current partial datadir at h=599435 + upload as
  `v0.1.30-partial-h599435`. Document the gap.
- (b) Skip v0.1.30 entirely, snapshot directly from v0.1.31 (it picks up at
  h=635506 with a fresh datadir).
- (c) Use a stock binary + manual FilePlugin replay from Sauron archive
  RPC.

## Repo state

```
main (150fcd1) ← Initial commit, public on github.com/pokt-network/pocket-scribe

Local working tree: clean (matches HEAD).
LFS: 32 objects (5.3 GB) pushed.
```

What's in:
- 4 skills (validated)
- 38 schema migrations (244 tables, applied OK on TimescaleDB)
- 22 ADRs + 10 architecture docs + 4 mermaid diagrams
- archeology/ (32 binaries via LFS, scripts, patches, samples, docs)
- third_party/proto/ (poktroll 32 versions + cosmos-sdk 2 versions)
- Tooling configs: Makefile, buf.yaml, sqlc.yaml, go.mod

What's NOT in (intentional, comes in Phase 1):
- `cmd/`, `internal/*` — no Go runtime code
- `Tiltfile` is a stub
- No tests yet

## Roadmap pointer

Phase 1 (next): Spike a working end-to-end pipeline with ONE module
(supplier) + ONE aggregate (claims_hourly) + Hasura/PostgREST/WS expose,
on top of the schema we already have.

See `ROADMAP.md` for the full breakdown. Pending tasks tracked in this
session (some completed, others moved to Phase 1):

- [pending] Paso 1 — Schema foundation + ADR-028 ✅ (done, marked pending in task list)
- [pending] Paso 2 — sqlc + types base
- [pending] Paso 3 — Decoder runtime + router
- [pending] Paso 4 — Consumer runtime genérico
- [pending] Paso 5 — Primer aggregate + sealing
- [pending] Paso 6 — CLI ps + composición
