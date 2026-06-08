# Findings — what we learned doing this

> Consolidated technical findings from the archeology run. Distilled from
> live notes (`HANDOFF*.md`, `RESUME.md`, `results.md`, `migration/LOG.md`)
> kept during the run.

## The headline problem

A fresh poktroll node started from `genesis.json` halts at **block height
96610** with `LastResultsHash` mismatch ([poktroll#1481][1481]). The bad
window is `[94370, 102141]` — covering binaries v0.1.15 and v0.1.16. The
fix landed in v0.1.17 but historical blocks **cannot be re-derived from
genesis by any node, ever**.

The consequence: any indexer that wants per-block fidelity from height 1
must consume **pre-captured per-version FilePlugin output** — there's no
way to produce it from scratch today.

[1481]: https://github.com/pokt-network/poktroll/issues/1481

## What the archeology run produces

For each of 32 mainnet upgrades (v0.1.0 … v0.1.33, skipping v0.1.1 which
was never released):

1. **Datadir tarball** — the chain state at the moment we halt the binary
   at its `runs_until` height.
2. **FilePlugin tarball** — every `block-N-data` + `block-N-meta` file the
   binary emitted while running.
3. **Patched binary** — bit-exact pocketd used for the capture
   (in `binaries/` via Git LFS).
4. **sha256 sums** — for integrity verification.

All four go to the Hetzner Object Storage bucket
`pocketscribe-mainnet-archeology` under `mainnet/{version}/`.

## Key findings

### 1. Why we needed patched binaries

The stock poktroll release binaries don't emit per-block FilePlugin output
by default (it's gated by `app.toml` config). But even with the config on,
two real bugs surfaced:

- **`streaming_file.go` write race** — under sustained load (block-by-block
  catch-up syncing), the file writer would sporadically truncate output.
  Otto's fix landed in the official codebase (now in v0.1.34); for v0.1.0
  through v0.1.33 we **needed our patched build per version**.
- **`MorseClaimableAccount` shim for v0.1.15 / v0.1.16** — the Morse
  migration code in those releases hits the non-deterministic replay
  window. To get past h=99293 we shim'd a minimal stub of
  `morse_claimable_account.go` (see `patches/morse_claimable_account_shim.go`).

### 2. cosmos-sdk dependency bumps mid-history

Across 33 releases, cosmos-sdk was bumped exactly once:

- **v0.1.0 → v0.1.11**: cosmos-sdk **v0.50.13** (+ cometbft v0.38.12)
- **v0.1.12 → v0.1.33**: cosmos-sdk **v0.53.0** (+ cometbft v0.38.17 / .19)

This shows up cleanly in our binary sizes:
v0.1.0–v0.1.11 ≈ 144 MB, v0.1.12+ ≈ 166 MB.

### 3. Per-version snapshot sizes

The datadir grows roughly linearly with height. Quick sample
(see [VERSIONS.md](./VERSIONS.md) for the full table):

| Version | Block range | Datadir | FilePlugin | Notes |
|---|---|---|---|---|
| v0.1.0 | 1 .. 78620 | 270 MB | 14 files | Genesis binary, very small payload |
| v0.1.12 | 78697 .. 80509 | 281 MB | 3626 files | Cosmos-sdk bump |
| v0.1.17 | 102142 .. 116099 | 616 MB | 27916 files | First post-discontinuity stable binary |
| v0.1.19 | 117454 .. 135296 | 1.3 GB | 35686 files | First "live mainnet activity" version |
| v0.1.21 | 138931 .. 155172 | 5.4 GB | 32484 files | Activity exploded |
| v0.1.22 | 155173 .. 161108 | 6.8 GB | 11872 files | |
| v0.1.24 | 161169 .. 190973 | 20 GB | 2.5 GB FilePlugin | |
| v0.1.26 | 190979 .. 247892 | 57 GB | (huge) | The big one |
| v0.1.27 | 247893 .. 287931 | (mid) | The major shape refactor (8 fields added, 3 removed in EventClaimSettled) |
| v0.1.33 | tip | (open) | Current live binary |

### 4. Stall behaviour during catch-up sync

While syncing forward, pocketd periodically stalls (no new blocks for >180s)
before recovering. This is **NOT** block-determinism failure (the chain
*does* progress overall). It seems to be peer-discovery / state-sync churn
when the binary catches up — once it lands on the right peers it proceeds.

The orchestrator handles stalls with `kill+retry` cycles (default 15, bumped
to 60 for v0.1.30 which exhibits more frequent stalls). Each retry resumes
from the on-disk datadir; nothing is lost.

### 5. Chain-tip detection requires external RPC

There's no marker in the local chain that says "you've caught up to
mainnet tip". Solution implemented in `orchestrator.sh`'s tip mode:

- Poll local height via `127.0.0.1:26657/status`
- Poll mainnet tip via `https://sauron-rpc.infra.pocket.network/status`
- "Caught up" = local within `TIP_PROXIMITY` (3) blocks of tip
- Hold for `TIP_STABLE_SEC` (300) seconds → SIGINT → snapshot

The Sauron RPC is the canonical PocketScribe endpoint
(see [configs/networks/mainnet.yaml](../configs/networks/mainnet.yaml)).
`*.poktroll.com` / `shannon-grove-*` are previous-team infra and are
explicitly NOT canonical for this project.

### 6. Idempotency turned out to be the most important property

Every level is idempotent:

- **Bucket check** — skip versions whose datadir is already uploaded.
- **Local datadir** — partial states are preserved across orchestrator
  restarts (the `cleanup_partial_fileplugin` function refuses to delete
  when local height > 0).
- **Migration** — uploads use `rclone --checksum`; partial uploads
  resume cleanly.

This let us kill the orchestrator at any point, rotate operators, and
relaunch without losing progress. The two FATAL aborts we hit (v0.1.27
in early run, v0.1.30 in late run) both recovered cleanly with a
restart + retry bump.

## Operational lessons

### Disk planning

- Datadir grows to ~50 GB on the active binary even though tarballs are
  smaller post-compression. **Provision at least 1 TB free** on the
  orchestrator host before any backfill run. Pure SSD (NVMe RAID1
  preferred) — IO is the bottleneck.

### SSH from WSL2

- `ssh.exe` (Windows OpenSSH + 1Password agent) works; `/usr/bin/ssh`
  doesn't have the key. First connection of a session prompts 1Password
  for approval.
- Avoid `-o BatchMode=yes -o ConnectTimeout=5` — they block the prompt.

### Logs grow huge

- Single-version run logs reached 125 MB (the `run-v0.1.0.log`).
- `start_node` filters pocketd stdout to keep only `committed state`,
  `ERR `, `panic`, `UPGRADE NEEDED`, `fatal` — without this filter the
  filesystem can fill up.

### Validate before snapshot

- ADR-027 added a **pre-snapshot validation** step (`validate-fileplugin`)
  that fast-fails if the per-block files are inconsistent (gaps, partial
  writes, bad magic bytes).
- After upload, a **spot-check** re-downloads a slice and compares
  sha256 — caught one network-corruption event during this run.

### The bucket is the source of truth

Snapshots are the canonical reproduction substrate. Once a snapshot is
in the bucket, **never delete it without a chain-of-custody process**.
Even local datadirs can be wiped — they're regenerable from the bucket.

## Open items

- **swap-watcher** — the v2→v3 swap automation never fired in production
  (single log line, "awaiting v0.1.29 upload"). Investigated post-run; the
  watcher script terminated silently for unknown reasons. Was workable
  manually so deprioritised. The script has been **removed** from the
  cleaned-up repo since the current orchestrator handles all versions in
  one process; the v3-swap pattern is obsolete.
- **v0.1.30 frequent stalls** — even with MAX_RETRIES=60 each retry only
  advances ~300 blocks. Root cause unknown; possibly FilePlugin
  back-pressure under high write volume. v0.1.30 captures ~150K blocks of
  high activity so this is the worst-case version in the run.

## See also

- [VERSIONS.md](./VERSIONS.md) — per-version table + bucket paths
- [`docs/decisions/ADR-021-shannon-history-discontinuity.md`](../docs/decisions/ADR-021-shannon-history-discontinuity.md)
- [`docs/decisions/ADR-027-fileplugin-output-validation.md`](../docs/decisions/ADR-027-fileplugin-output-validation.md)
