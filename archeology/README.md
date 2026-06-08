# PocketScribe — Chain Archeology

> Per-version FilePlugin snapshots of every poktroll mainnet upgrade, captured
> by running each released binary against a fresh chain in order. The output
> is the canonical input PocketScribe replays to reconstruct chain history
> without ever syncing the live network again.

## Why this exists

Poktroll's Shannon mainnet has a non-deterministic replay window
(`[94370, 102141]`, v0.1.15/v0.1.16 binaries — see [poktroll#1481][1481]).
A node started from `genesis.json` halts at h=96610 with `LastResultsHash`
mismatch. The historical blocks are still served by archive nodes (Sauron),
but **the chain cannot be re-derived from genesis by anyone, ever**.

PocketScribe's answer (codified by [ADR-021][ADR-021]):
**snapshot every upgrade boundary, replay the snapshots**.

The `archeology/` directory is the producer side: 32 binaries, one per
mainnet upgrade, each captured with our patched FilePlugin so the per-block
output is the canonical, reproducible artifact PocketScribe consumes.

[1481]: https://github.com/pokt-network/poktroll/issues/1481
[ADR-021]: ../docs/decisions/ADR-021-shannon-history-discontinuity.md

## What this produces

Two artifacts per version, uploaded to the Hetzner Object Storage bucket
`pocketscribe-mainnet-archeology`:

| Artifact | What it is | Why we keep it |
|---|---|---|
| `{version}-h{H}-datadir.tar.xz` | Full chain state at halt height | Boot point for the next binary; resume without re-sync |
| `{version}-h{H}-fileplugin.tar.xz` | All `block-N-data` + `block-N-meta` files emitted by FilePlugin | **The canonical input PocketScribe replays** |
| `{version}-pocketd-archeology.xz` | Compressed patched binary | Bit-exact reproducibility of the capture |
| `*.sha256` | Hash of each artifact | Integrity verification |

## Contents of this directory

```
archeology/
├── README.md                     # this file
├── FINDINGS.md                   # consolidated technical findings + lessons
├── VERSIONS.md                   # canonical version table (heights, paths, notes)
├── .env.example                  # template for the env vars the orchestrator needs
├── genesis.json                  # mainnet genesis (32 KB) for boot validation
│
├── scripts/                      # the orchestrator and helpers
│   ├── orchestrator.sh           # ▸ the canonical loop (tip-mode support included)
│   ├── lib.sh                    # shared helpers
│   ├── 00-preflight.sh           # env validation
│   ├── 10-bootstrap.sh           # initial chain bootstrap
│   ├── 20-fetch-binary.sh        # binary discovery
│   ├── 30-run-version.sh         # per-version run wrapper (used by lib.sh)
│   ├── 40-snapshot-version.sh    # tar + sha256 of datadir / fileplugin-output
│   ├── 50-upload-hetzner.sh      # rclone push + spot-check
│   ├── build-archeology.sh       # build patched binaries from poktroll source
│   ├── verify-genesis.sh         # confirm local genesis matches Sauron
│   ├── run-chapter.sh            # legacy chapter-based workflow (kept for ref)
│   ├── check-chapter-progress.sh
│   ├── merge-toml.py             # config merge helper
│   ├── discover-shim-candidates.sh
│   └── extract-canonical-overrides.sh
│
├── patches/
│   ├── 001-app-go-archeology.patch          # turns on FilePlugin in poktroll app.go (per-version)
│   └── morse_claimable_account_shim.go      # shim source for v0.1.15/v0.1.16 (broken-binary workaround)
│
├── binaries/                     # LFS — patched pocketd binaries, one per release
│   ├── v0.1.0/
│   │   ├── pocketd                # patched archive build
│   │   └── pocketd.sha256
│   └── ... (32 versions: v0.1.0, v0.1.2..v0.1.33)
│
├── configs/
│   ├── app.toml.template          # FilePlugin-enabled app.toml base
│   └── config.toml.template       # network/seeds template
│
└── samples/                       # representative fileplugin output for reference
    ├── block-190974-data
    ├── block-190974-meta
    ├── block-190975-data
    └── block-190975-meta
```

## How a contributor uses this

To replay history for a fresh PocketScribe install:

1. **Download** the desired range of `*-fileplugin.tar.xz` from the Hetzner
   bucket (public read once we open it).
2. **Untar** into a single directory.
3. Point PocketScribe's `ps fileplugin` sidecar at that directory.
4. Consumers indistinguishable from live-mode replay the entire chain.

To add a new poktroll version (when one is released):

1. `cd archeology/`, edit `versions.yaml` to add the new tag + `runs_until`.
2. Run `scripts/build-archeology.sh <new-tag>` to build the patched binary.
3. Run `scripts/orchestrator.sh` — idempotent, picks up the new version.
4. New `datadir.tar.xz` and `fileplugin.tar.xz` land in the bucket.

## Running the orchestrator

Required env vars (see [.env.example](./.env.example)):
- `BINARIES_DIR`, `NODE_HOME`, `FILEPLUGIN_OUTPUT` — local paths
- `SAURON_RPC`, `SAURON_LCD` — chain-tip detection in tip mode
- `HETZNER_*`, `RCLONE_REMOTE` — bucket upload

Standard invocation:

```bash
cp .env.example .env  # fill in HETZNER_* secrets
tmux new-session -d -s orch -c $PWD "bash scripts/orchestrator.sh; sleep 86400"
```

The orchestrator is **idempotent**: re-running skips versions already in
the bucket and resumes partials. Stalls trigger kill+retry; FATAL exit
after `$MAX_RETRIES` consecutive stalls per version.

## Status as of last run

See [VERSIONS.md](./VERSIONS.md) for the per-version status table.

## See also

- [`docs/decisions/ADR-021-shannon-history-discontinuity.md`](../docs/decisions/ADR-021-shannon-history-discontinuity.md) — the foundational decision
- [`docs/research/poktroll-sync-from-genesis.md`](../docs/research/poktroll-sync-from-genesis.md) — the original investigation
- [`docs/research/poktroll-versions.md`](../docs/research/poktroll-versions.md) — chain upgrade plans
