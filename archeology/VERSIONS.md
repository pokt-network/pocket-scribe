# Versions — canonical table

> Per-version status of the archeology run. Chain-authoritative upgrade
> heights from `cosmos/upgrade/v1beta1/applied_plan/{name}` via Sauron LCD,
> verified 2026-05-22 (refresh with `scripts/sauron-upgrades.sh`).

## Mainnet (`pocket`)

Genesis sha256:
`b4adc3614def79b63e777f90cf0a62cf43ea6a17715bdcd16c982ae8e44abee2`
(`pocket-network-genesis/master/shannon/mainnet/genesis.json`).

| Version | Applied @ | Runs until | Blocks | Bucket status | Shim | Notes |
|---|---:|---:|---:|---|---|---|
| v0.1.0 | 1 | 78620 | 78620 | ✅ uploaded | — | Genesis binary |
| v0.1.1 | — | — | — | ❌ never released | — | Tagged in repo but skipped |
| v0.1.2 | 78621 | 78631 | 11 | ✅ uploaded | — | |
| v0.1.3 | 78632 | 78640 | 9 | ✅ uploaded | — | |
| v0.1.4 | 78641 | 78653 | 13 | ✅ uploaded | — | |
| v0.1.5 | 78654 | 78658 | 5 | ✅ uploaded | — | |
| v0.1.6 | 78659 | 78664 | 6 | ✅ uploaded | — | |
| v0.1.7 | 78665 | 78670 | 6 | ✅ uploaded | — | |
| v0.1.8 | 78671 | 78677 | 7 | ✅ uploaded | — | |
| v0.1.9 | 78678 | 78682 | 5 | ✅ uploaded | — | |
| v0.1.10 | 78683 | 78688 | 6 | ✅ uploaded | — | |
| v0.1.11 | 78689 | 78696 | 8 | ✅ uploaded | — | Last cosmos-sdk v0.50 |
| v0.1.12 | 78697 | 80509 | 1813 | ✅ uploaded | — | First cosmos-sdk v0.53 |
| v0.1.13 | 80510 | 93824 | 13315 | ✅ uploaded | — | |
| v0.1.14 | 93825 | 94369 | 545 | ✅ uploaded | — | |
| v0.1.15 | 94370 | 99292 | 4923 | ✅ uploaded | ⚠️ MorseClaimableAccount stub | Inside the non-deterministic window |
| v0.1.16 | 99293 | 102141 | 2849 | ✅ uploaded | ⚠️ MorseClaimableAccount stub | Inside the non-deterministic window |
| v0.1.17 | 102142 | 116099 | 13958 | ✅ uploaded | — | **First post-discontinuity stable binary** (fix lands) |
| v0.1.18 | 116100 | 117453 | 1354 | ✅ uploaded | — | |
| v0.1.19 | 117454 | 135296 | 17843 | ✅ uploaded | — | First real mainnet activity |
| v0.1.20 | 135297 | 138930 | 3634 | ✅ uploaded | — | |
| v0.1.21 | 138931 | 155172 | 16242 | ✅ uploaded | — | Datadir grew to 5.4 GB |
| v0.1.22 | 155173 | 161108 | 5936 | ✅ uploaded | — | |
| v0.1.23 | 161109 | 161168 | 60 | ✅ uploaded | — | |
| v0.1.24 | 161169 | 190973 | 29805 | ✅ uploaded | — | FilePlugin output: 2.5 GB |
| v0.1.25 | 190974 | 190978 | 5 | ✅ uploaded | — | |
| v0.1.26 | 190979 | 247892 | 56914 | ✅ uploaded | — | Datadir: 57 GB |
| v0.1.27 | 247893 | 287931 | 40039 | ✅ uploaded | — | **Major EventClaimSettled refactor**; height differs from repo's proposed 247939 by −46 |
| v0.1.28 | 287932 | 382249 | 94318 | ✅ uploaded | — | |
| v0.1.29 | 382250 | 484472 | 102223 | ✅ uploaded | — | |
| v0.1.30 | 484473 | 635505 | 151033 | 🔄 in progress | — | Frequent stalls; needs MAX_RETRIES=60 |
| v0.1.31 | 635506 | 703869 | 68364 | ⏳ pending | — | |
| v0.1.32 | — | — | — | ❌ never applied | — | Tagged in repo but skipped on mainnet |
| v0.1.33 | 703870 | tip | open | ⏳ pending | — | Current live binary; runs until caught-up to mainnet tip |
| v0.1.34 | — | — | — | ❌ not live yet | — | Released but not yet hit on mainnet. **Has Otto's FilePlugin fix natively** — when it goes live, future capture switches to the official binary, no patch. |

## Versions per cosmos-sdk dependency

| cosmos-sdk | Poktroll versions |
|---|---|
| **v0.50.13** | v0.1.0 .. v0.1.11 |
| **v0.53.0** | v0.1.12 .. v0.1.33 |
| **v0.53.7** | v0.1.34+ (when live) |

## Bucket layout

```
pocketscribe-mainnet-archeology/mainnet/
├── v0.1.0/
│   ├── v0.1.0-h78620-datadir.tar.xz + .sha256
│   ├── v0.1.0-h78620-fileplugin.tar.xz + .sha256
│   └── v0.1.0-pocketd-archeology.xz + .sha256
├── v0.1.2/
└── ...
```

Replace `mainnet/` with `mainnet-staging/` for non-canonical runs.
