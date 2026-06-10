---
name: curate-version-fixtures
description: Curate FilePlugin fixtures for a poktroll version from the Hetzner archeology bucket — download, scan, select per spec §8.1, generate expected.json, enroll in the golden walker. Use when a new version's tarball lands in the bucket (v0.1.30 / v0.1.31 / v0.1.33 pending as of 2026-06-10) or when refreshing an era's coverage.
allowed-tools: Read, Write, Edit, Bash
---

# Curate version fixtures

## When
- A new poktroll version's `<ver>-h<H>-fileplugin.tar.xz` appears in
  `pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/<ver>/`
  (check: `rclone lsf pocketscribe-hetzner:pocketscribe-mainnet-archeology/mainnet/`).
  As of 2026-06-10 the archeology run on multi-1 has not yet uploaded
  v0.1.30, v0.1.31, v0.1.33.

## Steps
1. `scripts/curate_fixtures.sh <ver>` — downloads (sha256-verified), extracts
   to /tmp/fixtures-<ver>/, prints the per-height activity index.
2. Select ~3 heights per spec §8.1 (docs/superpowers/specs/2026-06-08-slice-1-design.md:356):
   boundary block (applied_height — confirm against the upgrades table or
   internal/fixturereport/mainnet.go), max supplier activity, quiet block.
3. Era dir: the decoder version DecoderFor(H) resolves to — confirm with the
   stderr `decoder:` line in step 4. New break version → new era dir AND
   follow .claude/rules/decoders.md rules 9–10 FIRST (machine-derived closure
   diff; never assume stability).
4. Per height H:
   cp /tmp/fixtures-<ver>/block-<H>-{meta,data} test/fixtures/<era>/
   go run ./tools/fixtureextract <H> test/fixtures/<era> \
     > test/fixtures/<era>/block-<H>-expected.json
5. If the version is NOT yet in internal/fixturereport/mainnet.go: add it
   (Name dotted, AppliedAtHeight from `ps sync-upgrades` / the chain,
   DecoderVersion underscored) and extend
   internal/router/mainnet_boundaries_test.go with the new boundary row.
   Remember: consumer wakeup is restart-based — after `ps sync-upgrades`
   lands a version that wakes a dormant consumer, restart that consumer.
6. `go test ./internal/fixturereport/ -run TestGoldenWalk` — must pass.
7. Update test/fixtures/README.md matrix row (PENDING → covered).
8. Optionally add a full-stack row in test/integration/ (see
   supplier_consumer_test.go cases table) for new eras.
9. `rm -rf /tmp/fixtures-<ver>`; commit
   `test(fixtures): curate <ver> fixtures (spec §8.1)`.
