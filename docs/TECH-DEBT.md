# Technical debt & deferred work registry

> Canonical registry of everything intentionally deferred out of completed slices.
> One line per item; when an item ships, delete its row (git history preserves it).
> Produced by the Slice 1 closure deep-review (2026-06-11); update on every slice close.

## Deferred from Slice 1 (spec promises not shipped)

| Item | Promised in | Target | Notes |
|---|---|---|---|
| `ps indexer` (all consumers in one process) | spec §12 CLI surface | Slice 2 | Only doc.go stub exists (`internal/app/indexer/`). |
| `ps inspect streams` / `ps inspect cursors` | spec §12 | Slice 2 | Only doc.go stub (`internal/app/inspect/`). |
| `ps doctor` (DB/NATS/node healthcheck) | spec §12 | Slice 2 | No package at all. |
| Gap escalation timers (`PS_GAP_WARN_AFTER` 5m / `PS_GAP_ERROR_AFTER` 30m) | spec §6 | Slice 2 or 4 | Deferred Phase B → "Phase G", never picked up; today only immediate WARN + `GapsTotal` metric (`runtime.go`, `batch.go`). |
| `ps fileplugin` live tail mode | spec §12 ("Phase 2 polish") | Phase 2 | Only `--bootstrap` implemented (`internal/app/fileplugin/cmd.go`). |
| Block consumer on BatchRuntime | residue of spec §14 item 5 | Slice 2 | ADR-024 valves shipped for supplier; block consumer still single-msg `Runtime`. |
| Golden fixtures v0.1.30 / v0.1.31 / v0.1.33 | spec §8 / Phase F note | When tarballs land in bucket | Archeology running on multi-1; `TODO(phase-f-pending)` in `golden_walk_test.go`; skill `curate-version-fixtures`. Live-era v0_1_30 decoder has no real-data golden until then. |

## Deferred to later slices (open questions, spec §14 + ADRs)

| Item | Source | Target |
|---|---|---|
| `aggregate_registry.consumers_needed` enforcement at materialization | spec §14 | Slice 2 |
| Materialized `block_seal` vs derived `is_sealed(H)` (perf call) | spec §14 | Slice 2 |
| `bucket_seal.sealed_by_consumers` population | spec §14 | Slice 2 |
| `processed_heights` at scale (hypertable/partitioning, ~635k rows/consumer) | spec §14 | Phase 2 (revisit if Slice 2 sealing queries hurt) |
| Dead-letter after N redelivery attempts | spec §10 | Slice 4 |
| Active self-heal via chain RPC injection | spec §14 | Slice 4 |
| Reconciler full feature set (entity drift detection) | spec §14, ADR-018 | Slice 4 |
| E2E with real poktroll node (Layer 5) | spec §11 | Slice 4 / Phase 2 |
| NATS stream sizing & retention for production | spec §14 | Slice 4 / Phase 3 |
| Backpressure tuning for lagging consumers | spec §14 | Slice 4 |
| `ps indexed-height-publisher` + envelope count cross-check enforcement | ADR-025 | unassigned — assign during Slice 2 planning |
| NATS retention offset + backup strategy | ADR-019 | unassigned |
| Table partitioning strategy | ADR-007 | Phase 2 |
| PNF interaction / poktroll issue #1481 follow-up | ADR-021 | unassigned |
| Archive upload pipeline (archeology → bucket automation) | ADR-026 (Proposed) | unassigned |
| `param_history` SCD2 writer (table exists, no writer; documented in-schema) | schema | future params module |
| `EventSupplierSlashed` / `MsgUpdateParam` consumers | Phase E decision 6 | future tokenomics/params modules |

## Known accepted debt (documented, revisit deliberately)

| Item | Where documented |
|---|---|
| Coverage gate excludes `internal/app/*` (composition roots; integration-covered wiring) | `scripts/covgate/main.go`, `internal/app/README.md` |
| jsonpb marshal test seams in decoders v0_1_0/v0_1_8/v0_1_27 (defensive guards unreachable with current types) | seam comments in each `supplier.go` |
| `evicted` map bounded-growth assumption (empties on restart) | ADR-024 amendment + `batch.go` comment |
| 10× `//nolint:staticcheck` SA1012 in `bootstrap_test.go` fakes (avoidable with `context.Background()`) | this registry only |
| Fixture-absent skip path in `supplier_consumer_test.go` is dead code that would mask losing the commutativity test | this registry only |
| Covgate exclusions not mirrored in `docs/architecture/10-testing.md` | this registry only |

When planning Slice 2: walk the first two tables and pull everything marked Slice 2.
