All verification complete. The mechanical .shapes recomputation, the proto-source INPUT, and my direct proto-file spot checks all agree. Here is the report.

---

# Supplier Shape Map — Synthesis Verification Report

Sources cross-checked: (1) INPUT proto-source diffs, (2) mechanical recomputation from `/home/overlordyorch/development/pocketscribe/docs/research/.shapes/*.json`, (3) `/home/overlordyorch/development/pocketscribe/docs/research/spine-shape-evolution.md`, (4) direct reads of `/home/overlordyorch/development/pocketscribe/third_party/proto/poktroll/v*/pocket/**.proto` for every disagreement. Note: there is no vendored `v0_1_1`; `v0_1_0 -> v0_1_2` is the first consecutive pair.

## 1. Definitive supplier-closure break table

Only 5 of 32 pairs touch the closure. All other 27 pairs: closure identical (byte-identical or comment-only).

| Break version | Type | Change kinds | Wire-breaking? |
|---|---|---|---|
| **v0_1_2** | `pocket.migration.MsgClaimMorseSupplier` | field_removed (`morse_src_address=3` -> `reserved 3`), field_added (`shannon_signing_address=6`, `morse_public_key=7`), signer option change | **YES** (removal+reserved: old decoder on new bytes loses Morse identity) |
| **v0_1_8** | `pocket.shared.ServiceConfigUpdate` | field_type_changed x2 (TAG REUSE: 1 `repeated SupplierServiceConfig`->`string operator_address`; 2 `uint64`->`SupplierServiceConfig`, wire-type 0->2), field_added (3, 4) | **YES** (hard unmarshal error / silent garbling both directions) |
| v0_1_8 (transitive) | `pocket.shared.Supplier` (field 6 embeds ServiceConfigUpdate) and everything embedding Supplier: all 5 supplier events, MsgStake/UnstakeSupplierResponse, EventMorseSupplierClaimed, MsgClaimMorseSupplierResponse | other (transitive wire break) | **YES** |
| **v0_1_12** | `pocket.migration.MsgClaimMorseSupplier` | field_renumbered (tag 3 UN-RESERVED, reused as `morse_node_address`), field_added (8) | **YES** (tag reuse with changed semantics) |
| v0_1_12 | `pocket.migration.MsgClaimMorseSupplierResponse` | field_removed (`morse_src_address=1` -> reserved), field_added (7, 8, 9 incl. new enum `MorseSupplierClaimSignerType`) | **YES** |
| v0_1_12 | `pocket.migration.EventMorseSupplierClaimed` | field_removed (`morse_src_address=3` -> reserved), field_added (6, 7, 8) | **YES** |
| v0_1_13 | `pocket.supplier.SupplierUnbondingReason` (enum) | value added `MIGRATION=3` | no (numeric preserved; name/String() rendering degrades on older decoders) |
| **v0_1_27** | `pocket.supplier.EventSupplierStaked` | field_removed (`supplier=1` -> reserved), field_added (`operator_address=3`) | **YES** (old decoder yields event with no supplier identity) |
| v0_1_27 | `pocket.supplier.EventSupplierServiceConfigActivated` | field_removed (`supplier=1`), field_added (`operator_address=3`, `service_id=4`) | **YES** |
| v0_1_27 | `pocket.tokenomics.EventSupplierSlashed` | field_removed (`claim=1`, `proof_missing_penalty=2` Coin), field_added (scalars 3–8; penalty changes type Coin->string AND number 2->3) | **YES** (old decoder yields fully empty event) |
| v0_1_27 | `pocket.supplier.MsgStakeSupplierResponse`, `MsgUnstakeSupplierResponse`, `MsgUpdateParamResponse` | field_removed (gutted to empty, reserved 1) | **YES** |
| v0_1_27 | `pocket.migration.EventMorseSupplierClaimed` | field_renumbered + type change (`claimed_balance` Coin@2 -> string@9; `claimed_supplier_stake` Coin@4 -> string@10) | **YES** |
| v0_1_27 | `pocket.migration.MsgClaimMorseSupplierResponse` | field_removed (ALL fields; reserved 1–5,7–9) | **YES** |
| v0_1_27 | `pocket.shared.RPCType` (enum) | value added `COMET_BFT=5` | no |

Stable across the entire range v0_1_0..v0_1_33: `pocket.shared.Supplier` (own field list), `SupplierServiceConfig`, `SupplierEndpoint`, `ServiceRevenueShare`, `ConfigOption`, `pocket.proof.Claim`, `pocket.session.SessionHeader`, `pocket.supplier.{Params, MsgStakeSupplier, MsgUnstakeSupplier, MsgUpdateParam, MsgUpdateParams, MsgUpdateParamsResponse}`, the unbonding events' own field lists, `cosmos.base.v1beta1.Coin`.

## 2. Cross-check between methods — disagreements and resolutions

The two break maps **agree on every break version**: mechanical .shapes recomputation over the transitive closure flags exactly {v0_1_2, v0_1_8, v0_1_12, v0_1_27} — identical to the proto-source INPUT. Four disagreements/limitations found, all investigated against the actual .proto files:

1. **`spine-shape-evolution.md` says `pocket.shared.Supplier` "unchanged" at v0.1.8** while the proto source declares a Supplier wire break. Resolution: I diffed `third_party/proto/poktroll/v0_1_7/pocket/shared/supplier.proto` vs `v0_1_8/...` — Supplier's own field list is indeed textually unchanged, but `ServiceConfigUpdate` (embedded at Supplier field 6) has tag-1 and tag-2 reuse with wire-type changes. **Both are "right" at their own granularity; the proto-source verdict governs decoding**: Supplier bytes ARE cross-version incompatible at v0_1_8. The curated doc is shallow (per-message, non-transitive) and tracks only 2 entities (`Supplier`, `EventClaimSettled`) — it must never be used as a break map. The full `.shapes` snapshots DO capture the ServiceConfigUpdate change; only the curated table hides it.

2. **.shapes is blind to enum changes** (snapshots contain a `messages` map only; no enum entries exist — verified: `SupplierUnbondingReason`, `RPCType`, `ClaimProofStatus`, `MorseSupplierClaimSignerType` are absent from all 33 snapshots). The INPUT reports `SUPPLIER_UNBONDING_REASON_MIGRATION=3` at v0_1_13 and `COMET_BFT=5` at v0_1_27. Resolution: verified both in proto source (diff of `v0_1_12/pocket/supplier/event.proto` vs `v0_1_13`, and `v0_1_26/pocket/shared/service.proto` vs `v0_1_27`) — **proto source is right, .shapes misses them**. Acceptable for the CI guard because enum value additions are never wire-breaking, but the guard cannot claim enum coverage.

3. **.shapes does not record `reserved` ranges**, so the v0_1_12 tag-3 reuse in `MsgClaimMorseSupplier` appears as a plain field addition in a consecutive-pair diff, hiding that tag 3 was live in v0_1_0 as `morse_src_address` with different semantics. Resolution: verified in proto — `v0_1_11` has `reserved 3;` inside MsgClaimMorseSupplier, `v0_1_12` redefines `string morse_node_address = 3`. **Proto source is right on severity** (breaking reuse, not additive). The mechanical guard still flags v0_1_12 as a break version (any field-list change), so the registry assertion is unaffected — only the breaking/non-breaking *classification* differs.

4. **`spine-shape-evolution.md` v0.1.27 row mentions only `EventClaimSettled` changes** and misses all nine supplier-closure types broken at that boundary. Resolution: artifact of the 2-entity tracking config (`.claude/skills/generate-decoder/tracked-entities.txt`), not a data error; the full snapshots contain everything. The curated doc's tracked-entity list should be widened to the supplier closure (recommendation, not done here — read-only).

Additionally confirmed: the .shapes recompute found **no break the INPUT missed** (Claim, SessionHeader, Coin, all shared types stable in every pair).

## 3. Shape ranges per decode category

A "range" = consecutive versions whose bytes one generated type decodes correctly.

**(a) Tx msgs — `MsgStakeSupplier` / `MsgUnstakeSupplier` (requests, as found in tx body bytes):**
- `[v0_1_0 .. v0_1_33]` — one single range. Neither message nor its transitive types (SupplierServiceConfig -> SupplierEndpoint/ServiceRevenueShare/ConfigOption) ever change.
- Caveat: the **responses** (`MsgStakeSupplierResponse`/`MsgUnstakeSupplierResponse`, embedded in tx-result `TxMsgData`) embed `shared.Supplier`, so they break twice: `[v0_1_0..v0_1_7][v0_1_8..v0_1_26][v0_1_27..v0_1_33]` (gutted to empty at v0_1_27). If tx results are decoded, category (a) inherits these boundaries.

**(b) Supplier events (5 supplier-module events + tokenomics `EventSupplierSlashed`):**
- `[v0_1_0 .. v0_1_7]` `[v0_1_8 .. v0_1_26]` `[v0_1_27 .. v0_1_33]`
- v0_1_8 boundary: all events embedding `Supplier` break transitively via ServiceConfigUpdate.
- v0_1_27 boundary: EventSupplierStaked / EventSupplierServiceConfigActivated / EventSupplierSlashed restructured (unbonding events retain `supplier=1` and don't break themselves, but the category's generated type set must change).
- Soft note (not a wire boundary): from v0_1_13, `reason=3 (MIGRATION)` appears in unbonding events; a pre-v0_1_13 generated enum renders it as a bare numeric. If the consumer maps reason via `String()`, treat v0_1_13 as a *semantic* boundary.

**(c) KV value — `pocket.shared.Supplier` transitive:**
- `[v0_1_0 .. v0_1_7]` `[v0_1_8 .. v0_1_33]`
- Single break at v0_1_8 (ServiceConfigUpdate in `service_config_history`). Nothing else in the Supplier transitive closure changes through v0_1_33.

## 4. Minimal registry set under the lenient router

Union of range-start versions across all three categories: `{v0_1_0, v0_1_8, v0_1_27}`.

**Minimal registry set = `{v0_1_0, v0_1_8, v0_1_27}`.** Extra registrations are harmless iff each extra version is shape-identical to its range (true for all currently registered versions).

**Current `router.DefaultRegistry()`** (`/home/overlordyorch/development/pocketscribe/internal/router/registry.go`) = `{v0_1_0, v0_1_10, v0_1_20, v0_1_28, v0_1_29, v0_1_30}` and `/home/overlordyorch/development/pocketscribe/internal/decoders/` has no `v0_1_8` or `v0_1_27` package. **Two fallback holes exist today:**
- Blocks at chain versions v0.1.8–v0.1.9 fall back to the `v0_1_0` decoder -> hard unmarshal errors / garbling on Supplier KV writes and every supplier event (ServiceConfigUpdate tag reuse).
- Blocks at chain version v0.1.27 fall back to the `v0_1_20` decoder -> EventSupplierStaked/ServiceConfigActivated/EventSupplierSlashed decode to effectively empty events (silent data loss, no error).

Required action (for the planner): vendor/generate and register `v0_1_8` and `v0_1_27` decoders. (If migration claim decoding is ever in scope, add `v0_1_2` and `v0_1_12` — see section 6.)

## 5. CI shape-guard design (verified algorithm — prototype run produced exactly the section-1 break set)

**File:** `internal/router/shapeguard_test.go` (unit layer; pure JSON parsing, no containers; runs on every commit).

**Inputs read from each `docs/research/.shapes/v0_1_N.json`:**
- top-level `version` (sanity-check against filename)
- `messages` — map of fully-qualified type name -> `{file, source, fields[]}`
- per field: read EXACTLY `tag` (int), `name` (string), `type` (string), `repeated` (bool, absent = false). **Ignore `comment` and `file`** (comment churn must not trip the guard).

**Algorithm (exact, reproduced verbatim-ready):**
1. Glob `docs/research/.shapes/v0_1_*.json`; sort numerically by patch (`strconv.Atoi` of suffix — NOT lexicographic, v0_1_2 must sort before v0_1_10).
2. Seed set (const slice in the test):
   `pocket.supplier.MsgStakeSupplier`, `pocket.supplier.MsgUnstakeSupplier`, `pocket.supplier.MsgStakeSupplierResponse`, `pocket.supplier.MsgUnstakeSupplierResponse`, `pocket.supplier.EventSupplierStaked`, `pocket.supplier.EventSupplierUnbondingBegin`, `pocket.supplier.EventSupplierUnbondingEnd`, `pocket.supplier.EventSupplierUnbondingCanceled`, `pocket.supplier.EventSupplierServiceConfigActivated`, `pocket.tokenomics.EventSupplierSlashed`, `pocket.shared.Supplier`.
   (Append the three `pocket.migration` claim types when that scope lands — see section 6.)
3. Per snapshot, compute the transitive closure: BFS from seeds; for each field, resolve `type` to a message key by trying, in order: (i) exact match in `messages`; (ii) `<package-of-containing-message> + "." + type` (package = container name minus last segment); (iii) `"pocket." + type`. Unresolvable types (scalars, enums, unvendored) are skipped. This resolution order is required because snapshots store types as written (`SupplierServiceConfig`, `shared.Supplier`, `pocket.shared.Supplier` all occur).
4. Normalize each closure message to a canonical form: the set of `(tag, name, type, repeated)` tuples sorted by tag.
5. For each consecutive pair (A, B): compare every type in `closure(A) UNION closure(B)`. Type **B is a break version** iff any type's normalized form differs OR a type exists in one snapshot's `messages` and not the other. (Union handles closure-membership churn, e.g. `pocket.proof.Claim` leaving the closure at v0_1_27 without a false positive.)
6. **Assertion:** `breakVersions ∪ {oldest snapshot version}` ⊆ keys of `router.DefaultRegistry()`. Failure message must name the version and the changed types.

**Comparison rule, stated precisely:** "any difference in the tag-sorted `(tag, name, type, repeated)` tuple set of any transitively-reachable closure message between consecutive snapshots marks the later version as a break version" — deliberately stricter than wire-breaking, because even additive fields are silently dropped under earlier-decoder fallback (data loss for the indexer).

**Expected output with the core+responses seed set today:** breaks = `{v0_1_8, v0_1_27}`; with baseline `v0_1_0` the required registry subset is `{v0_1_0, v0_1_8, v0_1_27}` — i.e., **this test FAILS against the current registry until v0_1_8 and v0_1_27 decoders are registered**, which is the correct forcing function. A future `v0_1_34` snapshot with any supplier-closure change fails CI until registered.

**Documented blind spots (assert nothing, note in test comment):** enum values (.shapes has no enums; enum additions are non-wire-breaking), `reserved` ranges (tag-reuse severity invisible, but the field-change itself is still caught), gogoproto options (`casttype`/`nullable` — Go-type only, not wire).

## 6. Param-scope and migration-type answers

**`MsgUpdateParam` / `MsgUpdateParams` / `pocket.supplier.Params`: NO change anywhere in v0_1_0..v0_1_33.** Verified mechanically across all 33 snapshots: `Params` = `{min_stake=1 Coin, staking_fee=2 Coin}`, `MsgUpdateParam` = `{authority=1, name=2, as_coin=3 Coin}`, `MsgUpdateParams` = `{authority=1, params=2}` — frozen for the entire range. The KV handler can discriminate supplier params writes with ONE generated type for all versions. Sole exception: `MsgUpdateParamResponse` was gutted (`params=1` -> `reserved 1`) at v0_1_27 — relevant only if tx results are decoded. (Note: `pocket.shared.MsgUpdateParamResponse` got the same treatment at v0_1_27; `pocket.shared.Params` itself unchanged.)

**`pocket.migration.MsgClaimMorseSupplier` / `EventMorseSupplierClaimed`: YES, they break — at versions the supplier-module map does not cover.** If these are ever decoded (and they should be — `EventMorseSupplierClaimed` creates suppliers, carrying `shared.Supplier supplier = 5` throughout):
- `MsgClaimMorseSupplier` ranges: `[v0_1_0]` `[v0_1_2..v0_1_11]` `[v0_1_12..v0_1_33]` — both boundaries wire-breaking (tag-3 removal+reserve at v0_1_2; tag-3 un-reserve/reuse at v0_1_12). Byte-identical from v0_1_12 through v0_1_33.
- `EventMorseSupplierClaimed` ranges: `[v0_1_0..v0_1_11]` `[v0_1_12..v0_1_26]` `[v0_1_27..v0_1_33]` — wire-breaking at both (morse_src_address removed at v0_1_12; Coin->string renumber of both amounts at v0_1_27). It also inherits the v0_1_8 Supplier transitive break via field 5, so its true safe ranges are `[v0_1_0..v0_1_7][v0_1_8..v0_1_11][v0_1_12..v0_1_26][v0_1_27..v0_1_33]`.
- Registry consequence: adding migration decode scope adds **`v0_1_2` and `v0_1_12`** to the minimal registry set, i.e. `{v0_1_0, v0_1_2, v0_1_8, v0_1_12, v0_1_27}`. The CI guard picks this up automatically the moment the three migration types are appended to its seed list (mechanically verified: prototype with migration seeds yields breaks `{v0_1_2, v0_1_8, v0_1_12, v0_1_27}`).
- Practical note: Morse claims occur only during the migration window, and the supplier row itself also arrives via the KV snapshot path; if the consumer relies on KV writes rather than the event payload, only the event's *attribution* fields (morse addresses, amounts, signer type) are at risk — but those are exactly the fields that moved at v0_1_12/v0_1_27, so per-range decoders are mandatory if they are persisted.