# Slice 1 — Phase G Implementation Plan: Hardening + reconciler refresh

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement spec §9 Phase G: ADR-024 partial-flush valves (triggers 2-3) + orphaned heightBuf eviction, reconciler hardening (metrics + immediate first sync + tests), the deferred edge-case tests (large-block caps through the pipeline, intra-tx fault injection, upgrade-boundary-through-pipeline, restart corners, `migrate down`), combined unit+integration coverage measurement with a hard gate (≥90% `internal/`, 100% decoders), a hardened `make ci` (race, integration-tag lint, coverage gate), a GitHub Actions workflow, and per-component READMEs — closing the Slice 1 exit criterion (spec §15).

**Architecture:** The valves require `block_time` before the envelope arrives, so the sidecar stamps every fan-out message with a `Pocket-Block-Time` NATS header (unix nanos from the block header it already decoded) — ADR-022/ADR-024 amendments first, code second. `BatchRuntime` gains a size valve (synchronous, on append), a time valve + eviction sweep (background goroutine, mutex-protected buffer, injectable clock), and a new `store.FlushOnly` (tx without cursor advance). Eviction uses an in-memory evicted-heights set + envelope Nak so a late envelope can never seal a hole (see Task 5 rationale). Coverage is measured by merging unit and integration text profiles with two small in-repo Go scripts (no new deps).

**Tech Stack:** Go 1.26, pgx v5, NATS JetStream (`nats.go/jetstream`), testcontainers (existing harness), prometheus/client_golang, GitHub Actions.

---

## Context (read before starting)

| What | Where |
|---|---|
| Spec Phase G + exit criterion | `docs/superpowers/specs/2026-06-08-slice-1-design.md:494-504` (§9-G), `:643-655` (§15) |
| ADR-024 triggers 2-3 + ack discipline | `docs/decisions/ADR-024-consumer-batching.md` (Decision + Amendment) |
| ADR-022 fan-out / ordering contract | `docs/decisions/ADR-022-nats-payload-discipline.md` |
| ADR-018 reconciler behavior | `docs/decisions/ADR-018-no-hardcoded-upgrades.md:62-63` |
| BatchRuntime today (trigger 1 only) | `internal/consumer/batch.go` |
| Sidecar fan-out + size caps | `internal/fileplugin/bootstrap.go`, `internal/fileplugin/sizecap.go` |
| Reconciler cmd (exists, untested, no metrics) | `internal/app/reconciler/cmd.go` |
| Consumer rules | `.claude/rules/consumers.md` |

**Decisions locked with the user (2026-06-10):**
1. Coverage bar measured on COMBINED unit+integration profiles; hard gate in `make ci`. Bar: ≥90% per package under `internal/` (excluding generated code and zero-statement packages), 100% on `internal/decoders/*` (excluding `gen/`).
2. Large-block cap behavior verified at INTEGRATION level (testcontainers sidecar→NATS→consumer), not real-node E2E (Layer 5 stays deferred to Slice 4).
3. GitHub Actions workflow IS in scope: lint + unit(race) + integration + coverage gate on PR.
4. READMEs for ~9 major components; trivial packages get `doc.go` only.
5. `block_time` for partial flushes: sidecar stamps `Pocket-Block-Time` header on every fan-out message (ADR-022 amendment).
6. Eviction semantics: drop after `batch_evict_after` (default 10× `batch_max_age` = 50s) WITHOUT ack — NATS redelivers; WARN + metric.

**Hard rules (handoff):** no AI footer in commits; no push (user pushes); TDD; lint must ALSO pass with `--build-tags=integration`; gofmt before every commit; decoders 100% coverage, `internal/` ≥90% with REAL error paths (no happy-path padding); version-based never network-based; HANDOFF/SESSION-LOG stay local.

**Numbering note:** new integration tests in this plan are Phase G hardening tests, NOT spec §11.1 numbered scenarios (those end at 27). Comment them `// phase G: <short purpose>`.

---

## Part 1 — ADR amendments (docs before code)

### Task 0: Branch

- [ ] **Step 1: Create the phase branch from main**

```bash
cd /home/overlordyorch/development/pocketscribe
git checkout main && git checkout -b slice-1/phase-g
```

Expected: branch `slice-1/phase-g` created from `d8e052c` (or current main HEAD).

---

### Task 1: Amend ADR-022 and ADR-024

**Files:**
- Modify: `docs/decisions/ADR-022-nats-payload-discipline.md` (append Amendment section)
- Modify: `docs/decisions/ADR-024-consumer-batching.md` (append Amendment section)

- [ ] **Step 1: Append to ADR-022**

```markdown
## Amendment (Phase G, 2026-06-10): Pocket-Block-Time header

Every fan-out message (per-tx, per-event, per-KV) AND the envelope carries the
NATS header `Pocket-Block-Time` = the consensus block time as unix nanoseconds,
taken from the block header the sidecar already decodes for the envelope.

Rationale: ADR-024 triggers 2-3 (partial flush) write rows BEFORE the envelope
arrives, but Invariant 1 requires every row to carry `(block_height, block_time)`
from the consensus header. The height is in the subject; the time must therefore
travel on each fan-out message. ~30 bytes per message; well under the caps.

The header is informative on the envelope (its payload already carries
`time_unix_nano`); it is REQUIRED on fan-out messages. Consumers treat a fan-out
message without the header as un-partial-flushable (valves skip it with a WARN);
the block-boundary fence path is unaffected.
```

- [ ] **Step 2: Append to ADR-024**

```markdown
## Amendment (Phase G, 2026-06-10): valves implemented + eviction semantics

Triggers 2 (size cap) and 3 (time cap) are implemented in
`internal/consumer/batch.go`. Partial flushes run through `store.FlushOnly`
(BEGIN → handler write → COMMIT; NO cursor advance, NO processed_heights row)
and pass a nil envelope to `BatchHandler.FlushHeight` — handlers derive
`types.Position` from `Message.TimeUnixNano` (the `Pocket-Block-Time` header,
ADR-022 amendment) when the envelope is nil. Flushed messages stay UNACKED and
their Nats-Msg-Ids stay in the dedup set; the fence acks everything after the
final commit, exactly as before.

Orphaned-buffer eviction (new): a height buffer whose envelope has not arrived
within `batch_evict_after` (default 10× `batch_max_age` = 50 s) is dropped from
memory WITHOUT acking — metric `pocketscribe_consumer_evictions_total`, WARN
log. NATS redelivers the unacked messages on AckWait expiry and the buffer
reconstructs. Redelivery timing is NOT ordered relative to a Nak'd envelope
(AckWait timers are per-message), so the runtime cannot assume the rebuilt
buffer is complete when a late envelope arrives. Instead it records the number
of distinct Nats-Msg-Ids seen at eviction time (`evicted[height] = len(seen)`,
which includes partially-flushed messages — their ids stay in the dedup set).
The fence for an evicted height is Nak'd (mark KEPT) until the rebuilt
buffer's seen-count reaches the recorded count; only then does the flush
proceed and the mark clear. A late envelope can therefore never seal a hole
left by an eviction, regardless of redelivery interleaving. If a rebuilding
buffer is evicted again, the recorded count is `max(previous, len(seen))`.
Process restart clears the mark set — safe, because on re-subscribe JetStream
redelivers ALL outstanding unacked messages in stream-sequence order (fan-out
before envelope), the same crash-recovery model tests 3/12 already verify; the
mark only exists to cover the steady-state case where fan-out AckWait timers
have not yet expired when the Nak'd envelope returns.

Knobs (BatchConfig, defaults per this ADR): `MaxRows` 5000, `MaxAge` 5 s,
`EvictAfter` 50 s. Metric `pocketscribe_consumer_partial_flushes_total
{consumer,reason}` with reason ∈ {size,time}. The evicted-heights map grows
only with heights whose envelope NEVER arrives (chronic sidecar failure) and
empties on restart; bounded-growth assumption documented in code.
```

- [ ] **Step 3: Commit**

```bash
git add docs/decisions/ADR-022-nats-payload-discipline.md docs/decisions/ADR-024-consumer-batching.md
git commit -m "docs(adr): amend ADR-022/ADR-024 for Pocket-Block-Time header and batching valves"
```

---

## Part 2 — Valves (ADR-024 triggers 2-3 + eviction)

### Task 2: Sidecar stamps `Pocket-Block-Time` on fan-out

**Files:**
- Modify: `internal/nats/subjects.go` (header name constant — single source)
- Modify: `internal/fileplugin/sizecap.go` (publishFn signature)
- Modify: `internal/fileplugin/bootstrap.go` (closure + fanOutHeight pass time)
- Test: `internal/fileplugin/bootstrap_test.go`, `internal/fileplugin/sizecap_test.go`

- [ ] **Step 1: Write the failing test (header present on every published message)**

In `internal/fileplugin/bootstrap_test.go`, the existing fake publish func gains a header check. Extend the fake-publisher type used by the `fanOutHeight` tests to record the new 4th argument and assert:

```go
// In the existing fanOutHeight happy-path test, after collecting published messages:
for _, p := range published {
	if p.blockTimeNano <= 0 {
		t.Errorf("subject %s: missing Pocket-Block-Time (got %d)", p.subj, p.blockTimeNano)
	}
}
// And assert the value equals the fixture header time:
// p.blockTimeNano == header.Time.UnixNano() for the curated block.
```

- [ ] **Step 2: Run to verify it fails to compile** (signature mismatch is the failure)

Run: `go test ./internal/fileplugin/ 2>&1 | head -20`
Expected: compile error — fake publisher does not match new signature yet (after Step 3 below changes prod code, tests drive the rest).

- [ ] **Step 3: Implement**

`internal/nats/subjects.go` — add:

```go
// HeaderBlockTime carries the consensus block time (unix nanoseconds) on every
// fan-out message (ADR-022 amendment, Phase G). Required for partial flushes.
const HeaderBlockTime = "Pocket-Block-Time"
```

`internal/fileplugin/sizecap.go` — publishFn gains the time parameter (passthrough):

```go
type publishFn func(subj string, data []byte, msgID string, blockTimeNano int64) error
```

(`capPublish` body: pass `blockTimeNano` through unchanged; cap logic untouched.)

**Signature-change blast radius (update ALL of these in this task):**
- `fanOutHeight`'s `publish` parameter type (bootstrap.go:82) — same 4-arg form.
- The 4 call-site groups inside `fanOutHeight`: tx loop (~line 123), `emit` closure for events (~line 140), KV loop (~line 173), envelope (~line 192) — each gains the `btn` argument.
- EVERY fake publisher literal in `internal/fileplugin/bootstrap_test.go` (~8 anonymous closures) and `internal/fileplugin/sizecap_test.go` (~2) — mechanical 4th-param addition; keep their assertions; preserve any `//nolint` annotations attached to those literals.

`internal/fileplugin/bootstrap.go` — the JetStream closure publishes with headers:

```go
publish := capPublish(func(subj string, data []byte, msgID string, blockTimeNano int64) error {
	msg := &nats.Msg{Subject: subj, Data: data, Header: nats.Header{}}
	msg.Header.Set(natsx.HeaderBlockTime, strconv.FormatInt(blockTimeNano, 10))
	_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
	return err
}, slog.Default(), fpm)
```

(import `"github.com/nats-io/nats.go"`.) In `fanOutHeight`, compute `btn := header.Time.UnixNano()` once after `DecodeBlockHeader` and pass it to every `publish(...)` call, including the envelope.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/fileplugin/ ./internal/nats/`
Expected: PASS (sizecap tests updated for the 4-arg signature; assertions unchanged).

- [ ] **Step 5: Commit**

```bash
git add internal/nats/subjects.go internal/fileplugin/
git commit -m "feat(fileplugin): stamp Pocket-Block-Time header on fan-out (ADR-022 amendment)"
```

---

### Task 3: `store.FlushOnly` + `Message.TimeUnixNano` capture

**Files:**
- Create: `internal/store/flush_only.go`
- Modify: `internal/consumer/types.go` (Message field)
- Modify: `internal/consumer/batch.go` (capture header in handle())
- Test: `test/integration/store_error_paths_test.go` (FlushOnly), `internal/consumer/batch_test.go` (header capture)

- [ ] **Step 1: Write the failing integration test for FlushOnly**

Append to `test/integration/store_error_paths_test.go`:

```go
// phase G: FlushOnly commits handler writes WITHOUT touching cursor tables.
func TestFlushOnlyNoCursorAdvance(t *testing.T) {
	st := storeFrom(t) // REAL helper: test/integration/helpers_test.go:15
	ctx := context.Background()
	require.NoError(t, st.RegisterConsumer(ctx, "flushonly-test", ""))

	// Use store.InsertBlock inside the write fn (it handles decoder_version_id
	// itself — mirror how internal/consumer/block's handler calls it; build a
	// minimal types.BlockHeader like TestInsertBlock_ContextCancelled in this
	// file does). Height: 999991.
	err := st.FlushOnly(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return insertTestBlock(ctx, tx, st, 999991) // tiny local helper wrapping store.InsertBlock with a fixed header
	})
	require.NoError(t, err)

	// Row committed:
	var n int
	require.NoError(t, st.Pool().QueryRow(ctx, `SELECT count(*) FROM block WHERE height = 999991`).Scan(&n))
	require.Equal(t, 1, n)
	// Cursor untouched:
	up, err := st.ConsolidatedUpTo(ctx, "flushonly-test")
	require.NoError(t, err)
	require.Equal(t, int64(0), up)
	// No processed_heights row:
	done, err := st.HasProcessed(ctx, "flushonly-test", 999991)
	require.NoError(t, err)
	require.False(t, done)

	// Error path: write fn failure rolls back the block insert.
	err = st.FlushOnly(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := insertTestBlock(ctx, tx, st, 999992); err != nil {
			return err
		}
		return fmt.Errorf("boom")
	})
	require.Error(t, err)
	require.NoError(t, st.Pool().QueryRow(ctx, `SELECT count(*) FROM block WHERE height = 999992`).Scan(&n))
	require.Equal(t, 0, n)
}
```

(Implementer: `insertTestBlock` is a ~6-line local helper you write in this test file calling `store.InsertBlock` with a fixed `types.BlockHeader` — copy the header literal from `TestInsertBlock_ContextCancelled` in this same file. Check `InsertBlock`'s real signature before writing it.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test -tags=integration -count=1 -run TestFlushOnlyNoCursorAdvance ./test/integration/`
Expected: FAIL — `st.FlushOnly undefined`.

- [ ] **Step 3: Implement `internal/store/flush_only.go`**

```go
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// FlushOnly runs write inside one transaction WITHOUT advancing any cursor or
// recording processed_heights (ADR-024 triggers 2-3: partial flush). The
// caller must NOT ack NATS messages on success — only the block-boundary
// fence advances the cursor and acks.
func (s *Store) FlushOnly(ctx context.Context, write func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	if err := write(ctx, tx); err != nil {
		return fmt.Errorf("partial flush write: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("partial flush commit: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Header capture — failing unit test**

In `internal/consumer/batch_test.go` (follow the file's existing fake-msg pattern; if the existing fakes don't expose headers, extend the fake — NOT the prod code — to carry `nats.Header`):

```go
// phase G: fan-out messages capture Pocket-Block-Time into Message.TimeUnixNano.
// Build a fake jetstream.Msg for subject pokt.tx.7.0 with header
// Pocket-Block-Time: "1700000000000000000", drive it through handle(), then
// assert the buffered Message has TimeUnixNano == 1700000000000000000.
// Also: a fan-out msg WITHOUT the header buffers with TimeUnixNano == 0.
```

- [ ] **Step 5: Implement capture**

`internal/consumer/types.go`:

```go
type Message struct {
	Height       int64
	Subject      string
	MsgID        string
	TimeUnixNano int64 // Pocket-Block-Time header; 0 when absent (pre-Phase-G streams)
	Data         []byte
}
```

`internal/consumer/batch.go` `handle()`, fan-out path, where the Message is appended:

```go
var btn int64
if v := msg.Headers().Get(natsx.HeaderBlockTime); v != "" {
	btn, _ = strconv.ParseInt(v, 10, 64) // malformed → 0 → valves skip (WARN)
}
b.msgs = append(b.msgs, Message{Height: height, Subject: subject, MsgID: msgID, TimeUnixNano: btn, Data: msg.Data()})
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/consumer/... && go test -tags=integration -count=1 -run TestFlushOnlyNoCursorAdvance ./test/integration/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/store/flush_only.go internal/consumer/ test/integration/store_error_paths_test.go
git commit -m "feat(store,consumer): FlushOnly tx helper + Pocket-Block-Time capture (ADR-024 valves groundwork)"
```

---

### Task 4: Trigger 2 — size valve (synchronous) + nil-envelope handler contract

**Files:**
- Modify: `internal/metrics/metrics.go` (Consumer gains PartialFlushes, Evictions)
- Modify: `internal/consumer/batch.go` (BatchConfig knobs, partialFlushLocked, size check on append)
- Modify: `internal/consumer/supplier/handler.go` (nil-env Position derivation)
- Modify: `internal/consumer/types.go` ONLY if BatchHandler doc comment lives there (document nil-env contract)
- Test: `internal/consumer/supplier/handler_test.go`, `internal/consumer/batch_test.go`

- [ ] **Step 1: Metrics first (no test — declarative registration, covered by use)**

In `internal/metrics/metrics.go`, `Consumer` struct gains:

```go
PartialFlushes *prometheus.CounterVec // partial flushes by reason (ADR-024 triggers 2-3): labels consumer, reason
Evictions      *prometheus.CounterVec // orphaned height buffers dropped: label consumer
```

In `NewConsumer`, register them following the existing `counter(...)` helper, names `partial_flushes_total` (labels `consumer`, `reason`) and `evictions_total` (label `consumer`), subsystem `consumer` (same as siblings).

- [ ] **Step 2: Failing unit test — supplier handler accepts nil envelope**

In `internal/consumer/supplier/handler_test.go` (mirror the existing quiet-height FlushHeight unit test's fake-tx setup):

```go
// phase G: nil envelope (partial flush) derives Position from Message.TimeUnixNano.
// Call h.FlushHeight(ctx, tx, nil, msgs) where msgs[0] = a valid supplier fan-out
// Message with Height: 102542, TimeUnixNano: <fixture time>, Data: <reuse bytes
// from an existing decode test in this package>.
// Assert: no error, and the row insert used block_time == time.Unix(0, TimeUnixNano).UTC()
// (assert via the same mechanism the existing handler tests inspect inserts).
// Also: FlushHeight(ctx, tx, nil, nil) must return an error ("partial flush with no messages").
```

- [ ] **Step 3: Implement nil-env contract in `internal/consumer/supplier/handler.go`**

Replace the `env.Height` / `env.TimeUnixNano` usage at the top of `FlushHeight` (currently lines ~66-74):

```go
var height, tnano int64
switch {
case env != nil:
	height, tnano = env.Height, env.TimeUnixNano
case len(msgs) > 0 && msgs[0].TimeUnixNano > 0:
	height, tnano = msgs[0].Height, msgs[0].TimeUnixNano
default:
	return fmt.Errorf("partial flush requires messages with Pocket-Block-Time (ADR-022 amendment)")
}
dec, err := h.router.DecoderFor(height)
// ... unchanged ...
pos := types.Position{Height: height, Time: time.Unix(0, tnano).UTC()}
```

Document on the `BatchHandler` interface (wherever `FlushHeight` is declared): `env == nil` means partial flush (ADR-024 triggers 2-3) — write rows for msgs only, no envelope-derived rows, and derive Position from `msgs[0]`.

- [ ] **Step 4: Failing unit test — size valve fires**

In `internal/consumer/batch_test.go`:

```go
// phase G: size valve (ADR-024 trigger 2).
// Construct BatchRuntime with BatchConfig{..., MaxRows: 3} and a recording fake
// BatchHandler. Drive 3 fan-out msgs (with Pocket-Block-Time headers) for height 7
// through handle(). Assert:
//   - fake handler's FlushHeight was called ONCE with env == nil and 3 msgs;
//   - store cursor was NOT advanced (use the test store / fake store the file already uses;
//     if batch_test.go is pure-unit with a nil store, route partial flush through an
//     injectable flushOnly func — see Step 5 note);
//   - buffered msgs slice is now empty but acks retained (len(b.acks) == 3) and
//     seen-map still contains the 3 ids (a redelivery of msg 2 after the partial
//     flush must hit the dedup path: drive it and assert handler NOT called again);
//   - metrics PartialFlushes{consumer,reason="size"} == 1;
//   - a 4th msg buffers normally (no immediate re-flush below MaxRows).
// And: msgs WITHOUT TimeUnixNano (header absent) must NOT trigger a partial flush
// even when MaxRows is exceeded — assert handler not called, WARN logged once.
```

- [ ] **Step 5: Implement trigger 2 in `internal/consumer/batch.go`**

`BatchConfig` gains knobs; `heightBuf` gains bookkeeping:

```go
type BatchConfig struct {
	// ... existing fields ...
	MaxRows    int           // ADR-024 trigger 2; 0 → default 5000
	MaxAge     time.Duration // ADR-024 trigger 3; 0 → default 5s
	EvictAfter time.Duration // orphan eviction; 0 → default 10×MaxAge
	Now        func() time.Time // injectable clock for valve tests; nil → time.Now
}

type heightBuf struct {
	msgs        []Message
	acks        []jetstream.Msg
	seen        map[string]bool
	firstAt     time.Time // when the first message buffered (valve clock)
	flushedRows int       // rows already written by partial flushes
	warnedNoTime bool     // header-missing WARN emitted once per height
}
```

`NewBatchRuntime` applies defaults (`5000`, `5*time.Second`, `10*cfg.MaxAge`, `time.Now` — the single `time.Now` reference carries `//nolint:forbidigo // operational valve clock, never written to chain data rows (Invariant 1)`).

In `handle()` fan-out path after the append + Buffered gauge update:

```go
if len(b.msgs) >= r.maxRows {
	r.partialFlushLocked(ctx, height, b, "size")
}
```

`partialFlushLocked` (caller holds `r.mu` — the mutex itself lands in Task 5; in this task add the field and lock handle() so the diff is once):

```go
// partialFlushLocked writes the pending buffered rows WITHOUT advancing the
// cursor (ADR-024 triggers 2-3). Caller holds r.mu. Failure keeps the buffer
// intact — the next trigger or the fence retries; idempotent upserts absorb
// any rows that did commit.
func (r *BatchRuntime) partialFlushLocked(ctx context.Context, height int64, b *heightBuf, reason string) {
	if len(b.msgs) == 0 {
		return
	}
	if b.msgs[0].TimeUnixNano == 0 {
		if !b.warnedNoTime {
			b.warnedNoTime = true
			r.logger.Warn("partial flush skipped: messages lack Pocket-Block-Time", "consumer", r.handler.ID(), "height", height)
		}
		return
	}
	err := r.store.FlushOnly(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.FlushHeight(ctx, tx, nil, b.msgs)
	})
	if err != nil {
		r.logger.Error("partial flush failed; keeping buffer", "consumer", r.handler.ID(), "height", height, "reason", reason, "err", err)
		return
	}
	b.flushedRows += len(b.msgs)
	b.msgs = nil // acks + seen retained: fence acks after final commit (invariant 5)
	b.firstAt = r.now() // reset valve/eviction clock: data flowed, height is alive
	r.metrics.PartialFlushes.WithLabelValues(r.handler.ID(), reason).Inc()
	r.updateBufferedLocked()
}

// updateBufferedLocked sets the Buffered gauge to the TOTAL pending messages
// across all open heights (the existing per-event Set(len(b.msgs)) is wrong
// with multiple open heights — fix it at the existing call sites too).
func (r *BatchRuntime) updateBufferedLocked() {
	total := 0
	for _, b := range r.buf {
		total += len(b.msgs)
	}
	r.metrics.Buffered.WithLabelValues(r.handler.ID()).Set(float64(total))
}
```

Set `b.firstAt = r.now()` when the heightBuf is created (fan-out path only). Replace the two existing `Buffered...Set(...)` call sites in `handle()` with `r.updateBufferedLocked()`.

**Implementation order within this task matters:** implement the handler nil-env contract (Step 3) BEFORE wiring `partialFlushLocked` calls — otherwise the valve path nil-derefs `env.Height` in the supplier handler.

- [ ] **Step 6: Run tests**

Run: `go test ./internal/consumer/... ./internal/metrics/`
Expected: PASS, including the redelivery-after-partial-flush dedup assertion.

- [ ] **Step 7: Commit**

```bash
git add internal/consumer/ internal/metrics/metrics.go
git commit -m "feat(consumer): ADR-024 trigger 2 size valve with nil-envelope partial flush"
```

---

### Task 5: Trigger 3 — time valve + orphan eviction (background sweep)

**Files:**
- Modify: `internal/consumer/batch.go` (mutex, valveLoop, eviction, evicted-set, fence Nak)
- Test: `internal/consumer/batch_test.go`

**Design rationale (read first):** eviction drops a buffer whose envelope never arrived. If the envelope later arrives against an empty buffer, the quiet-height path would flush EMPTY and advance the cursor — silently losing the evicted rows. Timing-based prevention is UNSOUND: a Nak'd envelope redelivers after `reprocessDelay` (~seconds) while the evicted fan-out messages only redeliver when their per-message AckWait expires (60 s on the supplier consumer) — the envelope would race a still-empty buffer. Deterministic prevention instead: `r.evicted map[int64]int` records `len(b.seen)` (distinct Nats-Msg-Ids, INCLUDING partially-flushed ones) at eviction. The fence path for an evicted height returns an error while the rebuilt buffer's `len(b.seen)` is below the recorded count — `consume()` already Naks on error (NEVER Nak inside `handle()` and then return nil: `consume()` would Ack the envelope right after, and Nak-then-Ack on the same delivery is not contract-safe). When the count is reached, the flush proceeds and the mark clears. Re-eviction of a rebuilding buffer records `max(previous, len(seen))`. Restart clears the map — safe: on re-subscribe, JetStream redelivers ALL outstanding unacked in stream-sequence order (fan-out before envelope), the crash-recovery model tests 3/12 already verify.

- [ ] **Step 1: Failing unit tests**

In `internal/consumer/batch_test.go`, with an injectable fake clock (`Now` knob) and direct calls to the sweep function (do NOT sleep in unit tests):

```go
// phase G: time valve (ADR-024 trigger 3).
// MaxAge=5s. Buffer 2 msgs at t0 via handle(). Advance fake clock to t0+6s.
// Call r.sweepValves(ctx). Assert handler.FlushHeight called once with env==nil,
// PartialFlushes{reason="time"} == 1, buffer pending emptied, acks retained.

// phase G: orphan eviction.
// EvictAfter=50s. Buffer 2 msgs at t0. Advance clock t0+51s. sweepValves.
// Assert: buf deleted from r.buf, Evictions == 1, NO acks issued (fake msgs
// record Ack calls — must be zero), r.evicted[height] == 2 (the seen-count).

// phase G: late envelope after eviction is rejected until the buffer rebuilds.
// After the eviction above, drive the envelope through handle(): assert
// handle returns a non-nil error (consume() will Nak it), handler NOT called,
// cursor NOT advanced, r.evicted[height] STILL == 2 (mark kept).
// Re-drive ONE of the two fan-out msgs (partial redelivery), then the envelope
// again: STILL an error (1 < 2 — incomplete rebuild must keep failing).
// Re-drive the second fan-out msg, then the envelope: normal full flush
// (handler called WITH env and both msgs, cursor advanced, mark cleared).

// phase G: re-eviction of a rebuilding buffer keeps the max seen-count.
// Evict at count 2, redeliver 1 msg, advance clock past EvictAfter again,
// sweepValves → r.evicted[height] stays 2 (max(2,1)), Evictions == 2.

// phase G: eviction clock resets on partial flush (b.firstAt updated), so an
// actively-flushing height does not evict; buffers with msgs=nil but
// flushedRows>0 and a stale firstAt still evict (the fence may never come).
```

- [ ] **Step 2: Implement in `internal/consumer/batch.go`**

Add fields:

```go
type BatchRuntime struct {
	// ... existing ...
	mu      sync.Mutex
	now     func() time.Time
	maxRows int
	maxAge  time.Duration
	evictAfter time.Duration
	evicted map[int64]int // height → seen-count at eviction; fence rejects until rebuilt (see ADR-024 amendment)
}
```

`handle()` takes `r.mu.Lock(); defer r.mu.Unlock()` at the top (the whole body, including DB flush — correctness over concurrency; the valve sweep is the only other holder, note this in a comment). `NewBatchRuntime` initializes `evicted: map[int64]int{}` and applies the knob defaults; the single default-clock line carries the lint suppression EXACTLY here:

```go
	if cfg.Now == nil {
		cfg.Now = time.Now //nolint:forbidigo // operational valve clock; never written to chain-data rows (Invariant 1)
	}
```

Fence path, BEFORE the quiet-height fallback (return an ERROR — `consume()` Naks on error; do NOT Nak here and return nil, `consume()` would then Ack the envelope):

```go
if want, ok := r.evicted[height]; ok {
	have := 0
	if b := r.buf[height]; b != nil {
		have = len(b.seen)
	}
	if have < want {
		return fmt.Errorf("evicted height %d rebuilding: %d/%d messages redelivered", height, have, want)
	}
	delete(r.evicted, height) // rebuilt: proceed to a normal full flush below
}
```

Sweep (called from a goroutine started in `Run()` after the dormancy gate, ticking every `min(maxAge/2, time.Second)`; stop on ctx.Done):

```go
func (r *BatchRuntime) sweepValves(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for h, b := range r.buf {
		switch {
		case now.Sub(b.firstAt) > r.evictAfter:
			delete(r.buf, h)
			if len(b.seen) > r.evicted[h] { // re-eviction keeps the max seen-count
				r.evicted[h] = len(b.seen)
			}
			r.metrics.Evictions.WithLabelValues(r.handler.ID()).Inc()
			r.updateBufferedLocked()
			r.logger.Warn("evicted orphaned height buffer (no envelope)", "consumer", r.handler.ID(), "height", h, "seen", len(b.seen), "pending", len(b.msgs), "flushed_rows", b.flushedRows)
		case len(b.msgs) > 0 && now.Sub(b.firstAt) > r.maxAge:
			r.partialFlushLocked(ctx, h, b, "time")
		}
	}
}
```

`Run()` addition (after dormancy gate, before the consume loop):

```go
go func() {
	tick := r.maxAge / 2
	if tick > time.Second { tick = time.Second }
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sweepValves(ctx)
		}
	}
}()
```

- [ ] **Step 3: Run tests (race!)**

Run: `go test -race ./internal/consumer/...`
Expected: PASS, no data races.

- [ ] **Step 4: Commit**

```bash
git add internal/consumer/
git commit -m "feat(consumer): ADR-024 trigger 3 time valve + orphaned heightBuf eviction"
```

---

### Task 6: Integration — valves + large-block caps through the pipeline

**Files:**
- Create: `test/integration/batch_valves_test.go`
- Create: `test/integration/sidecar_caps_test.go`

Use the existing integration harness (NATS + Postgres testcontainers, supplier consumer; mirror `batch_runtime_crash_test.go` setup).

**Harness prerequisites (write these FIRST, in this task):**
- The harness has `startRuntime` (`test/integration/nats_helpers_test.go:128`) for plain Runtime but NO BatchRuntime starter with custom knobs. Add `startBatchRuntime(t *testing.T, ..., cfg consumer.BatchConfig) ...` (mirror however `batch_runtime_crash_test.go` builds its BatchRuntime today, parameterizing `MaxRows`/`MaxAge`/`EvictAfter`).
- Configure the test JetStream consumer's `AckWait` explicitly (e.g. 2 s) so eviction-path redeliveries happen quickly and deterministically relative to `EvictAfter=1s`.
- `internal/fileplugin`'s meta/data writer helpers (`buildMinimalMeta` etc.) are NOT exported. For the caps test, duplicate the ~30 lines of writer code as local helpers in `sidecar_caps_test.go` (length-delimited records: RequestFinalizeBlock + ResponseFinalizeBlock marshal — copy the format from `bootstrap_test.go`); do NOT export the fileplugin test helpers.

- [ ] **Step 1: Valves integration test**

```go
// phase G: partial flush end-to-end. Publish (direct JetStream publish, with
// Pocket-Block-Time headers, mirroring sidecar subjects/Msg-Ids) 12 supplier
// fan-out msgs for height H with the consumer configured MaxRows=5 — expect:
//   - 2 partial flushes (size) visible via metric scrape or rows present in DB
//     while cursor is still at H-1 (poll: rows appear, ConsolidatedUpTo < H);
//   - then publish the envelope → cursor advances to H, all msgs acked
//     (no redeliveries observed for 2×AckWait), row count exact (no dupes).
// phase G: eviction end-to-end. Publish fan-out for H+1 with NO envelope,
// consumer MaxAge=200ms EvictAfter=1s → eviction metric fires, then publish
// the envelope → first delivery Nak'd, redelivery round rebuilds → cursor
// eventually reaches H+1 with exact rows (idempotency absorbs the partial rows).
```

- [ ] **Step 2: Sidecar caps integration test**

```go
// phase G: large-block caps through sidecar→NATS→consumer (decision: integration-level).
// Build a synthetic FilePlugin dir (the bootstrap_test.go fixtures show the
// block-{H}-meta/-data format; reuse its writer helpers if exported, else
// construct minimal meta/data files):
//   - height H1: one tx payload ~300 KiB (> SoftCapBytes, < HardCapBytes) →
//     Bootstrap publishes it; consumer processes; assert WARN counter
//     pocketscribe_fileplugin_oversize_soft_total == 1 and the block consumer
//     advances past H1.
//   - height H2: one tx payload ~1.1 MiB (> HardCapBytes) → Bootstrap returns
//     error for that height (height aborted, envelope NEVER published); assert
//     oversize_refused metric == 1, consumer cursor does NOT advance to H2,
//     and the consumer's eviction valve eventually drops the partial buffer
//     (evictions_total ≥ 1) — the operator-visible state per ADR-024 amendment.
// NATS server in the harness must allow >1MiB test publish attempts to be
// REFUSED at the sidecar, not the server: no server max_payload change needed
// because the sidecar never publishes the oversize payload.
```

- [ ] **Step 3: Run**

Run: `go test -tags=integration -count=1 -run 'TestBatchValves|TestSidecarCaps' ./test/integration/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/integration/batch_valves_test.go test/integration/sidecar_caps_test.go
git commit -m "test(integration): ADR-024 valves + sidecar payload caps through the pipeline"
```

---

## Part 3 — Reconciler hardening

### Task 7: Reconciler metrics + immediate first sync + tests

**Files:**
- Modify: `internal/metrics/metrics.go` (NewReconciler)
- Create: `internal/app/reconciler/loop.go`
- Modify: `internal/app/reconciler/cmd.go` (use runLoop + metrics)
- Test: `internal/app/reconciler/loop_test.go`

- [ ] **Step 1: Failing unit test for the loop**

`internal/app/reconciler/loop_test.go`:

```go
package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

// phase G: runLoop syncs IMMEDIATELY on start, then on every tick; errors are
// counted and do not stop the loop; ctx cancel exits with ctx.Err().
func TestRunLoopImmediateThenTicks(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.NewReconciler(reg)
	calls := make(chan struct{}, 16)
	n := 0
	sync := func(ctx context.Context) (int, error) {
		calls <- struct{}{}
		n++
		if n == 2 {
			return 0, errors.New("lcd down")
		}
		return 3, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, 20*time.Millisecond, sync, slog.Default(), m) }()

	// Immediate first sync (before any tick):
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("no immediate first sync")
	}
	// At least two more ticks (one of them errors):
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("tick %d never fired", i+1)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if got := testutil.ToFloat64(m.SyncErrors); got < 1 {
		t.Fatalf("SyncErrors = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(m.Syncs); got < 2 {
		t.Fatalf("Syncs = %v, want >= 2", got)
	}
}
```

(`prometheus.NewRegistry` is imported for the fresh registry; `testutil.ToFloat64` reads the counters.)

- [ ] **Step 2: Implement**

`internal/metrics/metrics.go`:

```go
// Reconciler instruments the ps reconciler upgrade-refresh loop (ADR-018).
type Reconciler struct {
	Syncs      prometheus.Counter // successful upgrade syncs
	SyncErrors prometheus.Counter // failed upgrade syncs (loop continues; router serves cached table)
}

func NewReconciler(reg prometheus.Registerer) *Reconciler {
	c := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Subsystem: "reconciler", Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	return &Reconciler{
		Syncs:      c("syncs_total", "successful upgrade table refreshes"),
		SyncErrors: c("sync_errors_total", "failed upgrade refreshes (cached table keeps serving)"),
	}
}
```

`internal/app/reconciler/loop.go`:

```go
package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

// runLoop refreshes the upgrades table immediately, then every interval, until
// ctx is canceled (ADR-018: failures are logged and counted; the router keeps
// serving the cached upgrades table until the next successful sync).
func runLoop(ctx context.Context, interval time.Duration, sync func(context.Context) (int, error), logger *slog.Logger, m *metrics.Reconciler) error {
	refresh := func() {
		n, err := sync(ctx)
		if err != nil {
			m.SyncErrors.Inc()
			logger.Error("reconciler: upgrades sync failed", "err", err)
			return
		}
		m.Syncs.Inc()
		logger.Info("reconciler: upgrades synced", "count", n)
	}
	refresh() // immediate first sync — do not wait a full interval at startup
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			refresh()
		}
	}
}
```

`cmd.go` RunE body: keep config/store/syncer setup, then replace the inline loop with:

```go
m := metrics.NewReconciler(prometheus.DefaultRegisterer)
return runLoop(ctx, interval, func(ctx context.Context) (int, error) {
	return syncer.Sync(ctx, st, cfg.Network.UpgradeNames)
}, slog.Default(), m)
```

- [ ] **Step 3: Verify signal wiring**

The loop exits via `cmd.Context()` cancellation. Confirm `cmd/ps/main.go` (or `internal/app/root.go`) installs `signal.NotifyContext` (the consumer subcommands rely on the same mechanism) — if it does NOT, add it there (one `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)` around `root.ExecuteContext`), with a unit-less manual check: `go run ./cmd/ps reconciler --config configs/<existing yaml> --interval 1h` + Ctrl-C exits cleanly. Document the finding in the commit body.

- [ ] **Step 4: Run**

Run: `go test ./internal/app/reconciler/ ./internal/metrics/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/app/reconciler/ internal/metrics/metrics.go
git commit -m "feat(reconciler): immediate first sync + prometheus counters + loop unit tests"
```

---

## Part 4 — Deferred edge-case tests

### Task 8: `migrate down` + intra-tx fault injection

**Files:**
- Modify: `test/integration/store_error_paths_test.go`

- [ ] **Step 1: migrate down test**

```go
// phase G: Migrate("down") rolls back exactly one migration and Migrate("up")
// reapplies it. The harness SHARES one Postgres container (helpers expose
// pg.DSN + pg.Reset) — down-migrating it would corrupt sibling tests. Start a
// DEDICATED container for this test with the same testcontainers starter the
// harness uses for the shared one (find it in the TestMain/helpers wiring of
// test/integration — e.g. tc.StartPostgres(ctx) — and call it locally with
// t.Cleanup(terminate)).
func TestMigrateDownOneStep(t *testing.T) {
	dsn := startDedicatedPostgres(t) // local helper wrapping the harness's container starter
	ctx := context.Background()
	require.NoError(t, store.Migrate(ctx, dsn, "up"))
	require.NoError(t, store.Migrate(ctx, dsn, "down"))   // one step down (goose.DownContext)
	require.NoError(t, store.Migrate(ctx, dsn, "status")) // still coherent
	require.NoError(t, store.Migrate(ctx, dsn, "up"))     // round-trip: reapplies cleanly
}
```

(Implementer: check the LAST migration is down-safe; goose Down on a migration without a Down section errors — if migration NNNN lacks `-- +goose Down`, the correct assertion is that explicit error, and the test documents which migration blocks down-migration. Do not write a fake Down clause just to pass.)

- [ ] **Step 2: intra-tx fault injection test**

```go
// phase G: crash AFTER partial rows written but BEFORE commit → full rollback,
// no partial state, cursor untouched, redelivery converges (invariants 4+5).
// Use a handler wrapper that, on first FlushHeight call, performs its writes
// via the real handler THEN returns an error (forcing ProcessHeight rollback
// AFTER tx work happened — this is the gap batch_runtime_crash_test.go does
// not cover: its fault fires BEFORE any write).
// Assert after the failed attempt: zero supplier rows for H, cursor < H,
// HasProcessed(H) false. Then let redelivery run with the fault disarmed:
// rows exact, cursor == H, seal computes.
```

Place next to the existing fault tests in `test/integration/batch_runtime_crash_test.go` style — same harness, new test func `TestIntraTxFaultRollsBackPartialWrites` (file: `store_error_paths_test.go` or `batch_runtime_crash_test.go`, implementer's call — keep with its siblings).

- [ ] **Step 3: Run**

Run: `go test -tags=integration -count=1 -run 'TestMigrateDown|TestIntraTxFault' ./test/integration/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/integration/
git commit -m "test(integration): migrate-down round-trip + intra-tx fault rollback"
```

---

### Task 9: Upgrade boundary through the full pipeline

**Files:**
- Create: `test/integration/boundary_pipeline_test.go`

- [ ] **Step 1: Write the test**

```go
// phase G: feed the v0_1_26→v0_1_27 boundary through sidecar→NATS→consumers.
// Fixtures: the v0.1.26 era lives under test/fixtures/v0_1_20/ (decoder-range
// dir; era heights 135297-247892) — last curated v0.1.26-era height: 227782
// (also 190979, quiet 220010). First v0.1.27-era height: 247894 under
// test/fixtures/v0_1_27/ (proxy for true boundary 247893, a 15MB migration
// dump excluded by the >5MB rule — see test/fixtures/README.md). VERIFY both
// triplets exist on disk before writing assertions; if the README matrix
// disagrees with this comment, the README wins.
// Setup: real upgrades table rows via fixturereport.MainnetUpgrades() (the
// chain-authoritative list), DB-driven router (NOT NewStaticRouter — the point
// is exercising the height→decoder dispatch the production path uses).
// Drive both heights with Bootstrap() and run block + supplier consumers.
// Assert:
//   - block rows exist for both heights with the CORRECT decoder_version_id
//     on each side of the boundary (query decoder_version_id joined to its name);
//   - supplier rows (if the chosen fixtures have supplier activity) decode
//     without error — i.e., no decoder mismatch at the boundary;
//   - both heights seal (AND-seal with both consumers).
```

- [ ] **Step 2: Run**

Run: `go test -tags=integration -count=1 -run TestBoundaryPipeline ./test/integration/`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/integration/boundary_pipeline_test.go
git commit -m "test(integration): upgrade boundary v0_1_26->v0_1_27 through full pipeline"
```

---

### Task 10: Restart corners

**Files:**
- Modify: `test/integration/resilience_test.go`

- [ ] **Step 1: Partial simultaneous restart**

```go
// phase G: one consumer crashes, the other keeps processing; crashed one
// restarts and catches up; AND-seal correct throughout.
// Mirror TestMultipleConsumersCrashRecover's harness:
//   - run block + supplier consumers; kill ONLY supplier at height 3;
//   - publish heights 4-6; assert block cursor reaches 6 while supplier stays 3
//     and NO height >3 seals (AND-seal holds with a lagging member);
//   - restart supplier; assert it converges to 6 and heights 4-6 seal.
```

- [ ] **Step 2: Restart with an open partially-flushed height**

```go
// phase G: crash AFTER a partial flush (rows committed, cursor NOT advanced,
// msgs unacked) → restart → redelivery re-buffers → fence flush → idempotent
// convergence (exact row count, cursor advances, no dupes).
// Configure MaxRows small (e.g. 3), publish 5 fan-out msgs, wait for the
// partial-flush metric/rows, kill the consumer BEFORE publishing the envelope,
// restart, publish the envelope, assert convergence.
```

- [ ] **Step 3: Run**

Run: `go test -tags=integration -count=1 -run 'TestPartialSimultaneousRestart|TestRestartWithOpenPartialFlush' ./test/integration/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/integration/resilience_test.go
git commit -m "test(integration): partial restart + restart over open partially-flushed height"
```

---

## Part 5 — Coverage: tooling, then fills

### Task 11: Combined coverage tooling + gate

**Files:**
- Create: `scripts/covmerge/main.go`
- Create: `scripts/covgate/main.go`
- Modify: `Makefile` (`coverage` target)

- [ ] **Step 1: covmerge — merge text profiles (no external dep)**

`scripts/covmerge/main.go`:

```go
// Command covmerge merges Go cover profiles (text format) by summing counts
// per block. Usage: covmerge out.cov in1.cov in2.cov [...]
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: covmerge OUT IN...")
		os.Exit(2)
	}
	counts := map[string]int64{} // "file:l.c,l.c numStmt" -> max/sum count
	order := []string{}
	for _, in := range os.Args[2:] {
		f, err := os.Open(in)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 1024*1024)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "mode:") {
				continue
			}
			i := strings.LastIndexByte(line, ' ')
			key, cnt := line[:i], line[i+1:]
			var c int64
			fmt.Sscanf(cnt, "%d", &c)
			if _, seen := counts[key]; !seen {
				order = append(order, key)
			}
			counts[key] += c
		}
		f.Close()
	}
	out, err := os.Create(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer out.Close()
	fmt.Fprintln(out, "mode: atomic")
	for _, k := range order {
		fmt.Fprintf(out, "%s %d\n", k, counts[k])
	}
}
```

- [ ] **Step 2: covgate — per-package thresholds from the merged profile**

`scripts/covgate/main.go`:

```go
// Command covgate enforces per-package coverage thresholds from a merged cover
// profile: 100% for internal/decoders/* (excluding gen/), 90% for everything
// else under internal/. Packages with zero statements are skipped. Generated
// trees ("/gen/") and internal/proto are excluded entirely.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
)

const module = "github.com/pokt-network/pocketscribe/"

type agg struct{ stmts, covered int64 }

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: covgate merged.cov")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	pkgs := parseProfile(f) // extracted for unit-testability (no os.Exit inside)
	failed := report(pkgs, os.Stdout)
	if failed {
		os.Exit(1)
	}
}

// parseProfile aggregates statement/covered counts per package from a cover
// profile. Each line: "path/file.go:l.c,l.c numStmt count".
func parseProfile(r io.Reader) map[string]*agg {
	pkgs := map[string]*agg{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line) // [file:ranges, numStmt, count]
		if len(fields) != 3 {
			continue
		}
		file := fields[0][:strings.LastIndexByte(fields[0], ':')] // ranges contain '.' and ',' but the file/ranges separator is the LAST ':'
		nstmt, err1 := strconv.ParseInt(fields[1], 10, 64)
		cnt, err2 := strconv.ParseInt(fields[2], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		pkg := strings.TrimPrefix(path.Dir(file), module)
		if strings.Contains(pkg, "/gen") || pkg == "internal/proto" || !strings.HasPrefix(pkg, "internal/") {
			continue
		}
		a := pkgs[pkg]
		if a == nil {
			a = &agg{}
			pkgs[pkg] = a
		}
		a.stmts += nstmt
		if cnt > 0 {
			a.covered += nstmt
		}
	}
	return pkgs
}
// report prints the per-package table sorted by name and returns whether any
// package is below its bar.
func report(pkgs map[string]*agg, w io.Writer) bool {
	names := make([]string, 0, len(pkgs))
	for pkg := range pkgs {
		names = append(names, pkg)
	}
	sort.Strings(names)
	failed := false
	for _, pkg := range names {
		a := pkgs[pkg]
		if a.stmts == 0 {
			continue
		}
		pct := 100 * float64(a.covered) / float64(a.stmts)
		want := 90.0
		if strings.HasPrefix(pkg, "internal/decoders/") || pkg == "internal/decoders" {
			want = 100.0
		}
		status := "ok"
		if pct < want {
			status = "FAIL"
			failed = true
		}
		fmt.Fprintf(w, "%-50s %6.1f%% (want %.0f%%) %s\n", pkg, pct, want, status)
	}
	return failed
}
```

(Write `scripts/covgate/main_test.go`: feed `parseProfile` a 5-line synthetic profile — one decoder file fully covered, one internal file at 50% — assert the aggregation numbers and that `report` returns true. Same for covmerge if you extract its merge loop into a function; both scripts count toward no package bar — they live outside `internal/` — but their tests keep the gate itself honest.)

- [ ] **Step 3: Makefile target**

```makefile
coverage: ## Combined unit+integration coverage with per-package gate (90/100)
	@go test -count=1 -coverprofile=cover.unit.out -coverpkg=./internal/... ./internal/...
	@go test -tags=integration -count=1 -coverprofile=cover.int.out -coverpkg=./internal/... ./test/...
	@go run ./scripts/covmerge cover.merged.out cover.unit.out cover.int.out
	@go run ./scripts/covgate cover.merged.out
```

(`./cmd/...` is deliberately excluded: `cmd/ps` is a `package main` shim with no tests; the gate only scores `internal/`. Update the covgate imports: `bufio, fmt, io, os, path, sort, strconv, strings`.)

Add `cover.*.out` to `.gitignore`.

- [ ] **Step 4: Run and record the baseline**

Run: `make coverage`
Expected: the gate RUNS and prints the per-package table; it will FAIL on packages below bar — that failure list is the WORK ORDER for Tasks 12-13. Save the output to the task notes (do not commit cover files).

- [ ] **Step 5: Commit**

```bash
git add scripts/covmerge scripts/covgate Makefile .gitignore
git commit -m "build(coverage): combined unit+integration profile merge + per-package gate (90/100)"
```

---

### Task 12: Decoders to 100% (v0_1_0, v0_1_8, v0_1_27)

**Files:**
- Test: `internal/decoders/v0_1_0/*_test.go`, `internal/decoders/v0_1_8/*_test.go`, `internal/decoders/v0_1_27/*_test.go`

Unit-only baseline: v0_1_0 92.9%, v0_1_8 95.0%, v0_1_27 95.0% (the combined profile from Task 11 may differ — use ITS numbers).

- [ ] **Step 1: Identify uncovered lines per package**

```bash
go tool cover -func=cover.merged.out | grep -E 'decoders/v0_1_(0|8|27)/' | grep -v '100.0%'
```

- [ ] **Step 2: Write tests for exactly those paths**

Rules (memoria `feedback_coverage_bar` — REAL error paths, no padding):
- Uncovered branches in decoders are almost always malformed-bytes error returns: feed truncated/corrupt protobuf bytes and assert the wrapped error message.
- Decode functions with version-specific shapes: use the golden fixture bytes from `test/fixtures/` for valid-path gaps; mutate a copy (truncate at N bytes, flip a field tag) for error paths.
- NEVER assert on padding like `_ = String()` — every new test asserts a behavior.

- [ ] **Step 3: Verify 100%**

Run: `make coverage` → the three decoder packages report 100.0%.

- [ ] **Step 4: Commit**

```bash
git add internal/decoders/
git commit -m "test(decoders): close v0_1_0/v0_1_8/v0_1_27 to 100% with malformed-bytes error paths"
```

---

### Task 13: `internal/` packages to ≥90% (combined)

**Files (driven by the Task 11 gate output — expected offenders from the unit-only baseline):**
- Test: `internal/metrics/metrics_test.go`, `internal/log/*_test.go`, `internal/types/*_test.go`, `internal/app/inspect/*_test.go`, `internal/app/indexer/*_test.go`, plus whatever the merged gate still flags among `internal/consumer`, `internal/consumer/supplier`, `internal/fileplugin`, `internal/nats`, `internal/router`, `internal/store`, `internal/upgrades`.

- [ ] **Step 1: Re-run the gate; list real offenders**

Run: `make coverage` — packages already ≥90% on the COMBINED profile need nothing. Do not write tests for packages the gate passes.

- [ ] **Step 2: Fill, package by package, smallest first**

Per-package guidance (real error paths only):
- `internal/log`: construction + level parsing + bad-level error.
- `internal/types`: any method with logic (String/Validate); pure structs have zero statements and the gate skips them.
- `internal/app/inspect`, `internal/app/indexer`: cobra cmd construction (`NewCmd()` returns wired command, flags registered, required-flag validation errors), NOT live execution.
- `internal/metrics`: each `NewX` registers without panic on a fresh registry; double-registration panic is REAL behavior — assert it.
- `internal/nats`: subject builders' edge cases, malformed-subject errors in `HeightFromSubject`.
- `internal/router`, `internal/store`, `internal/upgrades`, `internal/fileplugin`, `internal/consumer*`: whatever specific lines the merged profile says — typically error wrapping on DB/HTTP failures. For store error paths prefer the existing `store_error_paths_test.go` integration patterns (real Postgres, induced failures: closed pool, constraint violation) over mocks.

- [ ] **Step 3: Gate green**

Run: `make coverage`
Expected: exit 0, all packages ok.

- [ ] **Step 4: Commit (one commit per package or small groups — keep them reviewable)**

```bash
git add internal/ test/integration/
git commit -m "test(internal): close coverage to >=90% per package on combined profile"
```

---

## Part 6 — CI + docs

### Task 14: Harden `make ci` + GitHub Actions

**Files:**
- Modify: `Makefile`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Makefile**

```makefile
ci: vet fmt-check lint lint-integration test-race ## Fast CI checks (no containers)

ci-full: ci coverage ## Everything incl. integration + coverage gate

lint-integration: ## Lint with the integration build tag (handoff hard rule)
	@golangci-lint run --build-tags=integration ./...

test-race: ## Unit tests under the race detector
	@go test -race -count=1 ./...
```

(Keep the existing `test` target; `ci` now runs race by default — SEMANTIC CHANGE to the existing `ci: vet fmt-check lint test`, mention it in the commit body. `ci-full` is what the workflow's heavy job and the pre-merge gate run.)

- [ ] **Step 2: Workflow**

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]
jobs:
  checks:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: golangci-lint
        uses: golangci/golangci-lint-action@v8
        with:
          version: v2.11.4   # match the local binary (config schema version: "2")
          args: --timeout=5m
      - name: lint (integration tags)
        uses: golangci/golangci-lint-action@v8
        with:
          version: v2.11.4
          args: --build-tags=integration --timeout=5m
      - run: make vet fmt-check
      - run: make test-race
  integration:
    runs-on: ubuntu-latest
    timeout-minutes: 25
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: integration + combined coverage gate
        run: make coverage
```

(Two action invocations instead of a raw `run:` for the integration-tag pass — the action guarantees the binary; a bare `golangci-lint` in `run:` is not guaranteed on PATH. Version pinned to the local 2.11.4 (`golangci-lint version`, config `version: "2"`). ubuntu-latest ships Docker; testcontainers works out of the box.)

- [ ] **Step 3: Verify locally**

Run: `make ci && make ci-full`
Expected: both green. (Workflow itself can only be verified on push — user pushes; note this in the PR/commit message.)

- [ ] **Step 4: Commit**

```bash
git add Makefile .github/workflows/ci.yml
git commit -m "build(ci): race + integration-tag lint in make ci; GitHub Actions workflow with coverage gate"
```

---

### Task 15: Per-component READMEs

**Files:**
- Create: `internal/consumer/README.md`, `internal/fileplugin/README.md`, `internal/store/README.md`, `internal/router/README.md`, `internal/decoders/README.md`, `internal/nats/README.md`, `internal/upgrades/README.md`, `internal/protover/README.md`, `internal/app/README.md`
- Verify: trivial packages (`internal/log`, `internal/version`, `internal/types`, `internal/metrics`, `internal/config`, `internal/fixturereport`, `internal/proto`, `internal/chain` if present) each have a `doc.go` with a package comment — create missing ones (3-6 lines each).

- [ ] **Step 1: Write the 9 READMEs**

Template (keep each under ~60 lines; link, don't duplicate):

```markdown
# internal/<pkg>

<One-paragraph purpose: what this package owns, in PocketScribe vocabulary.>

## Invariants honored
<Bullet list referencing CLAUDE.md invariant numbers + ADRs this package implements.>

## Entry points
<The 2-5 exported types/functions a reader starts from, one line each.>

## Testing
<Which test layers cover this package and where the tests live.>
```

Content anchors per package (the writer reads the package first; these are the load-bearing facts each README must state):
- `consumer`: BatchRuntime fence + valves (ADR-024), ack-after-commit (invariant 5), dormancy gate, Nats-Msg-Id dedup, eviction semantics.
- `fileplugin`: ADR-022 fan-out + envelope-last ordering contract, payload caps, Pocket-Block-Time header.
- `store`: ProcessHeight vs FlushOnly, append-only (invariants 2-4), consolidation/sealing queries, goose migrations.
- `router`: height→decoder dispatch from upgrades table, version-based never network-based.
- `decoders`: one dir per version, 100% coverage rule, shape-guard, never modify an existing version dir.
- `nats`: subjects single source (DRY rule), header constants, JetStream wrappers.
- `upgrades`: ADR-018, LCD sync, idempotent upsert.
- `protover`: semver-canonical comparisons, dotted vs underscored spellings.
- `app`: composition roots per subcommand; thin cobra wiring, logic lives in domain packages.

- [ ] **Step 2: Commit**

```bash
git add internal/*/README.md internal/*/doc.go
git commit -m "docs: per-component READMEs for the 9 major internal packages"
```

---

### Task 16: Spec completion note + final gates

**Files:**
- Modify: `docs/superpowers/specs/2026-06-08-slice-1-design.md` (append Phase G complete note)

- [ ] **Step 1: Full gates**

```bash
make ci-full
go test -tags=integration -count=1 ./test/...
```

Expected: everything green, coverage gate exit 0.

- [ ] **Step 2: Append the Phase G completion note to the spec** (same style as Phases A-F; summarize: valves + eviction, Pocket-Block-Time header, reconciler hardening, edge-case tests added, coverage 90/100 combined gate, make ci hardened, GitHub Actions, READMEs; state Slice 1 §15 exit criterion met — all 7 bullets).

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-slice-1-design.md
git commit -m "docs(spec): record Phase G completion - Slice 1 exit criterion met"
```

- [ ] **Step 4: Merge (orchestrator does this, not a subagent)**

```bash
git checkout main && git merge --no-ff slice-1/phase-g -m "Merge Phase G: hardening + reconciler refresh - Slice 1 complete"
```

NO push — the user pushes.

---

## Execution notes

- Subagent-driven: implementer (sonnet) + reviewer (sonnet) per task, evidence-based review (commands run, output shown).
- Task order: 0→1→2→3→4→5→6 strictly sequential (each builds on the previous). 7, 8, 9, 10 independent of each other (parallelizable AFTER 6, since 10 uses valve knobs). 11 before 12/13. 14 after 13 (gate must pass). 15 anytime after 5 (READMEs describe valves). 16 last.
- Every task: gofmt before commit; lint with and without `-tags=integration` must stay clean.
- The valves change BatchRuntime semantics — if ANY existing integration test (1-27) breaks, that is a regression to fix in the task that broke it, never a test to weaken.
- Empty-block edge case (spec §9-G): already covered by tests 21 (quiet heights + AND-seal) and 22b (quiet-height cancel/restart); quiet heights create NO heightBuf (buffer exists only after a fan-out message), so long quiet runs cannot accumulate orphans — no new test needed; the eviction tests in Task 6 cover the orphan path.
