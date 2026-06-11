package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	psv1 "github.com/pokt-network/pocketscribe/internal/proto/gen/pocketscribe/v1"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// BatchRuntime drives a fan-out consumer: buffer per height → flush on the
// pokt.block.{H} envelope in ONE Postgres tx → ack everything after commit
// (invariants 4+5; ADR-024 block-boundary fence; triggers 2-3 size/time valves done;
// orphaned heightBuf eviction arrives in next commit).
type BatchRuntime struct {
	handler        BatchHandler
	store          *store.Store
	consumer       jetstream.Consumer
	logger         *slog.Logger
	metrics        *metrics.Consumer
	genesisVersion string

	mu  sync.Mutex // guards buf; the only other holder is the valve sweep goroutine (T5)
	buf map[int64]*heightBuf

	// Valve knobs (ADR-024 trigger 2-3). Populated by NewBatchRuntime from BatchConfig.
	maxRows    int
	maxAge     time.Duration
	evictAfter time.Duration
	now        func() time.Time // operational valve clock; NEVER written to chain-data rows (Invariant 1)
	// flushFn / processFn are seams over store.FlushOnly / store.ProcessHeight
	// (wired by NewBatchRuntime); unit tests inject fakes — no Store interface.
	flushFn   func(ctx context.Context, write func(ctx context.Context, tx pgx.Tx) error) error
	processFn func(ctx context.Context, consumer string, height int64, write func(ctx context.Context, tx pgx.Tx) error) (int64, error)
}

type heightBuf struct {
	msgs         []Message
	acks         []jetstream.Msg
	seen         map[string]bool // Nats-Msg-Id dedup of AckWait redeliveries
	firstAt      time.Time       // time the first message arrived (valve/eviction clock reset on partial flush)
	flushedRows  int             // rows already written by partial flushes before the fence arrives
	warnedNoTime bool            // emit WARN at most once per height when Pocket-Block-Time is absent
}

// BatchConfig wires a BatchRuntime's collaborators.
type BatchConfig struct {
	Handler  BatchHandler
	Store    *store.Store
	Consumer jetstream.Consumer
	Logger   *slog.Logger
	Metrics  *metrics.Consumer
	// GenesisVersion is network.genesis_decoder_version; empty disables the dormancy gate.
	GenesisVersion string

	// MaxRows is the size-cap trigger (ADR-024 trigger 2): partial flush when
	// buffered rows ≥ MaxRows. Default 5000.
	MaxRows int
	// MaxAge is the time-cap trigger (ADR-024 trigger 3): partial flush when
	// the oldest buffered message exceeds MaxAge. Default 5s.
	MaxAge time.Duration
	// EvictAfter is the orphaned-buffer eviction window: a height buffer is dropped
	// when no envelope has arrived within EvictAfter. Default 10×MaxAge (50s).
	EvictAfter time.Duration
	// Now is the clock used for valve and eviction decisions. Defaults to time.Now.
	// MUST NOT be used when writing chain-data rows (Invariant 1).
	Now func() time.Time
}

// NewBatchRuntime constructs a BatchRuntime.
func NewBatchRuntime(cfg BatchConfig) *BatchRuntime {
	if cfg.MaxRows == 0 {
		cfg.MaxRows = 5000
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 5 * time.Second
	}
	if cfg.EvictAfter == 0 {
		cfg.EvictAfter = 10 * cfg.MaxAge
	}
	if cfg.Now == nil {
		// time.Now used as a function VALUE (not a call): forbidigo bans calls only.
		// This clock is for valve/eviction decisions; never used for chain-data rows (Invariant 1).
		cfg.Now = time.Now
	}
	var flushFn func(ctx context.Context, write func(ctx context.Context, tx pgx.Tx) error) error
	var processFn func(ctx context.Context, consumer string, height int64, write func(ctx context.Context, tx pgx.Tx) error) (int64, error)
	if cfg.Store != nil {
		flushFn = cfg.Store.FlushOnly
		processFn = cfg.Store.ProcessHeight
	}
	return &BatchRuntime{
		handler:        cfg.Handler,
		store:          cfg.Store,
		consumer:       cfg.Consumer,
		logger:         cfg.Logger,
		metrics:        cfg.Metrics,
		genesisVersion: cfg.GenesisVersion,
		buf:            make(map[int64]*heightBuf),
		maxRows:        cfg.MaxRows,
		maxAge:         cfg.MaxAge,
		evictAfter:     cfg.EvictAfter,
		now:            cfg.Now,
		flushFn:        flushFn,
		processFn:      processFn,
	}
}

// Run self-registers the consumer, then processes messages until ctx is
// canceled, transparently re-establishing the subscription across NATS
// disconnects. It returns ctx.Err() on clean shutdown.
func (r *BatchRuntime) Run(ctx context.Context) error {
	if err := r.store.RegisterConsumer(ctx, r.handler.ID(), r.handler.FirstValidVersion()); err != nil {
		return err
	}
	if d, err := dormant(ctx, r.store, r.handler.ID(), r.handler.FirstValidVersion(), r.genesisVersion, r.logger); err != nil {
		return err
	} else if d {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.consume(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Recoverable (NATS disconnect / iterator closed by the server):
			// back off, then re-establish the subscription.
			r.logger.Warn("consume interrupted; reconnecting", "consumer", r.handler.ID(), "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectDelay):
			}
			continue
		}
		return nil // consume returns nil only on clean ctx cancellation
	}
}

// consume creates a message iterator and processes messages until ctx is
// canceled (returns nil) or the iterator fails (returns the error so Run can
// reconnect). Each message follows the ack-after-commit protocol.
func (r *BatchRuntime) consume(ctx context.Context) error {
	iter, err := r.consumer.Messages()
	if err != nil {
		return err
	}
	defer iter.Stop()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			iter.Stop() // unblocks iter.Next()
		case <-done:
		}
	}()
	defer close(done)

	for {
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err // disconnect / closed by server → Run reconnects
		}
		if herr := r.handle(ctx, msg); herr != nil {
			r.logger.Error("process failed; will redeliver", "consumer", r.handler.ID(), "err", herr)
			_ = msg.NakWithDelay(reprocessDelay)
			continue
		}
		// Ack-after-commit (Invariant 5): only the envelope triggers a Postgres
		// commit, so only envelope messages are acked here.  Fan-out messages are
		// stored in b.acks and acked inside handle() AFTER ProcessHeight commits
		// (lines 175-176).  Acking fan-out messages here — before commit — would
		// violate Invariant 5: a crash after the early ack but before the envelope
		// arrives would permanently lose those rows (no redelivery, no DB row).
		if natsx.IsBlockSubject(msg.Subject()) {
			_ = msg.Ack()
		}
	}
}

func (r *BatchRuntime) handle(ctx context.Context, msg jetstream.Msg) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := r.handler.ID()
	subject := msg.Subject()
	height, err := natsx.HeightFromSubject(subject)
	if err != nil {
		_ = msg.Term()
		r.logger.Error("bad subject; terminating", "consumer", id, "subject", subject)
		return nil // terminated, not propagatable
	}
	if !natsx.IsBlockSubject(subject) {
		b := r.buf[height]
		if b == nil {
			var firstAt time.Time
			if r.now != nil {
				firstAt = r.now()
			}
			b = &heightBuf{seen: map[string]bool{}, firstAt: firstAt}
			r.buf[height] = b
		}
		msgID := ""
		if md, err := msg.Metadata(); err == nil {
			msgID = fmt.Sprintf("%d", md.Sequence.Stream) // fallback ordering key
		}
		if hdr := msg.Headers().Get("Nats-Msg-Id"); hdr != "" {
			msgID = hdr
		}
		if b.seen[msgID] {
			// Redelivery of an already-buffered message (AckWait expired while
			// the height is still open). NEVER ack here: acking ANY delivery
			// acks the MESSAGE (invariant 5 — a crash before the flush commit
			// would then lose it permanently). InProgress resets the ack timer
			// so the server pauses redelivery while we wait for the fence.
			_ = msg.InProgress()
			return nil
		}
		b.seen[msgID] = true
		var btn int64
		if v := msg.Headers().Get(natsx.HeaderBlockTime); v != "" {
			btn, _ = strconv.ParseInt(v, 10, 64) // malformed → 0 → valves skip (WARN)
		}
		b.msgs = append(b.msgs, Message{Height: height, Subject: subject, MsgID: msgID, TimeUnixNano: btn, Data: msg.Data()})
		b.acks = append(b.acks, msg)
		// Size valve (ADR-024 trigger 2): partial flush when buffered rows hit MaxRows.
		if r.maxRows > 0 && len(b.msgs) >= r.maxRows {
			r.partialFlushLocked(ctx, height, b, "size")
		}
		r.updateBufferedLocked()
		return nil
	}
	// ── the fence: envelope closes the height ──
	var env psv1.BlockEnvelope
	if err := env.Unmarshal(msg.Data()); err != nil {
		return fmt.Errorf("block envelope at height %d: %w", height, err)
	}
	b := r.buf[height]
	if b == nil {
		b = &heightBuf{} // quiet height: empty flush still advances the cursor
	}
	next, err := r.processFn(ctx, id, height, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.FlushHeight(ctx, tx, &env, b.msgs)
	})
	if err != nil {
		return err // envelope is Nak'd; buffered msgs stay buffered (dedup absorbs their redeliveries)
	}
	// Fan-out acks happen strictly AFTER commit (invariant 5). Do NOT "optimize"
	// by acking before/at buffering: an AckWait redelivery of a buffered msg hits
	// the seen-map (InProgress, never acked before commit); after a crash,
	// redeliveries re-buffer into an empty runtime, re-flush, and every insert is
	// ON CONFLICT no-op.
	for _, a := range b.acks {
		_ = a.Ack()
	}
	delete(r.buf, height)
	r.metrics.Processed.WithLabelValues(id).Inc()
	r.metrics.Consolidated.WithLabelValues(id).Set(float64(next))
	r.updateBufferedLocked()
	if next < height {
		r.metrics.GapsTotal.WithLabelValues(id).Inc()
		r.logger.Warn("gap detected", "consumer", id, "from", next+1, "to", height-1, "processed", height)
	}
	return nil
}

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
	err := r.flushFn(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.FlushHeight(ctx, tx, nil, b.msgs)
	})
	if err != nil {
		r.logger.Error("partial flush failed; keeping buffer", "consumer", r.handler.ID(), "height", height, "reason", reason, "err", err)
		return
	}
	b.flushedRows += len(b.msgs)
	b.msgs = nil        // acks + seen retained: fence acks after final commit (invariant 5)
	b.firstAt = r.now() // reset valve/eviction clock: data flowed, height is alive
	r.metrics.PartialFlushes.WithLabelValues(r.handler.ID(), reason).Inc()
}

// updateBufferedLocked sets the Buffered gauge to the TOTAL pending messages
// across all open heights.
func (r *BatchRuntime) updateBufferedLocked() {
	total := 0
	for _, b := range r.buf {
		total += len(b.msgs)
	}
	r.metrics.Buffered.WithLabelValues(r.handler.ID()).Set(float64(total))
}
