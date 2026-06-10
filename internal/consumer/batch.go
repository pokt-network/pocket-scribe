package consumer

import (
	"context"
	"fmt"
	"log/slog"
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
// (invariants 4+5; ADR-024 block-boundary fence; size/time valves are Phase G).
type BatchRuntime struct {
	handler  BatchHandler
	store    *store.Store
	consumer jetstream.Consumer
	logger   *slog.Logger
	metrics  *metrics.Consumer
	// TODO(phase-g): partial-flush valves (ADR-024 triggers 2-3) + orphaned heightBuf eviction.
	buf map[int64]*heightBuf // accessed only from the consume goroutine
}

type heightBuf struct {
	msgs []Message
	acks []jetstream.Msg
	seen map[string]bool // Nats-Msg-Id dedup of AckWait redeliveries
}

// BatchConfig wires a BatchRuntime's collaborators.
type BatchConfig struct {
	Handler  BatchHandler
	Store    *store.Store
	Consumer jetstream.Consumer
	Logger   *slog.Logger
	Metrics  *metrics.Consumer
}

// NewBatchRuntime constructs a BatchRuntime.
func NewBatchRuntime(cfg BatchConfig) *BatchRuntime {
	return &BatchRuntime{
		handler:  cfg.Handler,
		store:    cfg.Store,
		consumer: cfg.Consumer,
		logger:   cfg.Logger,
		metrics:  cfg.Metrics,
		buf:      make(map[int64]*heightBuf),
	}
}

// Run self-registers the consumer, then processes messages until ctx is
// canceled, transparently re-establishing the subscription across NATS
// disconnects. It returns ctx.Err() on clean shutdown.
func (r *BatchRuntime) Run(ctx context.Context) error {
	if err := r.store.RegisterConsumer(ctx, r.handler.ID(), r.handler.FirstValidVersion()); err != nil {
		return err
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
	id := r.handler.ID()
	subject := msg.Subject()
	height, err := natsx.HeightFromSubject(subject)
	if err != nil {
		_ = msg.Term()
		r.logger.Error("bad subject; terminating", "consumer", id, "subject", subject)
		return nil //nolint:nilerr // terminated, not propagatable
	}
	if !natsx.IsBlockSubject(subject) {
		b := r.buf[height]
		if b == nil {
			b = &heightBuf{seen: map[string]bool{}}
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
		b.msgs = append(b.msgs, Message{Height: height, Subject: subject, MsgID: msgID, Data: msg.Data()})
		b.acks = append(b.acks, msg)
		r.metrics.Buffered.WithLabelValues(id).Set(float64(len(b.msgs)))
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
	next, err := r.store.ProcessHeight(ctx, id, height, func(ctx context.Context, tx pgx.Tx) error {
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
	r.metrics.Buffered.WithLabelValues(id).Set(0)
	r.metrics.Processed.WithLabelValues(id).Inc()
	r.metrics.Consolidated.WithLabelValues(id).Set(float64(next))
	if next < height {
		r.metrics.GapsTotal.WithLabelValues(id).Inc()
		r.logger.Warn("gap detected", "consumer", id, "from", next+1, "to", height-1, "processed", height)
	}
	return nil
}
