package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
)

// reprocessDelay is how long a Nak'd message waits before redelivery, so a
// transient Postgres outage does not spin the loop.
const reprocessDelay = 500 * time.Millisecond

// Runtime drives one consumer: subscribe → ack-after-commit → repeat.
type Runtime struct {
	handler        Handler
	store          *store.Store
	consumer       jetstream.Consumer
	logger         *slog.Logger
	metrics        *metrics.Consumer
	genesisVersion string
}

// Config wires a Runtime's collaborators.
type Config struct {
	Handler  Handler
	Store    *store.Store
	Consumer jetstream.Consumer
	Logger   *slog.Logger
	Metrics  *metrics.Consumer
	// GenesisVersion is network.genesis_decoder_version; empty disables the dormancy gate.
	GenesisVersion string
}

// NewRuntime constructs a Runtime.
func NewRuntime(cfg Config) *Runtime {
	return &Runtime{
		handler:        cfg.Handler,
		store:          cfg.Store,
		consumer:       cfg.Consumer,
		logger:         cfg.Logger,
		metrics:        cfg.Metrics,
		genesisVersion: cfg.GenesisVersion,
	}
}

// reconnectDelay is the backoff between attempts to re-establish the message
// iterator after a NATS disconnect.
const reconnectDelay = 500 * time.Millisecond

// Run self-registers the consumer, then processes messages until ctx is
// canceled, transparently re-establishing the subscription across NATS
// disconnects. It returns ctx.Err() on clean shutdown.
func (r *Runtime) Run(ctx context.Context) error {
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
func (r *Runtime) consume(ctx context.Context) error {
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
		_ = msg.Ack() // ack only AFTER a successful commit (invariant #5)
	}
}

func (r *Runtime) handle(ctx context.Context, msg jetstream.Msg) error {
	id := r.handler.ID()
	height, err := natsx.HeightFromBlockSubject(msg.Subject())
	if err != nil {
		_ = msg.Term() // unparseable subject: never redeliver
		r.logger.Error("bad subject; terminating", "consumer", id, "subject", msg.Subject())
		return nil //nolint:nilerr // err is a parse error; message has been terminated, not a propagatable failure
	}
	m := Message{
		Height:  height,
		Subject: msg.Subject(),
		MsgID:   natsx.MsgID(msg.Subject(), height, 0),
		Data:    msg.Data(),
	}

	next, err := r.store.ProcessHeight(ctx, id, height, func(ctx context.Context, tx pgx.Tx) error {
		return r.handler.Handle(ctx, tx, m)
	})
	if err != nil {
		return err
	}

	r.metrics.Processed.WithLabelValues(id).Inc()
	r.metrics.Consolidated.WithLabelValues(id).Set(float64(next))
	if next < height { // a gap sits between the contiguous frontier and this height
		r.metrics.GapsTotal.WithLabelValues(id).Inc()
		r.logger.Warn("gap detected", "consumer", id, "from", next+1, "to", height-1, "processed", height)
	}
	return nil
}
