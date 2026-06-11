package metrics // package comment lives in internal/metrics/doc.go (do not repeat it — revive package-comments)

import "github.com/prometheus/client_golang/prometheus"

const namespace = "pocketscribe"

// Consumer holds the metrics emitted by the generic consumer runtime.
type Consumer struct {
	Processed      *prometheus.CounterVec // messages successfully processed
	GapsTotal      *prometheus.CounterVec // times a gap was observed during consolidation
	Consolidated   *prometheus.GaugeVec   // current consolidated_up_to high-water mark
	Buffered       *prometheus.GaugeVec   // fan-out messages buffered per consumer awaiting the block-boundary flush
	PartialFlushes *prometheus.CounterVec // partial flushes by reason (ADR-024 triggers 2-3): labels consumer, reason
	Evictions      *prometheus.CounterVec // orphaned height buffers dropped without acking: label consumer
}

// NewConsumer constructs and registers the consumer metric vectors on reg.
func NewConsumer(reg prometheus.Registerer) *Consumer {
	counter := func(name, help string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, []string{"consumer"})
		reg.MustRegister(v)
		return v
	}
	counter2 := func(name, help string, labels []string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, labels)
		reg.MustRegister(v)
		return v
	}
	gauge := func(name, help string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, []string{"consumer"})
		reg.MustRegister(v)
		return v
	}
	return &Consumer{
		Processed:      counter("messages_processed_total", "Messages processed (committed) per consumer."),
		GapsTotal:      counter("gaps_total", "Gap observations during contiguous consolidation, per consumer."),
		Consolidated:   gauge("consolidated_up_to", "Per-consumer contiguous high-water mark (block height)."),
		Buffered:       gauge("buffered_messages", "Fan-out messages buffered per consumer awaiting the block-boundary flush."),
		PartialFlushes: counter2("partial_flushes_total", "Partial flushes triggered before the block-boundary fence (ADR-024 triggers 2-3), by reason.", []string{"consumer", "reason"}),
		Evictions:      counter("evictions_total", "Orphaned height buffers dropped from memory without acking (AckWait redelivers them)."),
	}
}

// Reconciler instruments the ps reconciler upgrade-refresh loop (ADR-018).
type Reconciler struct {
	Syncs      prometheus.Counter // successful upgrade syncs
	SyncErrors prometheus.Counter // failed upgrade syncs (loop continues; router serves cached table)
}

// NewReconciler constructs and registers the reconciler metric set on reg.
func NewReconciler(reg prometheus.Registerer) *Reconciler {
	c := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "reconciler", Name: name, Help: help,
		})
		reg.MustRegister(v)
		return v
	}
	return &Reconciler{
		Syncs:      c("syncs_total", "successful upgrade table refreshes"),
		SyncErrors: c("sync_errors_total", "failed upgrade refreshes (cached table keeps serving)"),
	}
}

// FilePlugin holds the metrics emitted by the fileplugin sidecar.
type FilePlugin struct {
	OversizeSoft    prometheus.Counter // payloads above the 256 KiB soft cap (still published)
	OversizeRefused prometheus.Counter // payloads above the 1 MiB hard cap (refused)
}

// NewFilePlugin constructs and registers the sidecar metric set on reg.
func NewFilePlugin(reg prometheus.Registerer) *FilePlugin {
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "fileplugin", Name: name, Help: help,
		})
		reg.MustRegister(c)
		return c
	}
	return &FilePlugin{
		OversizeSoft:    counter("oversize_soft_total", "Payloads above the 256 KiB soft cap (published anyway)."),
		OversizeRefused: counter("oversize_refused_total", "Payloads above the 1 MiB hard cap (refused at the source)."),
	}
}
