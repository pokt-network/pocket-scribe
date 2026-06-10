package metrics // package comment lives in internal/metrics/doc.go (do not repeat it — revive package-comments)

import "github.com/prometheus/client_golang/prometheus"

const namespace = "pocketscribe"

// Consumer holds the metrics emitted by the generic consumer runtime.
type Consumer struct {
	Processed    *prometheus.CounterVec // messages successfully processed
	GapsTotal    *prometheus.CounterVec // times a gap was observed during consolidation
	Consolidated *prometheus.GaugeVec   // current consolidated_up_to high-water mark
	Buffered     *prometheus.GaugeVec   // fan-out messages buffered per consumer awaiting the block-boundary flush
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
	gauge := func(name, help string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: "consumer", Name: name, Help: help,
		}, []string{"consumer"})
		reg.MustRegister(v)
		return v
	}
	return &Consumer{
		Processed:    counter("messages_processed_total", "Messages processed (committed) per consumer."),
		GapsTotal:    counter("gaps_total", "Gap observations during contiguous consolidation, per consumer."),
		Consolidated: gauge("consolidated_up_to", "Per-consumer contiguous high-water mark (block height)."),
		Buffered:     gauge("buffered_messages", "Fan-out messages buffered per consumer awaiting the block-boundary flush."),
	}
}
