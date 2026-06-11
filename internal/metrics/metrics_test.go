package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestConsumerMetricsRegisterAndCount(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConsumer(reg)

	m.GapsTotal.WithLabelValues("noop-a").Inc()
	m.GapsTotal.WithLabelValues("noop-a").Inc()

	if got := testutil.ToFloat64(m.GapsTotal.WithLabelValues("noop-a")); got != 2 {
		t.Fatalf("gaps_total = %v, want 2", got)
	}
	m.Consolidated.WithLabelValues("noop-a").Set(42)
	if got := testutil.ToFloat64(m.Consolidated.WithLabelValues("noop-a")); got != 42 {
		t.Fatalf("consolidated_up_to = %v, want 42", got)
	}
}

// TestConsumerPartialFlushesMetric verifies that PartialFlushes (ADR-024 trigger 2-3)
// is registered and increments correctly with consumer+reason labels.
func TestConsumerPartialFlushesMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConsumer(reg)

	m.PartialFlushes.WithLabelValues("supplier", "size").Inc()
	m.PartialFlushes.WithLabelValues("supplier", "size").Inc()
	m.PartialFlushes.WithLabelValues("supplier", "time").Inc()

	if got := testutil.ToFloat64(m.PartialFlushes.WithLabelValues("supplier", "size")); got != 2 {
		t.Fatalf("partial_flushes_total{size} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.PartialFlushes.WithLabelValues("supplier", "time")); got != 1 {
		t.Fatalf("partial_flushes_total{time} = %v, want 1", got)
	}
}

// TestConsumerEvictionsMetric verifies that Evictions (ADR-024 orphaned buffer drop)
// is registered and increments correctly with the consumer label.
func TestConsumerEvictionsMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConsumer(reg)

	m.Evictions.WithLabelValues("supplier").Inc()
	m.Evictions.WithLabelValues("supplier").Inc()
	m.Evictions.WithLabelValues("block").Inc()

	if got := testutil.ToFloat64(m.Evictions.WithLabelValues("supplier")); got != 2 {
		t.Fatalf("evictions_total{supplier} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.Evictions.WithLabelValues("block")); got != 1 {
		t.Fatalf("evictions_total{block} = %v, want 1", got)
	}
}
