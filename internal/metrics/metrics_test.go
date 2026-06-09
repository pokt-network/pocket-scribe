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
