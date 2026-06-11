package inspect

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// StreamRow holds display data for one JetStream stream.
type StreamRow struct {
	Name      string
	Msgs      uint64
	Bytes     uint64
	FirstSeq  uint64
	LastSeq   uint64
	Consumers []ConsumerInfo
}

// ConsumerInfo holds display data for one JetStream consumer.
type ConsumerInfo struct {
	Name     string
	Pending  uint64
	AckFloor uint64
}

// fmtBytes formats a byte count as a human-readable string (B/KB/MB/GB).
func fmtBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// RenderStreams formats stream rows into a tab-aligned table string.
// Each stream is followed by an indented consumer sub-table when consumers exist.
func RenderStreams(rows []StreamRow) string {
	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintln(tw, "STREAM\tMSGS\tBYTES\tFIRST_SEQ\tLAST_SEQ\tCONSUMERS")
	_, _ = fmt.Fprintln(tw, "------\t----\t-----\t---------\t--------\t---------")
	for _, r := range rows {
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%d\n",
			r.Name, r.Msgs, fmtBytes(r.Bytes),
			r.FirstSeq, r.LastSeq, len(r.Consumers),
		)
	}
	_ = tw.Flush()

	// Per-stream consumer detail below the main table.
	for _, r := range rows {
		if len(r.Consumers) == 0 {
			continue
		}
		_, _ = fmt.Fprintf(&sb, "\n  [%s consumers]\n", r.Name)
		ctw := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(ctw, "  CONSUMER\tPENDING\tACK_FLOOR")
		_, _ = fmt.Fprintln(ctw, "  --------\t-------\t---------")
		for _, c := range r.Consumers {
			_, _ = fmt.Fprintf(ctw, "  %s\t%d\t%d\n", c.Name, c.Pending, c.AckFloor)
		}
		_ = ctw.Flush()
	}

	return sb.String()
}
