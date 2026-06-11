package inspect

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// CursorRow holds display data for one consumer's cursor state.
type CursorRow struct {
	Name              string
	FirstValidVersion string
	Active            bool
	ConsolidatedUpTo  int64
	ProcessedCount    int64
	LastHeight        int64
}

// RenderCursors formats rows into a tab-aligned table string.
// Returns at minimum the header even when rows is empty.
func RenderCursors(rows []CursorRow) string {
	var sb strings.Builder
	tw := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintln(tw, "CONSUMER\tVERSION\tACTIVE\tCONSOLIDATED\tPROCESSED\tLAST HEIGHT")
	_, _ = fmt.Fprintln(tw, "--------\t-------\t------\t------------\t---------\t-----------")
	for _, r := range rows {
		active := "no"
		if r.Active {
			active = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\n",
			r.Name, r.FirstValidVersion, active,
			r.ConsolidatedUpTo, r.ProcessedCount, r.LastHeight,
		)
	}
	_ = tw.Flush()
	return sb.String()
}
