// Package doctor implements the `ps doctor` health-check command.
package doctor

import (
	"fmt"
	"strings"
)

// CheckResult holds the outcome of one health check.
type CheckResult struct {
	Name   string
	OK     bool
	Detail string
	Err    error
}

// RenderChecks formats a slice of CheckResult into a human-readable report
// and returns (output, exitCode). exitCode is 0 iff all checks passed.
func RenderChecks(results []CheckResult) (string, int) {
	var sb strings.Builder
	code := 0

	for _, r := range results {
		if !r.OK || r.Err != nil {
			code = 1
		}
		mark := "✓"
		detail := r.Detail
		if !r.OK || r.Err != nil {
			mark = "✗"
			if r.Err != nil {
				if detail != "" {
					detail = r.Err.Error() + " — " + detail
				} else {
					detail = r.Err.Error()
				}
			}
		}
		if detail != "" {
			fmt.Fprintf(&sb, "%s  %-14s  %s\n", mark, r.Name, detail)
		} else {
			fmt.Fprintf(&sb, "%s  %s\n", mark, r.Name)
		}
	}

	return sb.String(), code
}
