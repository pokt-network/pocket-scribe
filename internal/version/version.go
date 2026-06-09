package version

import "fmt"

// Build-time metadata. Overridden via -ldflags -X (see Makefile `build`).
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String renders the version line shown by `ps version`.
func String() string {
	return fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, Date)
}
