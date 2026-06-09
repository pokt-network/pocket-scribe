package log // package comment lives in internal/log/doc.go (do not repeat it — revive package-comments)

import (
	"io"
	"log/slog"
)

// New returns a JSON slog.Logger writing to w at the given level.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
