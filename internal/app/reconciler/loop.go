package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/pokt-network/pocketscribe/internal/metrics"
)

// runLoop refreshes the upgrades table immediately, then every interval, until
// ctx is canceled (ADR-018: failures are logged and counted; the router keeps
// serving the cached upgrades table until the next successful sync).
func runLoop(ctx context.Context, interval time.Duration, sync func(context.Context) (int, error), logger *slog.Logger, m *metrics.Reconciler) error {
	refresh := func() {
		n, err := sync(ctx)
		if err != nil {
			m.SyncErrors.Inc()
			logger.Error("reconciler: upgrades sync failed", "err", err)
			return
		}
		m.Syncs.Inc()
		logger.Info("reconciler: upgrades synced", "count", n)
	}
	refresh() // immediate first sync — do not wait a full interval at startup
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			refresh()
		}
	}
}
