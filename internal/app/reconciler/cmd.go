package reconciler

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/config"
	"github.com/pokt-network/pocketscribe/internal/store"
	upgrades "github.com/pokt-network/pocketscribe/internal/upgrades"
)

const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds the `ps reconciler` command (upgrades-refresh loop for Slice 1).
func NewCmd() *cobra.Command {
	var (
		cfgPath  string
		dsn      string
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "reconciler",
		Short: "Periodic reconciler: refreshes the upgrades table from the LCD (ADR-018)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			if len(cfg.Endpoints.LCD) == 0 {
				return fmt.Errorf("no LCD endpoints configured")
			}
			st, err := store.New(ctx, dsn)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer st.Close()
			syncer := upgrades.New(cfg.Endpoints.LCD[0], nil)
			logger := slog.Default()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					n, err := syncer.Sync(ctx, st, cfg.Network.UpgradeNames)
					if err != nil {
						logger.Error("reconciler: upgrades sync failed", "err", err)
						continue
					}
					logger.Info("reconciler: upgrades synced", "count", n)
				}
			}
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to network config YAML (required)")
	_ = cmd.MarkFlagRequired("config")
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Minute,
		"how often to refresh upgrade plans from the LCD")
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
