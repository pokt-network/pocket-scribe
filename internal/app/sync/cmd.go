package sync

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/config"
	"github.com/pokt-network/pocketscribe/internal/store"
	upgrades "github.com/pokt-network/pocketscribe/internal/upgrades"
)

const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds the `ps sync-upgrades` command.
func NewCmd() *cobra.Command {
	var (
		cfgPath string
		dsn     string
	)
	cmd := &cobra.Command{
		Use:   "sync-upgrades",
		Short: "Sync applied upgrade plans from the LCD into the upgrades table (ADR-018)",
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
			n, err := syncer.Sync(ctx, st, cfg.Network.UpgradeNames)
			if err != nil {
				return fmt.Errorf("sync upgrades: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "upserted %d upgrade(s)\n", n)
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to network config YAML (required)")
	_ = cmd.MarkFlagRequired("config")
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
