package fileplugin

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/config"
	"github.com/pokt-network/pocketscribe/internal/fileplugin"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
)

// NewCmd builds the `ps fileplugin` command.
func NewCmd() *cobra.Command {
	var (
		bootstrap bool
		cfgPath   string
		inputDir  string
		maxHeight int64
		natsURL   string
	)
	cmd := &cobra.Command{
		Use:   "fileplugin",
		Short: "Run the FilePlugin sidecar (fan-out ADR-022 block data to NATS)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !bootstrap {
				return fmt.Errorf("only --bootstrap mode is implemented; pass --bootstrap")
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			ctx := cmd.Context()
			nc, err := natsx.Connect(ctx, natsURL)
			if err != nil {
				return fmt.Errorf("connect nats: %w", err)
			}
			defer nc.Close()

			if _, err := nc.EnsureStream(ctx, 2*time.Minute); err != nil {
				return fmt.Errorf("ensure stream: %w", err)
			}

			heights, messages, err := fileplugin.Bootstrap(context.Background(), nc, inputDir, maxHeight, cfg.Network.ChainID)
			if err != nil {
				return fmt.Errorf("bootstrap: %w", err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "published %d height(s), %d message(s)\n", heights, messages)
			return nil
		},
	}
	cmd.Flags().BoolVar(&bootstrap, "bootstrap", false, "republish captured block files to NATS (required)")
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to network config YAML (required)")
	_ = cmd.MarkFlagRequired("config")
	cmd.Flags().StringVar(&inputDir, "input-dir", ".", "directory containing block-{H}-{meta,data} files")
	cmd.Flags().Int64Var(&maxHeight, "max-height", 0, "skip files above this height (0 = no cap)")
	cmd.Flags().StringVar(&natsURL, "nats-url", envOr("PS_NATS_URL", "nats://localhost:4222"),
		"NATS server URL; overrides $PS_NATS_URL")
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
