package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	blockhandler "github.com/pokt-network/pocketscribe/internal/consumer/block"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
	"github.com/pokt-network/pocketscribe/internal/types"
)

const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// storeInserter adapts the package-level store.InsertBlock to the
// blockhandler.Inserter interface.
type storeInserter struct{}

func (storeInserter) InsertBlock(ctx context.Context, tx pgx.Tx, h *types.BlockHeader) error {
	return store.InsertBlock(ctx, tx, h)
}

func newBlockCmd() *cobra.Command {
	var (
		dsn     string
		natsURL string
	)
	cmd := &cobra.Command{
		Use:   "block",
		Short: "Run the block consumer (reads BlockEnvelope, writes block table)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			st, err := store.New(ctx, dsn)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer st.Close()

			nc, err := natsx.Connect(ctx, natsURL)
			if err != nil {
				return fmt.Errorf("connect nats: %w", err)
			}
			defer nc.Close()

			stream, err := nc.EnsureStream(ctx, 2*time.Minute)
			if err != nil {
				return fmt.Errorf("ensure stream: %w", err)
			}

			jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
				Durable:       "block",
				FilterSubject: natsx.BlockSubjectFilter,
				AckPolicy:     jetstream.AckExplicitPolicy,
				DeliverPolicy: jetstream.DeliverAllPolicy,
				MaxDeliver:    -1,
				AckWait:       30 * time.Second,
			})
			if err != nil {
				return fmt.Errorf("create consumer: %w", err)
			}

			h := blockhandler.New(storeInserter{})
			rt := runtime.NewRuntime(runtime.Config{
				Handler:  h,
				Store:    st,
				Consumer: jsCons,
				Logger:   slog.Default(),
				Metrics:  metrics.NewConsumer(prometheus.DefaultRegisterer),
			})
			return rt.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
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
