package consumer

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/config"
	runtime "github.com/pokt-network/pocketscribe/internal/consumer"
	supplierhandler "github.com/pokt-network/pocketscribe/internal/consumer/supplier"
	"github.com/pokt-network/pocketscribe/internal/metrics"
	natsx "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/router"
	"github.com/pokt-network/pocketscribe/internal/store"
)

func newSupplierCmd() *cobra.Command {
	var (
		cfgPath string
		dsn     string
		natsURL string
	)
	cmd := &cobra.Command{
		Use:   "supplier",
		Short: "Run the supplier consumer (decodes tx/events/KV across version ranges, writes supplier tables)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

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

			// TxSubjectFilter ("pokt.tx.>") intentionally delivers ALL modules' txs — the
			// handler filters by type_url (spec §4.8 "filters internally"). Cost is
			// O(tx_count) buffered msgs per height; fine-grained tx routing is a Phase G
			// candidate (note added to the ADR-024 amendment).
			filters := []string{natsx.TxSubjectFilter, natsx.KVSubjectFilter("supplier"), natsx.BlockSubjectFilter}
			for _, et := range supplierhandler.EventTypes {
				filters = append(filters, natsx.EventSubjectFilter(et))
			}
			jsCons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
				Durable:        "supplier",
				FilterSubjects: filters,
				AckPolicy:      jetstream.AckExplicitPolicy,
				DeliverPolicy:  jetstream.DeliverAllPolicy,
				MaxDeliver:     -1,
				AckWait:        60 * time.Second,
				// MaxAckPending: unlimited — the batch runtime buffers an entire height's
				// fan-out msgs (up to ~15k KV + tx + events for large blocks) before the
				// BlockEnvelope fence arrives. The server default (1000) would stall
				// delivery of the envelope when a block has >1000 fan-out messages,
				// because JetStream stops sending once the in-flight unacked count
				// exceeds MaxAckPending (ADR-024 fence invariant; see phase-e root-cause).
				MaxAckPending: -1,
			})
			if err != nil {
				return fmt.Errorf("create consumer: %w", err)
			}

			rtr, err := router.NewDBRouter(ctx, st, router.DefaultRegistry(), cfg.Network.GenesisDecoderVersion)
			if err != nil {
				return fmt.Errorf("build router: %w", err)
			}

			ids, err := st.DecoderVersionIDs(ctx)
			if err != nil {
				return fmt.Errorf("load decoder version IDs: %w", err)
			}

			h := supplierhandler.New(rtr, ids)
			rt := runtime.NewBatchRuntime(runtime.BatchConfig{
				Handler:        h,
				Store:          st,
				Consumer:       jsCons,
				Logger:         slog.Default(),
				Metrics:        metrics.NewConsumer(prometheus.DefaultRegisterer),
				GenesisVersion: cfg.Network.GenesisDecoderVersion,
			})
			return rt.Run(ctx)
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to network config YAML (required)")
	_ = cmd.MarkFlagRequired("config")
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
	cmd.Flags().StringVar(&natsURL, "nats-url", envOr("PS_NATS_URL", "nats://localhost:4222"),
		"NATS server URL; overrides $PS_NATS_URL")
	return cmd
}
