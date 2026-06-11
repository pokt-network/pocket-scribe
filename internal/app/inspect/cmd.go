package inspect

import (
	"context"
	"fmt"
	"os"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"

	psnats "github.com/pokt-network/pocketscribe/internal/nats"
	"github.com/pokt-network/pocketscribe/internal/store"
)

const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"
const defaultNATSURL = natsgo.DefaultURL

// NewCmd builds the `ps inspect` command group.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Read-only observability over indexer state (cursors, streams)",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newCursorsCmd())
	cmd.AddCommand(newStreamsCmd())
	return cmd
}

// ── ps inspect cursors ────────────────────────────────────────────────────────

func newCursorsCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "cursors",
		Short: "Show per-consumer cursor state (consolidation, processed count, last height)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			st, err := store.New(ctx, dsn)
			if err != nil {
				return fmt.Errorf("open store: %w", err)
			}
			defer st.Close()

			rows, err := queryCursorRows(ctx, st)
			if err != nil {
				return err
			}
			fmt.Print(RenderCursors(rows))
			return nil
		},
	}
	cmd.Flags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")
	return cmd
}

// queryCursorRows joins consumer_registry + consumer_consolidation +
// processed_heights to build the display rows.
func queryCursorRows(ctx context.Context, st *store.Store) ([]CursorRow, error) {
	rows, err := st.Pool().Query(ctx, `
		SELECT
		    r.consumer_name,
		    r.first_valid_version,
		    r.active,
		    COALESCE(c.consolidated_up_to, 0)                     AS consolidated_up_to,
		    COALESCE(ph.processed_count, 0)                       AS processed_count,
		    COALESCE(ph.last_height, 0)                           AS last_height
		FROM consumer_registry r
		LEFT JOIN consumer_consolidation c
		    ON c.consumer_name = r.consumer_name
		LEFT JOIN (
		    SELECT consumer_name,
		           COUNT(*) AS processed_count,
		           MAX(height) AS last_height
		    FROM processed_heights
		    GROUP BY consumer_name
		) ph ON ph.consumer_name = r.consumer_name
		ORDER BY r.consumer_name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query cursor rows: %w", err)
	}
	defer rows.Close()

	var out []CursorRow
	for rows.Next() {
		var cr CursorRow
		if err := rows.Scan(
			&cr.Name, &cr.FirstValidVersion, &cr.Active,
			&cr.ConsolidatedUpTo, &cr.ProcessedCount, &cr.LastHeight,
		); err != nil {
			return nil, fmt.Errorf("scan cursor row: %w", err)
		}
		out = append(out, cr)
	}
	return out, rows.Err()
}

// ── ps inspect streams ────────────────────────────────────────────────────────

func newStreamsCmd() *cobra.Command {
	var natsURL string
	cmd := &cobra.Command{
		Use:   "streams",
		Short: "Show JetStream stream state (messages, bytes, consumers, pending)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			client, err := psnats.Connect(ctx, natsURL)
			if err != nil {
				return fmt.Errorf("connect nats: %w", err)
			}
			defer client.Close()

			rows, err := queryStreamRows(ctx, client.JetStream())
			if err != nil {
				return err
			}
			fmt.Print(RenderStreams(rows))
			return nil
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", envOr("PS_NATS_URL", defaultNATSURL),
		"NATS server URL; overrides $PS_NATS_URL")
	return cmd
}

// queryStreamRows enumerates all JetStream streams and their consumers.
func queryStreamRows(ctx context.Context, js jetstream.JetStream) ([]StreamRow, error) {
	var rows []StreamRow

	names := js.StreamNames(ctx)
	for name := range names.Name() {
		si, err := js.Stream(ctx, name)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warn: stream %q info: %v\n", name, err)
			continue
		}
		info := si.CachedInfo()
		row := StreamRow{
			Name:     info.Config.Name,
			Msgs:     info.State.Msgs,
			Bytes:    info.State.Bytes,
			FirstSeq: info.State.FirstSeq,
			LastSeq:  info.State.LastSeq,
		}

		// Enumerate consumers for this stream.
		consumerList := si.ConsumerNames(ctx)
		for cname := range consumerList.Name() {
			ci, err := si.Consumer(ctx, cname)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "warn: consumer %q info: %v\n", cname, err)
				continue
			}
			cinfo := ci.CachedInfo()
			row.Consumers = append(row.Consumers, ConsumerInfo{
				Name:     cinfo.Name,
				Pending:  cinfo.NumPending,
				AckFloor: cinfo.AckFloor.Stream,
			})
		}
		rows = append(rows, row)
	}
	if err := names.Err(); err != nil {
		return nil, fmt.Errorf("list stream names: %w", err)
	}

	return rows, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
