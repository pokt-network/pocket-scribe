package migrate

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// defaultDSN matches the Tilt-managed dev Postgres (see Makefile DEV_PG_DSN).
const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds `ps migrate {up,down,status}`.
func NewCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply, roll back, or inspect schema migrations (goose)",
	}
	cmd.PersistentFlags().StringVar(&dsn, "dsn", envOr("PS_DATABASE_DSN", defaultDSN),
		"Postgres DSN (libpq keyword or URL); overrides $PS_DATABASE_DSN")

	for _, sub := range []struct{ use, short, cmd string }{
		{"up", "Apply all pending migrations", "up"},
		{"down", "Roll back the most recent migration", "down"},
		{"status", "Show migration status", "status"},
	} {
		cmd.AddCommand(&cobra.Command{
			Use:   sub.use,
			Short: sub.short,
			Args:  cobra.NoArgs,
			RunE: func(c *cobra.Command, _ []string) error {
				return store.Migrate(c.Context(), dsn, sub.cmd)
			},
		})
	}
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
