// Package deregister is the composition root for the ps deregister-consumer subcommand.
package deregister

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/store"
)

// defaultDSN matches the Tilt-managed dev Postgres (see Makefile DEV_PG_DSN).
const defaultDSN = "host=localhost port=5432 user=pocketscribe password=dev_only_password dbname=pocketscribe sslmode=disable"

// NewCmd builds `ps deregister-consumer <name>`.
func NewCmd() *cobra.Command {
	var dsn string
	cmd := &cobra.Command{
		Use:   "deregister-consumer <name>",
		Short: "Decommission a consumer: flip active=false and remove it from the required set",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			s, err := store.New(c.Context(), dsn)
			if err != nil {
				return err
			}
			defer s.Close()
			changed, err := s.DeregisterConsumer(c.Context(), args[0])
			if err != nil {
				return err
			}
			if changed {
				_, err = fmt.Fprintf(c.OutOrStdout(), "consumer %q deregistered\n", args[0])
			} else {
				_, err = fmt.Fprintf(c.OutOrStdout(), "consumer %q was not active; no change\n", args[0])
			}
			return err
		},
	}
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
