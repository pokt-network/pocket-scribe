package consumer

import "github.com/spf13/cobra"

// NewCmd builds the `ps consumer` parent command.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "Run a module consumer (reads NATS, writes Postgres)",
	}
	cmd.AddCommand(newBlockCmd())
	return cmd
}
