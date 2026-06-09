package app

import (
	"github.com/spf13/cobra"

	"github.com/pokt-network/pocketscribe/internal/app/deregister"
	"github.com/pokt-network/pocketscribe/internal/app/migrate"
)

// NewRootCmd builds the `ps` command tree. cmd/ps/main.go executes it.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ps",
		Short:         "PocketScribe — a Go-native indexer for Pocket Network's Shannon protocol",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd())
	root.AddCommand(migrate.NewCmd())
	root.AddCommand(deregister.NewCmd())
	return root
}
