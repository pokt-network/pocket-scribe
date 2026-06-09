package app

import "github.com/spf13/cobra"

// NewRootCmd builds the `ps` command tree. cmd/ps/main.go executes it.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "ps",
		Short:         "PocketScribe — a Go-native indexer for Pocket Network's Shannon protocol",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newVersionCmd())
	return root
}
