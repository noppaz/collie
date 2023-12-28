package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	rowGroupsCmd := &cobra.Command{
		Use:   "row-groups",
		Short: "Show row group metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.RowGroupsCommand(filename)
		},
	}

	rootCmd.AddCommand(rowGroupsCmd)
}
