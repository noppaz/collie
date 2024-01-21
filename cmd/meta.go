package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	metaCmd := &cobra.Command{
		Use:   "meta",
		Short: "Show file metadata",
		Long:  "Prints overall file statistics as well as the column metadata of the file.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.MetaCommand(filename)
		},
	}

	rootCmd.AddCommand(metaCmd)
}
