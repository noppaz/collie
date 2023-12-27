package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	metaCmd := &cobra.Command{
		Use:   "meta",
		Short: "Show file overall metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.MetaCommand(filename)
		},
	}

	rootCmd.AddCommand(metaCmd)
}
