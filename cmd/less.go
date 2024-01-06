package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	var amount int
	lessCommand := &cobra.Command{
		Use:   "less",
		Short: "Show N sample rows from row group zero",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.LessCommand(filename, amount)
		},
	}

	lessCommand.Flags().IntVarP(&amount, "rows", "n", 1000, "Amount of rows to print")

	rootCmd.AddCommand(lessCommand)
}
