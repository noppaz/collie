package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	var amount int
	headCommand := &cobra.Command{
		Use:   "head",
		Short: "Show N sample rows from row group zero",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.HeadCommand(filename, amount)
		},
	}

	headCommand.Flags().IntVarP(&amount, "rows", "n", 10, "Amount of rows to print")

	rootCmd.AddCommand(headCommand)
}
