package cmd

import (
	"github.com/noppaz/collie/internal/commands"
	"github.com/spf13/cobra"
)

func init() {
	var amount int
	lessCommand := &cobra.Command{
		Use:   "less",
		Short: "Scroll through N values from the file",
		Long:  "Enables file row exploration through a less-like interface to explore the file's rows.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]
			return commands.LessCommand(filename, amount)
		},
	}

	lessCommand.Flags().IntVarP(&amount, "rows", "n", 1000, "Amount of rows to load to the buffer")

	rootCmd.AddCommand(lessCommand)
}
