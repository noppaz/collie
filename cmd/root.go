package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "v0.1.2"

var SilentErr = errors.New("SilentErr")

var rootCmd = &cobra.Command{
	Use:           "collie",
	SilenceErrors: true,
	SilenceUsage:  true,
	Version:       version,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		if err != SilentErr {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func init() {
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		cmd.Println(err)
		cmd.Println(cmd.UsageString())
		return SilentErr
	})
}
