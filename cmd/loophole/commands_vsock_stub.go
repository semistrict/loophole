//go:build !linux

package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func addVsockCommands(root *cobra.Command) {
	root.RunE = func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("vsock mode is only supported on Linux")
	}
}
