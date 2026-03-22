//go:build linux

package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/internal/client"
)

func addVsockCommands(root *cobra.Command) {
	root.AddCommand(
		vsockCheckpointCmd(),
		vsockFlushCmd(),
		vsockStatsCmd(),
	)
}

func vsockCheckpointCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint",
		Short: "Create a checkpoint of the loophole volume",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.NewVsock()
			cpID, err := c.Checkpoint()
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint %s created\n", cpID)
			return nil
		},
	}
}

func vsockFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush",
		Short: "Flush dirty pages to the backing store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.NewVsock()
			return c.Flush()
		},
	}
}

func vsockStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show volume debug info",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.NewVsock()
			data, err := c.Stats()
			if err != nil {
				return err
			}
			var pretty json.RawMessage
			indented, err := json.MarshalIndent(json.RawMessage(data), "", "  ")
			if err != nil {
				// Fall back to raw output.
				fmt.Println(string(data))
				return nil
			}
			_ = pretty
			fmt.Println(string(indented))
			return nil
		},
	}
}
