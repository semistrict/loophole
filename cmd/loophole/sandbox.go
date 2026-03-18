package main

import (
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/client"
)

func sandboxRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "Loophole volume control (sandbox mode)",
		Long:  "Control the current volume from inside a sandbox.\nDetected sandbox mode via /.loophole directory.",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	c := client.NewFromSocket(sandboxSocketPath)

	root.AddCommand(
		sbStatusCmd(c),
		sbFlushCmd(c),
		sbCheckpointCmd(c),
		sbCheckpointsCmd(c),
		sbCloneCmd(c),
	)

	return root
}

var (
	sbHeader = color.New(color.Bold, color.Underline)
	sbLabel  = color.New(color.FgCyan)
	sbDim    = color.New(color.Faint)
)

func sbStatusCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show volume status and metrics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			status, err := c.Status(ctx)
			if err != nil {
				return err
			}

			// Volume info
			_, _ = sbHeader.Println("Volume")
			printKV("name", status.Volume)
			printKV("state", status.State)
			printKV("mountpoint", status.Mountpoint)
			printKV("s3", status.S3)
			fmt.Println()

			// Metrics
			raw, err := c.Metrics(ctx)
			if err != nil {
				_, _ = sbDim.Printf("  (metrics unavailable: %v)\n", err)
				return nil
			}
			families, errParse := parseMetrics(raw)
			if errParse != nil {
				_, _ = sbDim.Printf("  (metrics parse error: %v)\n", errParse)
				return nil
			}
			printStats(families)

			return nil
		},
	}
}

func printKV(key, value string) {
	if value == "" {
		return
	}
	_, _ = sbLabel.Printf("  %-12s ", key)
	fmt.Println(value)
}

func sbFlushCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "flush",
		Short: "Flush volume data to S3",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.FlushVolume(cmd.Context(), ""); err != nil {
				return err
			}
			fmt.Println("flushed")
			return nil
		},
	}
}

func sbCheckpointCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint",
		Short: "Create a checkpoint of the current volume",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cpID, err := c.Checkpoint(cmd.Context(), "")
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint: %s\n", cpID)
			return nil
		},
	}
}

func sbCheckpointsCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoints",
		Short: "List checkpoints",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cps, err := c.ListCheckpoints(cmd.Context(), "")
			if err != nil {
				return err
			}
			for _, cp := range cps {
				fmt.Printf("%s  %s\n", cp.ID, cp.CreatedAt.Format(time.RFC3339))
			}
			return nil
		},
	}
}

func sbCloneCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "clone <name>",
		Short: "Create a clone of the current volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.Clone(cmd.Context(), client.CloneParams{Clone: args[0]}); err != nil {
				return err
			}
			fmt.Printf("cloned: %s\n", args[0])
			return nil
		},
	}
}
