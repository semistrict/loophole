package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

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
		sbCompactCmd(c),
		sbCheckpointCmd(c),
		sbCheckpointsCmd(c),
		sbCloneCmd(c),
		sbFreezeCmd(c),
		sbInfoCmd(c),
	)

	return root
}

func sbStatusCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show volume status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			status, err := c.Status(cmd.Context())
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(status)
		},
	}
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

func sbCompactCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "compact",
		Short: "Compact the volume (L0 → L1)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.CompactVolume(cmd.Context(), ""); err != nil {
				return err
			}
			fmt.Println("compacted")
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

func sbFreezeCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "freeze",
		Short: "Make the current volume permanently immutable",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.Freeze(cmd.Context(), ""); err != nil {
				return err
			}
			fmt.Println("frozen")
			return nil
		},
	}
}

func sbInfoCmd(c *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show volume metadata",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := c.VolumeInfo(cmd.Context(), "")
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(info)
		},
	}
}
