package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/client"
)

const chrootSocketPath = "/.loophole"

// inChroot returns true if running inside a loophole chroot (/.loophole socket exists).
func inChroot() bool {
	fi, err := os.Stat(chrootSocketPath)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSocket != 0
}

func chrootRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "Loophole volume control (chroot mode)",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(
		chrootFlushCmd(),
		chrootSnapshotCmd(),
		chrootCloneCmd(),
		chrootStatusCmd(),
	)

	return root
}

func chrootClient() *client.Client {
	return client.NewFromSocket(chrootSocketPath)
}

func chrootFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush",
		Short: "Flush volume data to S3",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			if err := c.Flush(cmd.Context()); err != nil {
				return err
			}
			fmt.Println("flushed")
			return nil
		},
	}
}

func chrootSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot <name>",
		Short: "Create a snapshot of this volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			if err := c.ChrootSnapshot(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", args[0])
			return nil
		},
	}
}

func chrootCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <name>",
		Short: "Clone this volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			mp, err := c.ChrootClone(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Printf("cloned to %s (mountpoint %s)\n", args[0], mp)
			return nil
		},
	}
}

func chrootStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show volume status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			volume, err := c.ChrootStatus(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("volume: %s\n", volume)
			return nil
		},
	}
}
