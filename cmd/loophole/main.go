//go:build linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "S3-backed FUSE filesystem with instant copy-on-write clones",
		Long:  "Loophole exposes a virtual block device backed by S3 with instant snapshots and clones.",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage: true,
	}

	root.AddCommand(
		startCmd(),
		createCmd(),
		mountCmd(),
		unmountCmd(),
		snapshotCmd(),
		cloneCmd(),
		statusCmd(),
		deviceCmd(),
	)

	return root
}

// --- High-level commands ---

func startCmd() *cobra.Command {
	var daemonize bool
	var debug bool

	cmd := &cobra.Command{
		Use:   "start <s3url>",
		Short: "Start the loophole daemon",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, err := loophole.ParseInstance(args[0])
			if err != nil {
				return err
			}
			dir := loophole.DefaultDir()

			if daemonize {
				return client.EnsureDaemon(dir, inst, "")
			}

			ctx := context.Background()
			d, err := daemon.Start(ctx, inst, dir, debug, nil)
			if err != nil {
				return err
			}
			return d.Serve(ctx)
		},
	}

	cmd.Flags().BoolVarP(&daemonize, "daemon", "d", false, "Run in background")
	cmd.Flags().BoolVar(&debug, "debug", false, "Enable debug logging")

	return cmd
}

func createCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <s3url> <volume>",
		Short: "Create and format a new volume",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, err := loophole.ParseInstance(args[0])
			if err != nil {
				return err
			}
			dir := loophole.DefaultDir()

			if err := client.EnsureDaemon(dir, inst, ""); err != nil {
				return err
			}

			c := client.New(dir, inst)
			if err := c.Create(cmd.Context(), args[1]); err != nil {
				return err
			}
			fmt.Printf("created volume %s\n", args[1])
			return nil
		},
	}
}

func mountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mount <s3url> <volume> <mountpoint>",
		Short: "Mount a volume as ext4",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, err := loophole.ParseInstance(args[0])
			if err != nil {
				return err
			}
			dir := loophole.DefaultDir()

			if err := client.EnsureDaemon(dir, inst, ""); err != nil {
				return err
			}

			c := client.New(dir, inst)
			if err := c.Mount(cmd.Context(), args[1], args[2]); err != nil {
				return err
			}
			fmt.Printf("mounted %s at %s\n", args[1], args[2])
			return nil
		},
	}
}

func unmountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unmount <mountpoint>",
		Short: "Unmount an ext4 filesystem",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := socketFromMountpoint(args[0])
			if err != nil {
				return err
			}

			c := client.NewFromSocket(sock)
			if err := c.Unmount(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("unmounted %s\n", args[0])
			return nil
		},
	}
}

func snapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot <mountpoint> <name>",
		Short: "Create a snapshot",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := socketFromMountpoint(args[0])
			if err != nil {
				return err
			}

			c := client.NewFromSocket(sock)
			if err := c.Snapshot(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", args[1])
			return nil
		},
	}
}

func cloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <mountpoint> <name> <clone_mountpoint>",
		Short: "Clone a volume and mount it",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := socketFromMountpoint(args[0])
			if err != nil {
				return err
			}

			c := client.NewFromSocket(sock)
			if err := c.Clone(cmd.Context(), args[0], args[1], args[2]); err != nil {
				return err
			}
			fmt.Printf("cloned to %s at %s\n", args[1], args[2])
			return nil
		},
	}
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status [s3url]",
		Short: "Show daemon status",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, _, err := resolveClientArgs(args)
			if err != nil {
				return err
			}

			status, err := c.Status(cmd.Context())
			if err != nil {
				return err
			}
			data, err := json.MarshalIndent(status, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal status: %w", err)
		}
			fmt.Println(string(data))
			return nil
		},
	}
}

// --- Device subcommands ---

func deviceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "device",
		Short: "Low-level device commands",
		Long:  "Low-level commands that operate on block devices directly, without ext4 mount management.",
	}

	cmd.AddCommand(
		deviceStartCmd(),
		deviceMountCmd(),
		deviceUnmountCmd(),
		deviceSnapshotCmd(),
		deviceCloneCmd(),
	)

	return cmd
}

func deviceStartCmd() *cobra.Command {
	cmd := startCmd()
	cmd.Short = "Start daemon in foreground"
	// Remove the --daemon flag — device start is always foreground.
	cmd.Flags().MarkHidden("daemon")
	return cmd
}

func deviceMountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mount [s3url] <volume>",
		Short: "Open a volume device",
		Long:  "When only one daemon is running, s3url can be omitted.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, rest, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if len(rest) != 1 {
				return fmt.Errorf("expected exactly 1 volume name, got %d", len(rest))
			}
			device, err := c.DeviceMount(cmd.Context(), rest[0])
			if err != nil {
				return err
			}
			fmt.Println(device)
			return nil
		},
	}
}

func deviceUnmountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unmount [s3url] <volume>",
		Short: "Close a volume device",
		Long:  "When only one daemon is running, s3url can be omitted.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, rest, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if len(rest) != 1 {
				return fmt.Errorf("expected exactly 1 volume name, got %d", len(rest))
			}
			return c.DeviceUnmount(cmd.Context(), rest[0])
		},
	}
}

func deviceSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot [s3url] <volume> <name>",
		Short: "Snapshot a volume device",
		Long:  "When only one daemon is running, s3url can be omitted.",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, rest, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if len(rest) != 2 {
				return fmt.Errorf("expected volume and snapshot name, got %d args", len(rest))
			}
			if err := c.DeviceSnapshot(cmd.Context(), rest[0], rest[1]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", rest[1])
			return nil
		},
	}
}

func deviceCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone [s3url] <volume> <name>",
		Short: "Clone a volume device",
		Long:  "When only one daemon is running, s3url can be omitted.",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, rest, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if len(rest) != 2 {
				return fmt.Errorf("expected volume and clone name, got %d args", len(rest))
			}
			device, err := c.DeviceClone(cmd.Context(), rest[0], rest[1])
			if err != nil {
				return err
			}
			fmt.Println(device)
			return nil
		},
	}
}

// --- helpers ---

// resolveClientArgs tries to parse args[0] as an S3 URL. If it looks like one,
// returns a client for it and the remaining args. Otherwise, falls back to
// auto-detecting the single running daemon and returns all args unchanged.
func resolveClientArgs(args []string) (*client.Client, []string, error) {
	dir := loophole.DefaultDir()
	if len(args) > 0 {
		if inst, parseErr := loophole.ParseInstance(args[0]); parseErr == nil {
			return client.New(dir, inst), args[1:], nil
		}
	}
	sock, err := dir.FindSocket()
	if err != nil {
		return nil, nil, err
	}
	return client.NewFromSocket(sock), args, nil
}

func socketFromMountpoint(mountpoint string) (string, error) {
	dir := loophole.DefaultDir()
	symPath := dir.MountSymlink(mountpoint)
	target, err := os.Readlink(symPath)
	if err != nil {
		return "", fmt.Errorf("cannot find daemon for mountpoint %q (no symlink at %s)", mountpoint, symPath)
	}
	return target, nil
}
