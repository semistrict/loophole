//go:build linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
)

func addPlatformCommands(root *cobra.Command) {
	root.AddCommand(
		stopCmd(),
		createCmd(),
		lsCmd(),
		mountCmd(),
		unmountCmd(),
		snapshotCmd(),
		cloneCmd(),
		statusCmd(),
		deviceCmd(),
	)
}

func startDaemon(ctx context.Context, inst loophole.Instance, dir loophole.Dir, debug bool, socketMode os.FileMode, mode loophole.Mode, s3opts *loophole.S3Options) error {
	d, err := daemon.Start(ctx, inst, dir, debug, true, socketMode, mode, s3opts)
	if err != nil {
		return err
	}
	return d.Serve(ctx)
}

func startDaemonBackground(inst loophole.Instance, dir loophole.Dir, mode loophole.Mode, unprivileged bool) error {
	c := client.New(dir, inst)
	c.Bin = selfBin
	c.Sudo = !unprivileged
	c.Mode = mode
	if err := c.EnsureDaemon(); err != nil {
		return err
	}
	fmt.Println("daemon started")
	return nil
}

func stopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop [s3url]",
		Short: "Stop the daemon",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, _, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if err := c.Shutdown(cmd.Context()); err != nil {
				return err
			}
			fmt.Println("daemon stopped")
			return nil
		},
	}
}

func createCmd() *cobra.Command {
	var mountpoint string
	cmd := &cobra.Command{
		Use:   "create [s3url] <volume>",
		Short: "Create and format a new volume",
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
			ctx := cmd.Context()
			volume := rest[0]
			if err := c.Create(ctx, volume); err != nil {
				return err
			}
			fmt.Printf("created volume %s\n", volume)
			if mountpoint != "" {
				if err := c.Mount(ctx, volume, mountpoint); err != nil {
					return err
				}
				fmt.Printf("mounted %s at %s\n", volume, mountpoint)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&mountpoint, "mount", "m", "", "mount the volume at this path after creation")
	return cmd
}

func lsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls [s3url]",
		Short: "List all volumes",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, _, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			volumes, err := c.ListVolumes(ctx)
			if err != nil {
				return err
			}

			// Build volume→mountpoint map from status.
			mounted := map[string]string{}
			if status, err := c.Status(ctx); err == nil {
				for mp, vol := range status.Mounts {
					mounted[vol] = mp
				}
			}

			green := color.New(color.FgGreen)
			dim := color.New(color.FgHiBlack)
			for _, v := range volumes {
				if mp, ok := mounted[v]; ok {
					green.Print(v)
					dim.Printf("  %s\n", mp)
				} else {
					fmt.Println(v)
				}
			}
			return nil
		},
	}
}

func mountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mount [s3url] <volume> <mountpoint>",
		Short: "Mount a volume as ext4",
		Long:  "When only one daemon is running, s3url can be omitted.",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, rest, err := resolveClientArgs(args)
			if err != nil {
				return err
			}
			if len(rest) != 2 {
				return fmt.Errorf("expected volume and mountpoint, got %d args", len(rest))
			}
			if err := c.Mount(cmd.Context(), rest[0], rest[1]); err != nil {
				return err
			}
			fmt.Printf("mounted %s at %s\n", rest[0], rest[1])
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
		Use:   "snapshot [mountpoint] <name>",
		Short: "Create a snapshot",
		Long:  "If run from within a loophole mount, the mountpoint can be omitted.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var mountpoint, name string
			if len(args) == 2 {
				mountpoint, name = args[0], args[1]
			} else {
				mp, err := detectMountpoint()
				if err != nil {
					return fmt.Errorf("no mountpoint given and %w", err)
				}
				mountpoint, name = mp, args[0]
			}
			sock, err := socketFromMountpoint(mountpoint)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(sock)
			if err := c.Snapshot(cmd.Context(), mountpoint, name); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", name)
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

// detectMountpoint walks up from the current working directory to find
// a loophole mountpoint (one that has a registered mount symlink).
func detectMountpoint() (string, error) {
	dir := loophole.DefaultDir()
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	path := cwd
	for {
		symPath := dir.MountSymlink(path)
		if _, err := os.Readlink(symPath); err == nil {
			return path, nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}
	return "", fmt.Errorf("not inside a loophole mount (checked from %s)", cwd)
}
