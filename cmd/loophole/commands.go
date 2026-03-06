package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
	"github.com/semistrict/loophole/filecmd"
)

func addCommands(root *cobra.Command) {
	root.AddCommand(
		stopCmd(),
		createCmd(),
		deleteCmd(),
		lsCmd(),
		mountCmd(),
		unmountCmd(),
		snapshotCmd(),
		cloneCmd(),
		statusCmd(),
		statsCmd(),
		deviceCmd(),
		fileCmd(),
		dbCmd(),
		breakLeaseCmd(),
	)
}

func startDaemon(ctx context.Context, inst loophole.Instance, dir loophole.Dir, socketMode os.FileMode) error {
	d, err := daemon.Start(ctx, inst, dir, true, socketMode)
	if err != nil {
		return err
	}
	return d.Serve(ctx)
}

func stopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the daemon",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClientOnly()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			status, _ := c.Status(ctx)
			if err := c.Shutdown(ctx); err != nil {
				return fmt.Errorf("no daemon running (socket %s)", c.Socket())
			}
			if status != nil {
				fmt.Printf("loophole stopped (%s)\n", status.S3)
			} else {
				fmt.Println("loophole stopped")
			}
			return nil
		},
	}
}

func createCmd() *cobra.Command {
	var mountpoint string
	var sizeStr string
	var noFormat bool
	var volType string
	cmd := &cobra.Command{
		Use:   "create <volume>",
		Short: "Create and format a new volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			var size uint64
			if sizeStr != "" {
				size, err = parseSize(sizeStr)
				if err != nil {
					return err
				}
			}
			ctx := cmd.Context()
			volume := args[0]
			if err := c.Create(ctx, client.CreateParams{
				Volume:   volume,
				Size:     size,
				NoFormat: noFormat,
				Type:     volType,
			}); err != nil {
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
	cmd.Flags().StringVarP(&sizeStr, "size", "s", "", "volume size (e.g. 100GB, 1TB, 512MB); default 100GB")
	cmd.Flags().BoolVar(&noFormat, "no-format", false, "create the volume without formatting")
	cmd.Flags().StringVarP(&volType, "type", "t", "", "volume/filesystem type (ext4, xfs); default from LOOPHOLE_DEFAULT_FS or ext4")
	return cmd
}

func deleteCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <volume>",
		Short: "Delete a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			volume := args[0]
			if !yes {
				fmt.Printf("Delete volume %q? [y/N] ", volume)
				var answer string
				if _, err := fmt.Scanln(&answer); err != nil {
					slog.Warn("read input", "error", err)
				}
				if answer != "y" && answer != "Y" {
					fmt.Println("aborted")
					return nil
				}
			}
			if err := c.Delete(cmd.Context(), volume); err != nil {
				return err
			}
			fmt.Printf("deleted volume %s\n", volume)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompt")
	return cmd
}

func lsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List all volumes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
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
					_, _ = green.Print(v)
					_, _ = dim.Printf("  %s\n", mp)
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
		Use:   "mount <volume> [mountpoint]",
		Short: "Mount a volume as ext4",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			volume := args[0]
			mountpoint := ""
			if len(args) == 2 {
				mountpoint = args[1]
			}
			if err := c.Mount(cmd.Context(), volume, mountpoint); err != nil {
				return err
			}
			if mountpoint != "" {
				fmt.Printf("mounted %s at %s\n", volume, mountpoint)
			} else {
				fmt.Printf("mounted %s\n", volume)
			}
			return nil
		},
	}
}

func unmountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unmount <mountpoint|volume>",
		Short: "Unmount an ext4 filesystem",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Try symlink-based lookup first (real mountpoints).
			if sock, err := socketFromMountpoint(args[0]); err == nil {
				c := client.NewFromSocket(sock)
				if err := c.Unmount(cmd.Context(), args[0]); err != nil {
					return err
				}
				fmt.Printf("unmounted %s\n", args[0])
				return nil
			}
			// Fall back to profile-based client.
			c, err := resolveClient()
			if err != nil {
				return err
			}
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
		Use:   "status",
		Short: "Show daemon status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClientOnly()
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

// --- File subcommands ---

func fileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "file",
		Short: "File operations on mounted volumes",
	}
	for _, reg := range filecmd.Commands {
		cmd.AddCommand(makeFileSubcmd(reg))
	}
	return cmd
}

func makeFileSubcmd(reg filecmd.Command) *cobra.Command {
	return &cobra.Command{
		Use:                reg.Usage,
		Short:              reg.Short,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			args = extractProfileFlag(args)

			for _, a := range args {
				if a == "-h" || a == "--help" {
					return cmd.Help()
				}
			}

			c, err := resolveClient()
			if err != nil {
				return err
			}

			// Send argv as [commandName, args...] to the server.
			argv := append([]string{reg.Name}, args...)
			result, err := c.File(cmd.Context(), argv)
			if err != nil {
				return err
			}
			defer func() { _ = result.Close() }()

			code, err := result.Demux(os.Stdout, os.Stderr)
			if err != nil {
				return err
			}
			if code != 0 {
				os.Exit(int(code))
			}
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
		deviceAttachCmd(),
		deviceDetachCmd(),
		deviceSnapshotCmd(),
		deviceCloneCmd(),
	)

	return cmd
}

func deviceStartCmd() *cobra.Command {
	cmd := startCmd()
	cmd.Short = "Start daemon in foreground"
	// device start is always foreground — hide the flag and set it.
	_ = cmd.Flags().MarkHidden("foreground")
	_ = cmd.Flags().Set("foreground", "true")
	return cmd
}

func deviceAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <volume>",
		Short: "Attach a volume device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			device, err := c.DeviceAttach(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Println(device)
			return nil
		},
	}
}

func deviceDetachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "detach <volume>",
		Short: "Detach a volume device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			return c.DeviceDetach(cmd.Context(), args[0])
		},
	}
}

func deviceSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot <volume> <name>",
		Short: "Snapshot a volume device",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.DeviceSnapshot(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", args[1])
			return nil
		},
	}
}

func deviceCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <volume> <name>",
		Short: "Clone a volume device",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			device, err := c.DeviceClone(cmd.Context(), args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println(device)
			return nil
		},
	}
}

// --- helpers ---

// parseSize parses a human-readable size string like "100GB", "1TB", "512MB".
func parseSize(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	// Check longest suffixes first so "GB" matches before "B".
	type sizeSuffix struct {
		suffix string
		mult   uint64
	}
	suffixes := []sizeSuffix{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}
	upper := strings.ToUpper(s)
	for _, ss := range suffixes {
		if strings.HasSuffix(upper, ss.suffix) && len(upper) > len(ss.suffix) {
			numStr := s[:len(s)-len(ss.suffix)]
			n, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q: %w", s, err)
			}
			return n * ss.mult, nil
		}
	}
	// Try plain number (bytes).
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (use e.g. 100GB, 1TB, 512MB)", s)
	}
	return n, nil
}

// extractProfileFlag strips -p/--profile from args and sets globalProfile.
// Needed for subcommands with DisableFlagParsing where cobra can't handle
// persistent flags.
func extractProfileFlag(args []string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if (args[i] == "-p" || args[i] == "--profile") && i+1 < len(args) {
			globalProfile = args[i+1]
			i++ // skip value
			continue
		}
		if strings.HasPrefix(args[i], "-p=") {
			globalProfile = args[i][3:]
			continue
		}
		if strings.HasPrefix(args[i], "--profile=") {
			globalProfile = args[i][10:]
			continue
		}
		out = append(out, args[i])
	}
	return out
}

// resolveClientOnly returns a client for the current profile without
// starting the daemon. Used by commands like stop that shouldn't auto-start.
func resolveClientOnly() (*client.Client, error) {
	dir := loophole.DefaultDir()
	inst, err := resolveProfile(dir)
	if err != nil {
		return nil, err
	}
	return client.New(dir, inst), nil
}

// resolveClient returns a client for the current profile's daemon,
// auto-starting it if necessary.
func resolveClient() (*client.Client, error) {
	dir := loophole.DefaultDir()
	inst, err := resolveProfile(dir)
	if err != nil {
		return nil, err
	}
	c := client.New(dir, inst)
	c.Bin = selfBin
	c.Sudo = runtime.GOOS == "linux" && inst.Mode.NeedsRoot()
	c.Profile = inst.ProfileName
	if err := c.EnsureDaemon(); err != nil {
		return nil, err
	}
	return c, nil
}

func breakLeaseCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "break-lease <volume>",
		Short: "Request the lease holder to release a volume",
		Long:  "Sends a release request to the remote lease holder. With -f, forcibly clears the lease if the holder doesn't respond.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			volume := args[0]
			graceful, err := c.BreakLease(cmd.Context(), volume, force)
			if err != nil {
				return err
			}
			if graceful {
				fmt.Printf("lease released gracefully for volume %q\n", volume)
			} else {
				fmt.Printf("lease forcibly broken for volume %q (holder did not respond)\n", volume)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&force, "force", "f", false, "forcibly clear the lease if the holder doesn't respond")
	return cmd
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
