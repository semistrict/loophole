package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/fsserver"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage"
)

func addCommands(root *cobra.Command) {
	root.AddCommand(
		formatCmd(),
		shutdownCmd(),
		createCmd(),
		deleteCmd(),
		lsCmd(),
		mountCmd(),
		checkpointCmd(),
		cloneCmd(),
		checkpointsCmd(),
		statusCmd(),
		statsCmd(),
		deviceCmd(),
		breakLeaseCmd(),
		s3testCmd(),
		gcCmd(),
	)
}

func formatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "format",
		Short: "Format the backing store by writing the top-level loophole descriptor",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := env.DefaultDir()
			inst, err := resolveProfile(dir)
			if err != nil {
				return err
			}
			store, err := storage.OpenStoreForProfile(cmd.Context(), inst)
			if err != nil {
				return err
			}
			desc, created, err := storage.FormatVolumeSet(cmd.Context(), store)
			if err != nil {
				return err
			}
			if created {
				fmt.Printf("formatted store volset_id=%s page_size=%d\n", desc.VolsetID, desc.PageSize)
				return nil
			}
			fmt.Printf("store already formatted volset_id=%s page_size=%d\n", desc.VolsetID, desc.PageSize)
			return nil
		},
	}
}

func shutdownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shutdown <mountpoint|volume|device>",
		Short: "Shut down a mounted or attached volume owner",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sock, err := socketFromTarget(args[0])
			if err != nil {
				return err
			}
			c := client.NewFromSocket(sock)
			ctx := cmd.Context()
			if err := c.Shutdown(ctx); err != nil {
				return err
			}
			if err := c.ShutdownWait(ctx); err != nil {
				return err
			}
			fmt.Printf("shutdown %s\n", args[0])
			return nil
		},
	}
}

func createCmd() *cobra.Command {
	var mountpoint string
	var sizeStr string
	var noFormat bool
	cmd := &cobra.Command{
		Use:   "create <volume>",
		Short: "Create and mount a new volume in the foreground",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var size uint64
			if sizeStr != "" {
				var err error
				size, err = parseSize(sizeStr)
				if err != nil {
					return err
				}
			}
			volume := args[0]
			inst, dir, opts, err := resolveOwnerOpts(volume)
			if err != nil {
				return err
			}
			return fsserver.CreateFSAndServe(cmd.Context(), inst, dir, storage.CreateParams{
				Volume:   volume,
				Size:     size,
				NoFormat: noFormat,
			}, mountpoint, opts)
		},
	}
	cmd.Flags().StringVarP(&mountpoint, "mount", "m", "", "mount the volume at this path (default: volume name)")
	cmd.Flags().StringVarP(&sizeStr, "size", "s", "", "volume size (e.g. 100GB, 1TB, 512MB); default 100GB")
	cmd.Flags().BoolVar(&noFormat, "no-format", false, "create the volume without formatting")
	return cmd
}

func deleteCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <volume>",
		Short: "Delete a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			vm, cleanup, err := openDirectManager(cmd.Context())
			if err != nil {
				return err
			}
			defer cleanup()
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
			if err := storage.DeleteVolume(cmd.Context(), vm.Store(), volume); err != nil {
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
			vm, cleanup, err := openDirectManager(cmd.Context())
			if err != nil {
				return err
			}
			defer cleanup()
			ctx := cmd.Context()
			volumes, err := storage.ListAllVolumes(ctx, vm.Store())
			if err != nil {
				return err
			}

			green := color.New(color.FgGreen)
			dim := color.New(color.FgHiBlack)
			for _, v := range volumes {
				if sock := socketForVolume(v); sock != "" {
					_, _ = green.Print(v)
					_, _ = dim.Printf("  %s\n", sock)
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
		Short: "Mount a volume as ext4 in the foreground",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			volume := args[0]
			mountpoint := ""
			if len(args) == 2 {
				mountpoint = args[1]
			}
			inst, dir, opts, err := resolveOwnerOpts(volume)
			if err != nil {
				return err
			}
			return fsserver.MountFSAndServe(cmd.Context(), inst, dir, volume, mountpoint, opts)
		},
	}
}

func checkpointCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint [mountpoint]",
		Short: "Create a checkpoint of a mounted volume",
		Long:  "If run from within a loophole mount, the mountpoint can be omitted.",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var mountpoint string
			if len(args) == 1 {
				mountpoint = args[0]
			} else {
				mp, err := detectMountpoint()
				if err != nil {
					return fmt.Errorf("no mountpoint given and %w", err)
				}
				mountpoint = mp
			}
			sock, err := socketFromMountpoint(mountpoint)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(sock)
			cpID, err := c.Checkpoint(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint %s created\n", cpID)
			return nil
		},
	}
}

func checkpointsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoints <volume>",
		Short: "List checkpoints for a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			vm, cleanup, err := openDirectManager(cmd.Context())
			if err != nil {
				return err
			}
			defer cleanup()
			checkpoints, err := storage.ListCheckpoints(cmd.Context(), vm.Store(), args[0])
			if err != nil {
				return err
			}
			for _, cp := range checkpoints {
				fmt.Printf("%s  %s\n", cp.ID, cp.CreatedAt.Format(time.RFC3339))
			}
			return nil
		},
	}
}

func cloneCmd() *cobra.Command {
	var fromCheckpoint string
	cmd := &cobra.Command{
		Use:   "clone <mountpoint-or-volume> <name>",
		Short: "Create a clone volume",
		Long: `Create a clone from a live mounted volume, or from a checkpoint.

With --from-checkpoint, the first arg is the volume name (not mountpoint):
  loophole clone --from-checkpoint <checkpoint_id> <volume> <clone_name>`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromCheckpoint != "" {
				c, err := resolveOwnerClient(args[0])
				if err != nil {
					return err
				}
				if err := c.Clone(cmd.Context(), client.CloneParams{
					Checkpoint: fromCheckpoint,
					Clone:      args[1],
				}); err != nil {
					return err
				}
				fmt.Printf("created clone %s from %s@%s\n", args[1], args[0], fromCheckpoint)
				return nil
			}

			c, err := resolveOwnerClient(args[0])
			if err != nil {
				return err
			}
			if err := c.Clone(cmd.Context(), client.CloneParams{
				Clone: args[1],
			}); err != nil {
				return err
			}
			fmt.Printf("created clone %s\n", args[1])
			return nil
		},
	}
	cmd.Flags().StringVar(&fromCheckpoint, "from-checkpoint", "", "clone from a checkpoint ID instead of a live volume")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <mountpoint|volume|device>",
		Short: "Show owner process status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveOwnerClient(args[0])
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
		deviceCreateCmd(),
		deviceAttachCmd(),
		deviceCheckpointCmd(),
		deviceCloneCmd(),
		deviceDDCmd(),
		deviceFlushCmd(),
	)

	return cmd
}

func deviceCreateCmd() *cobra.Command {
	var sizeStr string
	cmd := &cobra.Command{
		Use:   "create <volume>",
		Short: "Create and attach a new raw volume in the foreground",
		Long:  "Create a fresh unformatted volume for raw block-device use and attach it immediately. --size is required.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			size, err := parseSize(sizeStr)
			if err != nil {
				return err
			}
			if size == 0 {
				return fmt.Errorf("--size is required for device create")
			}
			volume := args[0]
			inst, dir, opts, err := resolveOwnerOpts(volume)
			if err != nil {
				return err
			}
			return fsserver.CreateBlockDeviceAndServe(cmd.Context(), inst, dir, storage.CreateParams{
				Volume: volume,
				Size:   size,
			}, opts)
		},
	}
	cmd.Flags().StringVarP(&sizeStr, "size", "s", "", "volume size (e.g. 100GB, 1TB, 512MB)")
	return cmd
}

func deviceAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <volume>",
		Short: "Attach a volume device in the foreground",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			volume := args[0]
			inst, dir, opts, err := resolveOwnerOpts(volume)
			if err != nil {
				return err
			}
			return fsserver.AttachBlockDeviceAndServe(cmd.Context(), inst, dir, volume, opts)
		},
	}
}

func deviceCheckpointCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint <volume|device>",
		Short: "Checkpoint a volume device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveOwnerClient(args[0])
			if err != nil {
				return err
			}
			cpID, err := c.DeviceCheckpoint(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint %s created\n", cpID)
			return nil
		},
	}
}

func deviceCloneCmd() *cobra.Command {
	var fromCheckpoint string
	cmd := &cobra.Command{
		Use:   "clone <volume> <name>",
		Short: "Clone a volume device",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveOwnerClient(args[0])
			if err != nil {
				return err
			}
			if err := c.DeviceClone(cmd.Context(), client.DeviceCloneParams{
				Checkpoint: fromCheckpoint,
				Clone:      args[1],
			}); err != nil {
				return err
			}
			fmt.Printf("created clone %s\n", args[1])
			return nil
		},
	}
	cmd.Flags().StringVar(&fromCheckpoint, "from-checkpoint", "", "clone from a checkpoint ID instead of the open writable device")
	return cmd
}

func deviceFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush <volume>",
		Short: "Flush a volume's dirty pages to storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveOwnerClient(args[0])
			if err != nil {
				return err
			}
			if err := c.DeviceFlush(cmd.Context()); err != nil {
				return err
			}
			fmt.Printf("flushed volume %s\n", args[0])
			return nil
		},
	}
}

func deviceDDCmd() *cobra.Command {
	var ifFlag, ofFlag, bsFlag string

	cmd := &cobra.Command{
		Use:   "dd [if=<source>] [of=<dest>]",
		Short: "Copy raw data between files and volumes",
		Long: `Copy raw block data between local files and loophole volumes.
Use the volume: syntax to refer to a volume as a raw block device.

Import (file -> volume): writes the image into an already-attached volume.
Export (volume -> file): reads raw volume data and writes it to a file.

Examples:
  loophole device dd if=/path/to/rootfs.ext4 of=myvolume:
  loophole device dd if=myvolume: of=/path/to/output.img`,
		Args:                  cobra.ArbitraryArgs,
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse dd-style key=value args.
			for _, arg := range args {
				k, v, ok := strings.Cut(arg, "=")
				if !ok {
					return fmt.Errorf("unknown argument %q (expected key=value)", arg)
				}
				switch k {
				case "if":
					ifFlag = v
				case "of":
					ofFlag = v
				case "bs":
					bsFlag = v
				default:
					return fmt.Errorf("unknown parameter %q", k)
				}
			}

			if ifFlag == "" || ofFlag == "" {
				return fmt.Errorf("if and of are required (e.g. loophole device dd if=image.ext4 of=myvolume:)")
			}

			_ = bsFlag // reserved for future use

			srcVol, _, srcIsVol := parseVolPath(ifFlag)
			dstVol, _, dstIsVol := parseVolPath(ofFlag)

			if srcIsVol && dstIsVol {
				return fmt.Errorf("both if and of are volumes; at least one must be a local file")
			}
			if !srcIsVol && !dstIsVol {
				return fmt.Errorf("neither if nor of is a volume; use volume: syntax (e.g. of=myvolume:)")
			}

			if dstIsVol {
				c, err := resolveOwnerClient(dstVol)
				if err != nil {
					return err
				}
				return deviceDDImport(cmd, c, ifFlag, dstVol)
			}
			c, err := resolveOwnerClient(srcVol)
			if err != nil {
				return err
			}
			return deviceDDExport(cmd, c, srcVol, ofFlag)
		},
	}

	cmd.Flags().StringVar(&ifFlag, "if", "", "input: local file or volume:")
	cmd.Flags().StringVar(&ofFlag, "of", "", "output: local file or volume:")
	cmd.Flags().StringVar(&bsFlag, "bs", "", "block size (e.g. 1M, 4M); default 4M")

	return cmd
}

// deviceDDImport writes a local file into an already-attached volume.
func deviceDDImport(cmd *cobra.Command, c *client.Client, filePath, volume string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open image: %w", err)
	}
	defer util.SafeClose(f, "close dd import input")

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat image: %w", err)
	}
	size := uint64(fi.Size())
	fmt.Printf("Import: %s (%d bytes, %.1f GiB) -> %s\n",
		filePath, size, float64(size)/(1<<30), volume)

	return c.DeviceDDWriteExisting(cmd.Context(), volume, f, os.Stdout)
}

// deviceDDExport reads a volume's raw data into a local file.
func deviceDDExport(cmd *cobra.Command, c *client.Client, volume, filePath string) error {
	ctx := cmd.Context()

	size, err := c.DeviceDDSize(ctx)
	if err != nil {
		return fmt.Errorf("get volume size: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer util.SafeClose(f, "close dd export output")

	fmt.Printf("Export: %s (%d bytes, %.1f GiB) -> %s\n",
		volume, size, float64(size)/(1<<30), filePath)

	return c.DeviceDDRead(ctx, f, os.Stdout)
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

func parseVolPath(s string) (volume, path string, isVol bool) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", s, false
	}
	prefix := s[:i]
	if strings.ContainsRune(prefix, '/') {
		return "", s, false
	}
	return prefix, s[i+1:], true
}

// extractProfileFlag strips -p/--profile from args and sets globalProfile.
// Needed for subcommands with DisableFlagParsing where cobra can't handle
// persistent flags.

func breakLeaseCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "break-lease <volume>",
		Short: "Request the lease holder to release a volume",
		Long:  "Sends a release request to the remote lease holder. With -f, forcibly clears the lease if the holder doesn't respond.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			vm, cleanup, err := openDirectManager(cmd.Context())
			if err != nil {
				return err
			}
			defer cleanup()
			volume := args[0]
			graceful, err := storage.BreakLease(cmd.Context(), vm.Store(), volume, force)
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

func gcCmd() *cobra.Command {
	var dryRun, yes bool
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Delete orphaned layers that are no longer referenced by any volume or checkpoint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			vm, cleanup, err := openDirectManager(cmd.Context())
			if err != nil {
				return err
			}
			defer cleanup()

			// Always do a dry-run first to show what would be deleted.
			result, err := storage.GarbageCollect(cmd.Context(), vm.Store(), true, 0)
			if err != nil {
				return err
			}

			fmt.Printf("Reachable layers: %d\n", result.ReachableLayers)
			fmt.Printf("Orphaned layers:  %d\n", result.OrphanedLayers)
			if result.OrphanedLayers == 0 {
				fmt.Println("Nothing to do.")
				return nil
			}

			if dryRun {
				fmt.Println("Run without --dry-run to delete orphaned layers.")
				return nil
			}

			if !yes {
				fmt.Printf("Delete %d orphaned layers? [y/N] ", result.OrphanedLayers)
				var answer string
				if _, err := fmt.Scanln(&answer); err != nil {
					slog.Warn("read input", "error", err)
				}
				if answer != "y" && answer != "Y" {
					fmt.Println("aborted")
					return nil
				}
			}

			result, err = storage.GarbageCollect(cmd.Context(), vm.Store(), false, 0)
			if err != nil {
				return err
			}
			fmt.Printf("Deleted %d objects (%.1f MB freed)\n", result.DeletedObjects, float64(result.DeletedBytes)/(1024*1024))
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be deleted without deleting")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompt")
	return cmd
}

func socketFromMountpoint(mountpoint string) (string, error) {
	dir := env.DefaultDir()
	symPath := dir.MountSymlink(mountpoint)
	target, err := os.Readlink(symPath)
	if err != nil {
		return "", fmt.Errorf("cannot find owner for mountpoint %q (no symlink at %s)", mountpoint, symPath)
	}
	return target, nil
}

func socketFromDevice(devicePath string) (string, error) {
	dir := env.DefaultDir()
	symPath := dir.DeviceSymlink(devicePath)
	target, err := os.Readlink(symPath)
	if err != nil {
		return "", fmt.Errorf("cannot find owner for device %q (no symlink at %s)", devicePath, symPath)
	}
	return target, nil
}

func socketForVolume(volume string) string {
	dir := env.DefaultDir()
	sock := dir.VolumeSocket(volume)
	if _, err := os.Stat(sock); err == nil {
		return sock
	}
	return ""
}

func socketFromTarget(target string) (string, error) {
	if sock, err := socketFromMountpoint(target); err == nil {
		return sock, nil
	}
	if strings.HasPrefix(target, "/") {
		if sock, err := socketFromDevice(target); err == nil {
			return sock, nil
		}
	}
	if sock := socketForVolume(target); sock != "" {
		return sock, nil
	}
	return "", fmt.Errorf("cannot find running owner process for %q", target)
}

func resolveOwnerClient(target string) (*client.Client, error) {
	if globalPID != 0 {
		return client.NewFromSocket(fsserver.EmbedSocketPath(globalPID)), nil
	}
	sock, err := socketFromTarget(target)
	if err != nil {
		return nil, err
	}
	return client.NewFromSocket(sock), nil
}

// resolveOwnerOpts resolves profile, dir, and ServerOptions for an owner command.
// It also checks for an existing owner process on the same socket.
func resolveOwnerOpts(volume string) (env.ResolvedProfile, env.Dir, fsserver.ServerOptions, error) {
	dir := env.DefaultDir()
	inst, err := resolveProfile(dir)
	if err != nil {
		return env.ResolvedProfile{}, "", fsserver.ServerOptions{}, err
	}
	socketPath := dir.VolumeSocket(volume)
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return env.ResolvedProfile{}, "", fsserver.ServerOptions{}, err
	}
	c := client.NewFromSocket(socketPath)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Status(timeoutCtx); err == nil {
		return env.ResolvedProfile{}, "", fsserver.ServerOptions{}, fmt.Errorf("volume %q is already managed at %s", volume, socketPath)
	}
	return inst, dir, fsserver.ServerOptions{
		Foreground: true,
		SocketPath: socketPath,
	}, nil
}

func openDirectManager(ctx context.Context) (*storage.Manager, func(), error) {
	dir := env.DefaultDir()
	inst, err := resolveProfile(dir)
	if err != nil {
		return nil, nil, err
	}
	vm, err := storage.OpenManagerForProfile(ctx, inst, dir)
	if err != nil {
		return nil, nil, err
	}
	return vm, func() {
		util.SafeClose(vm, "close direct manager")
	}, nil
}

// detectMountpoint walks up from the current working directory to find
// a loophole mountpoint (one that has a registered mount symlink).
func detectMountpoint() (string, error) {
	dir := env.DefaultDir()
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
