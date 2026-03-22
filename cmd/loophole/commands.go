package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/cached/cachedserver"
	"github.com/semistrict/loophole/internal/client"
	"github.com/semistrict/loophole/internal/env"
	"github.com/semistrict/loophole/internal/fsserver"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/util"
)

type resolvedTarget struct {
	Store  env.ResolvedStore
	Volume string
	Socket string
}

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
		cachedCmd(),
	)
}

func formatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "format <store-url>",
		Short: "Format the backing store by writing the top-level loophole descriptor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, err := resolveStore(args[0])
			if err != nil {
				return err
			}
			store, err := blob.Open(cmd.Context(), inst)
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
		Use:   "shutdown <mountpoint|device> | shutdown <store-url> <volume>",
		Short: "Shut down a mounted or attached volume owner",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveRunningTargetArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(target.Socket)
			ctx := cmd.Context()
			if err := c.Shutdown(ctx); err != nil {
				return err
			}
			if err := c.ShutdownWait(ctx); err != nil {
				return err
			}
			fmt.Printf("shutdown %s\n", target.Volume)
			return nil
		},
	}
}

func createCmd() *cobra.Command {
	var mountpoint string
	var sizeStr string
	var noFormat bool
	var fromDir string
	var fromRaw string
	cmd := &cobra.Command{
		Use:   "create <store-url> <volume>",
		Short: "Create and mount a new volume in the foreground",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromDir != "" && fromRaw != "" {
				return fmt.Errorf("--from-dir and --from-raw are mutually exclusive")
			}
			if noFormat && (fromDir != "" || fromRaw != "") {
				return fmt.Errorf("--from-dir/--from-raw cannot be combined with --no-format")
			}
			var size uint64
			if sizeStr != "" {
				var err error
				size, err = parseSize(sizeStr)
				if err != nil {
					return err
				}
			}

			// Image import: write directly to the blob store, no daemon needed.
			if fromDir != "" || fromRaw != "" {
				return createFromImage(cmd.Context(), args[0], args[1], fromDir, fromRaw, size, mountpoint)
			}

			inst, dir, opts, volume, err := resolveOwnerOpts(args[0], args[1])
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
	cmd.Flags().StringVar(&fromDir, "from-dir", "", "populate the new ext4 volume from this host directory using e2fsprogs")
	cmd.Flags().StringVar(&fromRaw, "from-raw", "", "populate the new volume from this raw image file")
	return cmd
}

// createFromImage imports a raw or ext4 image directly into the blob store,
// then optionally mounts the volume via the daemon.
func createFromImage(ctx context.Context, rawURL, volumeName, fromDir, fromRaw string, size uint64, mountpoint string) error {
	inst, err := resolveStore(rawURL)
	if err != nil {
		return err
	}
	inst, blobStore, err := storage.ResolveFormattedStore(ctx, inst)
	if err != nil {
		return err
	}

	volType := storage.VolumeTypeExt4

	var imagePath string
	var tempImage bool
	if fromDir != "" {
		if size == 0 {
			size = storage.DefaultVolumeSize
		}
		imgPath, err := fsserver.BuildExt4ImageFromDir(ctx, fromDir, size)
		if err != nil {
			return err
		}
		imagePath = imgPath
		tempImage = true
	} else {
		info, err := os.Stat(fromRaw)
		if err != nil {
			return fmt.Errorf("stat raw image: %w", err)
		}
		if info.IsDir() {
			return fmt.Errorf("raw image path %q is a directory", fromRaw)
		}
		imagePath = fromRaw
	}

	f, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("open image file: %w", err)
	}

	importErr := storage.CreateVolumeFromImage(ctx, blobStore, volumeName, volType, f)
	_ = f.Close()
	if tempImage {
		_ = os.Remove(imagePath)
	}
	if importErr != nil {
		return importErr
	}

	switch {
	case fromDir != "":
		fmt.Printf("created volume %s from directory %s\n", volumeName, fromDir)
	case fromRaw != "":
		fmt.Printf("created volume %s from raw image %s\n", volumeName, fromRaw)
	}

	// If mount was requested, start the daemon to mount the already-created volume.
	if mountpoint != "" {
		dir := env.DefaultDir()
		consoleLogLevel := globalLogLevel
		if consoleLogLevel == "" {
			consoleLogLevel = os.Getenv("LOOPHOLE_LOG_LEVEL")
		}
		if consoleLogLevel == "" {
			consoleLogLevel = "warn"
		}
		socketPath := dir.VolumeSocket(inst.VolsetID, volumeName)
		opts := fsserver.ServerOptions{
			Foreground:      true,
			ConsoleLogLevel: consoleLogLevel,
			SocketPath:      socketPath,
		}
		return fsserver.MountFSAndServe(ctx, inst, dir, volumeName, mountpoint, opts)
	}

	return nil
}

func deleteCmd() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <mountpoint|device> | delete <store-url> <volume>",
		Short: "Delete a volume",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveStoreVolumeArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			_, vm, cleanup, err := openDirectManager(cmd.Context(), target.Store.URL())
			if err != nil {
				return err
			}
			defer cleanup()
			volume := target.Volume
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
		Use:   "ls <store-url>",
		Short: "List all volumes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, vm, cleanup, err := openDirectManager(cmd.Context(), args[0])
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
				if sock := socketForVolume(inst, v); sock != "" {
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
		Use:   "mount <store-url> <volume> [mountpoint]",
		Short: "Mount a volume as ext4 in the foreground",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			volume := args[1]
			mountpoint := ""
			if len(args) == 3 {
				mountpoint = args[2]
			}
			inst, dir, opts, volume, err := resolveOwnerOpts(args[0], volume)
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
		Use:   "checkpoints <mountpoint|device> | checkpoints <store-url> <volume>",
		Short: "List checkpoints for a volume",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveStoreVolumeArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			_, vm, cleanup, err := openDirectManager(cmd.Context(), target.Store.URL())
			if err != nil {
				return err
			}
			defer cleanup()
			checkpoints, err := storage.ListCheckpoints(cmd.Context(), vm.Store(), target.Volume)
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
		Use:   "clone --from-checkpoint <checkpoint_id> <store-url> <volume> <name>",
		Short: "Create a clone volume from a checkpoint",
		Args:  cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromCheckpoint == "" {
				return fmt.Errorf("--from-checkpoint is required")
			}
			target, cloneName, err := parseCheckpointCloneArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			_, vm, cleanup, err := openDirectManager(cmd.Context(), target.Store.URL())
			if err != nil {
				return err
			}
			defer cleanup()
			if err := storage.Clone(cmd.Context(), vm.Store(), target.Volume, fromCheckpoint, cloneName); err != nil {
				return err
			}
			fmt.Printf("created clone %s from %s@%s\n", cloneName, target, fromCheckpoint)
			return nil
		},
	}
	cmd.Flags().StringVar(&fromCheckpoint, "from-checkpoint", "", "checkpoint ID to clone from (required)")
	return cmd
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <mountpoint|device> | status <store-url> <volume>",
		Short: "Show owner process status",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveRunningTargetArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(target.Socket)

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
		deviceDDCmd(),
		deviceFlushCmd(),
	)

	return cmd
}

func deviceCreateCmd() *cobra.Command {
	var sizeStr string
	cmd := &cobra.Command{
		Use:   "create <store-url> <volume>",
		Short: "Create and attach a new raw volume in the foreground",
		Long:  "Create a fresh unformatted volume for raw block-device use and attach it immediately. --size is required.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			size, err := parseSize(sizeStr)
			if err != nil {
				return err
			}
			if size == 0 {
				return fmt.Errorf("--size is required for device create")
			}
			inst, dir, opts, volume, err := resolveOwnerOpts(args[0], args[1])
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
		Use:   "attach <store-url> <volume>",
		Short: "Attach a volume device in the foreground",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, dir, opts, volume, err := resolveOwnerOpts(args[0], args[1])
			if err != nil {
				return err
			}
			return fsserver.AttachBlockDeviceAndServe(cmd.Context(), inst, dir, volume, opts)
		},
	}
}

func deviceCheckpointCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint <mountpoint|device> | checkpoint <store-url> <volume>",
		Short: "Checkpoint a volume device",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveRunningTargetArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(target.Socket)
			cpID, err := c.DeviceCheckpoint(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint %s created\n", cpID)
			return nil
		},
	}
}

func deviceFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush <mountpoint|device> | flush <store-url> <volume>",
		Short: "Flush a volume's dirty pages to storage",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveRunningTargetArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			c := client.NewFromSocket(target.Socket)
			if err := c.DeviceFlush(cmd.Context()); err != nil {
				return err
			}
			fmt.Printf("flushed volume %s\n", target.Volume)
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
				target, err := resolveRunningTargetArgs(cmd.Context(), []string{dstVol})
				if err != nil {
					return err
				}
				c := client.NewFromSocket(target.Socket)
				return deviceDDImport(cmd, c, ifFlag, dstVol)
			}
			target, err := resolveRunningTargetArgs(cmd.Context(), []string{srcVol})
			if err != nil {
				return err
			}
			c := client.NewFromSocket(target.Socket)
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

func breakLeaseCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "break-lease <mountpoint|device> | break-lease <store-url> <volume>",
		Short: "Request the lease holder to release a volume",
		Long:  "Sends a release request to the remote lease holder. With -f, forcibly clears the lease if the holder doesn't respond.",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := resolveStoreVolumeArgs(cmd.Context(), args)
			if err != nil {
				return err
			}
			_, vm, cleanup, err := openDirectManager(cmd.Context(), target.Store.URL())
			if err != nil {
				return err
			}
			defer cleanup()
			volume := target.Volume
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
		Use:   "gc <store-url>",
		Short: "Delete orphaned layers that are no longer referenced by any volume or checkpoint",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, vm, cleanup, err := openDirectManager(cmd.Context(), args[0])
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

func cachedCmd() *cobra.Command {
	var dir string
	cmd := &cobra.Command{
		Use:    "cached",
		Short:  "Run the page cache daemon (usually started automatically)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dir == "" {
				return fmt.Errorf("--dir is required")
			}

			if err := cachedserver.StartServer(dir); err != nil {
				return fmt.Errorf("start cached server: %w", err)
			}

			slog.Info("loophole cached started", "dir", dir)

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh

			slog.Info("shutting down")
			if err := cachedserver.Shutdown(); err != nil {
				return fmt.Errorf("close cached server: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "", "cache directory (required)")
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

func socketForVolume(inst env.ResolvedStore, volume string) string {
	dir := env.DefaultDir()
	sock := dir.VolumeSocket(inst.VolsetID, volume)
	if _, err := os.Stat(sock); err == nil {
		return sock
	}
	return ""
}

func socketFromMountedTarget(target string) (string, error) {
	if sock, err := socketFromMountpoint(target); err == nil {
		return sock, nil
	}
	if strings.HasPrefix(target, "/") {
		if sock, err := socketFromDevice(target); err == nil {
			return sock, nil
		}
	}
	return "", fmt.Errorf("target %q is not a mounted path or device; pass <store-url> <volume> for volume-name operations", target)
}

func parseStoreVolumePair(ctx context.Context, rawURL, volume string) (resolvedTarget, error) {
	inst, err := resolveStore(rawURL)
	if err != nil {
		return resolvedTarget{}, err
	}
	inst, _, err = storage.ResolveFormattedStore(ctx, inst)
	if err != nil {
		return resolvedTarget{}, err
	}
	return resolvedTarget{
		Store:  inst,
		Volume: volume,
		Socket: socketForVolume(inst, volume),
	}, nil
}

func resolveMountedTarget(ctx context.Context, target string) (resolvedTarget, error) {
	if globalPID != 0 {
		c := client.NewFromSocket(fsserver.EmbedSocketPath(globalPID))
		status, err := c.Status(ctx)
		if err != nil {
			return resolvedTarget{}, err
		}
		if status.StoreURL == "" || status.Volume == "" {
			return resolvedTarget{}, fmt.Errorf("embedded daemon did not report store_url/volume")
		}
		inst, err := resolveStore(status.StoreURL)
		if err != nil {
			return resolvedTarget{}, err
		}
		inst.VolsetID = status.VolsetID
		if inst.VolsetID == "" {
			inst, _, err = storage.ResolveFormattedStore(ctx, inst)
			if err != nil {
				return resolvedTarget{}, err
			}
		}
		return resolvedTarget{Store: inst, Volume: status.Volume, Socket: c.Socket()}, nil
	}

	sock, err := socketFromMountedTarget(target)
	if err != nil {
		return resolvedTarget{}, err
	}
	c := client.NewFromSocket(sock)
	status, err := c.Status(ctx)
	if err != nil {
		return resolvedTarget{}, err
	}
	if status.StoreURL == "" {
		return resolvedTarget{}, fmt.Errorf("owner %q did not report store_url", target)
	}
	if status.Volume == "" {
		return resolvedTarget{}, fmt.Errorf("owner %q did not report volume", target)
	}
	inst, err := resolveStore(status.StoreURL)
	if err != nil {
		return resolvedTarget{}, err
	}
	inst.VolsetID = status.VolsetID
	if inst.VolsetID == "" {
		inst, _, err = storage.ResolveFormattedStore(ctx, inst)
		if err != nil {
			return resolvedTarget{}, err
		}
	}
	return resolvedTarget{Store: inst, Volume: status.Volume, Socket: sock}, nil
}

// resolveOwnerOpts resolves the store, dir, and ServerOptions for an owner command.
// It also checks for an existing owner process on the same socket.
func resolveOwnerOpts(rawURL, volume string) (env.ResolvedStore, env.Dir, fsserver.ServerOptions, string, error) {
	dir := env.DefaultDir()
	inst, err := resolveStore(rawURL)
	if err != nil {
		return env.ResolvedStore{}, "", fsserver.ServerOptions{}, "", err
	}
	inst, _, err = storage.ResolveFormattedStore(context.Background(), inst)
	if err != nil {
		return env.ResolvedStore{}, "", fsserver.ServerOptions{}, "", err
	}
	socketPath := dir.VolumeSocket(inst.VolsetID, volume)
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return env.ResolvedStore{}, "", fsserver.ServerOptions{}, "", err
	}
	c := client.NewFromSocket(socketPath)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Status(timeoutCtx); err == nil {
		return env.ResolvedStore{}, "", fsserver.ServerOptions{}, "", fmt.Errorf("volume %q is already managed at %s", volume, socketPath)
	}
	consoleLogLevel := globalLogLevel
	if consoleLogLevel == "" {
		consoleLogLevel = os.Getenv("LOOPHOLE_LOG_LEVEL")
	}
	if consoleLogLevel == "" {
		consoleLogLevel = "warn"
	}
	return inst, dir, fsserver.ServerOptions{
		Foreground:      true,
		ConsoleLogLevel: consoleLogLevel,
		SocketPath:      socketPath,
	}, volume, nil
}

func openDirectManager(ctx context.Context, rawURL string) (env.ResolvedStore, *storage.Manager, func(), error) {
	dir := env.DefaultDir()
	inst, err := resolveStore(rawURL)
	if err != nil {
		return env.ResolvedStore{}, nil, nil, err
	}
	inst, _, err = storage.ResolveFormattedStore(ctx, inst)
	if err != nil {
		return env.ResolvedStore{}, nil, nil, err
	}
	vm, err := storage.OpenManagerForStore(ctx, inst, dir)
	if err != nil {
		return env.ResolvedStore{}, nil, nil, err
	}
	return inst, vm, func() {
		util.SafeClose(vm, "close direct manager")
	}, nil
}

func resolveStoreVolumeArgs(ctx context.Context, args []string) (resolvedTarget, error) {
	switch len(args) {
	case 1:
		return resolveMountedTarget(ctx, args[0])
	case 2:
		return parseStoreVolumePair(ctx, args[0], args[1])
	default:
		return resolvedTarget{}, fmt.Errorf("invalid target arguments")
	}
}

func resolveRunningTargetArgs(ctx context.Context, args []string) (resolvedTarget, error) {
	switch len(args) {
	case 1:
		return resolveMountedTarget(ctx, args[0])
	case 2:
		target, err := parseStoreVolumePair(ctx, args[0], args[1])
		if err != nil {
			return resolvedTarget{}, err
		}
		if target.Socket == "" {
			return resolvedTarget{}, fmt.Errorf("volume %q is not currently mounted or attached in store %s", target.Volume, target.Store.URL())
		}
		return target, nil
	default:
		return resolvedTarget{}, fmt.Errorf("invalid running target arguments")
	}
}

func parseCheckpointCloneArgs(ctx context.Context, args []string) (resolvedTarget, string, error) {
	switch len(args) {
	case 2:
		target, err := resolveStoreVolumeArgs(ctx, args[:1])
		if err != nil {
			return resolvedTarget{}, "", err
		}
		return target, args[1], nil
	case 3:
		target, err := resolveStoreVolumeArgs(ctx, args[:2])
		if err != nil {
			return resolvedTarget{}, "", err
		}
		return target, args[2], nil
	default:
		return resolvedTarget{}, "", fmt.Errorf("invalid clone arguments")
	}
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
