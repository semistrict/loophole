package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
)

func addCommands(root *cobra.Command) {
	root.AddCommand(
		stopCmd(),
		createCmd(),
		deleteCmd(),
		lsCmd(),
		mountCmd(),
		unmountCmd(),
		freezeCmd(),
		checkpointCmd(),
		cloneCmd(),
		checkpointsCmd(),
		statusCmd(),
		statsCmd(),
		deviceCmd(),
		breakLeaseCmd(),
		migrateCmd(),
	)
}

func startDaemon(ctx context.Context, inst loophole.Instance, dir loophole.Dir, opts daemon.Options) error {
	d, err := daemon.Start(ctx, inst, dir, opts)
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
			// Wait for flush + lease release to complete before returning.
			_ = c.ShutdownWait(ctx)
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
	cmd.Flags().StringVarP(&volType, "type", "t", "", "volume/filesystem type (ext4); default ext4")
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

func freezeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "freeze <volume>",
		Short: "Freeze a volume, making it permanently immutable",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.Freeze(ctx, args[0]); err != nil {
				return err
			}
			fmt.Printf("volume %q frozen\n", args[0])
			return nil
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
			cpID, err := c.Checkpoint(cmd.Context(), mountpoint)
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
			c, err := resolveClient()
			if err != nil {
				return err
			}
			checkpoints, err := c.ListCheckpoints(cmd.Context(), args[0])
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
		Use:   "clone <mountpoint> <name> <clone_mountpoint>",
		Short: "Clone a volume and mount it",
		Long: `Clone a live mounted volume, or clone from a checkpoint.

With --from-checkpoint, the first arg is the volume name (not mountpoint):
  loophole clone --from-checkpoint <checkpoint_id> <volume> <clone_name> <clone_mountpoint>`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromCheckpoint != "" {
				// args: <volume> <clone_name> <clone_mountpoint>
				c, err := resolveClient()
				if err != nil {
					return err
				}
				if err := c.CloneFromCheckpoint(cmd.Context(), args[0], fromCheckpoint, args[1], args[2]); err != nil {
					return err
				}
				fmt.Printf("cloned %s@%s to %s at %s\n", args[0], fromCheckpoint, args[1], args[2])
				return nil
			}

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
	cmd.Flags().StringVar(&fromCheckpoint, "from-checkpoint", "", "clone from a checkpoint ID instead of a live volume")
	return cmd
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
		deviceCheckpointCmd(),
		deviceCloneCmd(),
		deviceDDCmd(),
		deviceFlushCmd(),
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

func deviceCheckpointCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "checkpoint <volume>",
		Short: "Checkpoint a volume device",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			cpID, err := c.DeviceCheckpoint(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Printf("checkpoint %s created\n", cpID)
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

func deviceFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush <volume>",
		Short: "Flush a volume's memtable to storage",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := resolveClient()
			if err != nil {
				return err
			}
			if err := c.FlushVolume(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("flushed volume %s\n", args[0])
			return nil
		},
	}
}

func deviceDDCmd() *cobra.Command {
	var ifFlag, ofFlag, bsFlag, typeFlag string

	cmd := &cobra.Command{
		Use:   "dd [if=<source>] [of=<dest>]",
		Short: "Copy raw data between files and volumes",
		Long: `Copy raw block data between local files and loophole volumes.
Use the volume: syntax to refer to a volume as a raw block device.

Import (file → volume): creates a new volume and writes the image into it.
Export (volume → file): reads raw volume data and writes it to a file.

Examples:
  loophole device dd if=/path/to/rootfs.ext4 of=myvolume:
  loophole device dd if=myvolume: of=/path/to/output.img
  loophole device dd if=/path/to/rootfs.ext4 of=myvolume: type=ext4`,
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
				case "type":
					typeFlag = v
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

			c, err := resolveClient()
			if err != nil {
				return err
			}

			if dstIsVol {
				return deviceDDImport(cmd, c, ifFlag, dstVol, typeFlag)
			}
			return deviceDDExport(cmd, c, srcVol, ofFlag)
		},
	}

	cmd.Flags().StringVar(&ifFlag, "if", "", "input: local file or volume:")
	cmd.Flags().StringVar(&ofFlag, "of", "", "output: local file or volume:")
	cmd.Flags().StringVar(&bsFlag, "bs", "", "block size (e.g. 1M, 4M); default 4M")
	cmd.Flags().StringVar(&typeFlag, "type", "", "volume type for import (ext4); default ext4")

	return cmd
}

// deviceDDImport writes a local file into a new volume.
func deviceDDImport(cmd *cobra.Command, c *client.Client, filePath, volume, volType string) error {
	if volType == "" {
		volType = loophole.VolumeTypeExt4
	}
	if volType != loophole.VolumeTypeExt4 {
		return fmt.Errorf("unsupported volume type %q", volType)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open image: %w", err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat image: %w", err)
	}
	size := uint64(fi.Size())
	fmt.Printf("Import: %s (%d bytes, %.1f GiB) → %s (type=%s)\n",
		filePath, size, float64(size)/(1<<30), volume, volType)

	return c.DeviceDD(cmd.Context(), client.CreateParams{
		Volume: volume,
		Size:   size,
		Type:   volType,
	}, f, os.Stdout)
}

// deviceDDExport reads a volume's raw data into a local file.
func deviceDDExport(cmd *cobra.Command, c *client.Client, volume, filePath string) error {
	ctx := cmd.Context()

	info, err := c.VolumeInfo(ctx, volume)
	if err != nil {
		return fmt.Errorf("get volume info: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer func() { _ = f.Close() }()

	fmt.Printf("Export: %s (%d bytes, %.1f GiB) → %s\n",
		volume, info.Size, float64(info.Size)/(1<<30), filePath)

	return c.DeviceDDRead(ctx, volume, info.Size, f, os.Stdout)
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

// resolveClientOnly returns a client for the current profile without
// starting the daemon. Used by commands like stop that shouldn't auto-start.
func resolveClientOnly() (*client.Client, error) {
	if globalPID != 0 {
		return client.NewFromSocket(daemon.EmbedSocketPath(globalPID)), nil
	}
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
	if globalPID != 0 {
		return client.NewFromSocket(daemon.EmbedSocketPath(globalPID)), nil
	}
	dir := loophole.DefaultDir()
	inst, err := resolveProfile(dir)
	if err != nil {
		return nil, err
	}
	c := client.New(dir, inst)
	c.Bin = selfBin
	c.Sudo = runtime.GOOS == "linux"
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

// migrationState tracks which migrations have been run. Stored as migrations.json
// at the root of the S3 bucket.
type migrationState struct {
	Migrations map[string]migrationRecord `json:"migrations"`
}

type migrationRecord struct {
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at,omitempty"`
}

func loadMigrationState(ctx context.Context, store loophole.ObjectStore) (migrationState, string, error) {
	state, etag, err := loophole.ReadJSON[migrationState](ctx, store, "migrations.json")
	if errors.Is(err, loophole.ErrNotFound) {
		return migrationState{Migrations: map[string]migrationRecord{}}, "", nil
	}
	if err != nil {
		return migrationState{}, "", err
	}
	if state.Migrations == nil {
		state.Migrations = map[string]migrationRecord{}
	}
	return state, etag, nil
}

func saveMigrationState(ctx context.Context, store loophole.ObjectStore, state migrationState, etag string) (string, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	if etag == "" {
		if err := store.PutIfNotExists(ctx, "migrations.json", data); err != nil {
			if !errors.Is(err, loophole.ErrExists) {
				return "", err
			}
			// Race — someone else created it. Read back etag and CAS.
			_, freshEtag, rerr := loophole.ReadJSON[migrationState](ctx, store, "migrations.json")
			if rerr != nil {
				return "", fmt.Errorf("read back migrations.json: %w", rerr)
			}
			return store.PutBytesCAS(ctx, "migrations.json", data, freshEtag)
		}
		// Read back etag for subsequent CAS writes.
		_, freshEtag, rerr := loophole.ReadBytes(ctx, store, "migrations.json")
		if rerr != nil {
			return "", fmt.Errorf("read back migrations.json etag: %w", rerr)
		}
		return freshEtag, nil
	}
	return store.PutBytesCAS(ctx, "migrations.json", data, etag)
}

func migrateCmd() *cobra.Command {
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate S3 data to the current format",
		Long:  "Rewrites volume refs (timeline_id → layer_id) and merges layer meta.json into index.json.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			dir := loophole.DefaultDir()
			inst, err := resolveProfile(dir)
			if err != nil {
				return err
			}
			store, err := loophole.NewS3Store(ctx, inst)
			if err != nil {
				return fmt.Errorf("connect to S3: %w", err)
			}

			// Load migration state.
			state, stateEtag, err := loadMigrationState(ctx, store)
			if err != nil {
				return fmt.Errorf("load migrations.json: %w", err)
			}

			const migName = "001_timeline_id_to_layer_id"
			if rec, ok := state.Migrations[migName]; ok && rec.CompletedAt != "" {
				fmt.Printf("migration %s already completed at %s\n", migName, rec.CompletedAt)
				return nil
			}

			now := time.Now().UTC().Format(time.RFC3339)

			// Mark started (unless dry run).
			if !dryRun {
				state.Migrations[migName] = migrationRecord{StartedAt: now}
				newEtag, err := saveMigrationState(ctx, store, state, stateEtag)
				if err != nil {
					return fmt.Errorf("save migration started: %w", err)
				}
				stateEtag = newEtag
				fmt.Printf("migration %s: started at %s\n", migName, now)
			}

			// Migrate volume refs: timeline_id → layer_id.
			fmt.Println("migrating volume refs...")
			volRefs := store.At("volumes")
			objects, err := volRefs.ListKeys(ctx, "")
			if err != nil {
				return fmt.Errorf("list volumes: %w", err)
			}
			for _, obj := range objects {
				// Only process index.json files (volume refs).
				if !strings.HasSuffix(obj.Key, "/index.json") || strings.Contains(obj.Key, "/checkpoints/") {
					continue
				}
				raw, etag, err := loophole.ReadBytes(ctx, volRefs, obj.Key)
				if err != nil {
					fmt.Fprintf(os.Stderr, "  skip %s: %v\n", obj.Key, err)
					continue
				}

				var m map[string]json.RawMessage
				if err := json.Unmarshal(raw, &m); err != nil {
					fmt.Fprintf(os.Stderr, "  skip %s: bad JSON: %v\n", obj.Key, err)
					continue
				}

				changed := false

				// Rename timeline_id → layer_id.
				if val, ok := m["timeline_id"]; ok {
					if _, hasNew := m["layer_id"]; !hasNew {
						m["layer_id"] = val
					}
					delete(m, "timeline_id")
					changed = true
				}

				if !changed {
					fmt.Printf("  %s: ok\n", obj.Key)
					continue
				}

				out, err := json.Marshal(m)
				if err != nil {
					return fmt.Errorf("marshal %s: %w", obj.Key, err)
				}

				if dryRun {
					fmt.Printf("  %s: would rewrite (timeline_id → layer_id)\n", obj.Key)
					continue
				}

				if _, err := volRefs.PutBytesCAS(ctx, obj.Key, out, etag); err != nil {
					return fmt.Errorf("write %s: %w", obj.Key, err)
				}
				fmt.Printf("  %s: migrated\n", obj.Key)
			}

			// Migrate frozen layers: move FrozenAt from meta.json body to
			// index.json object metadata (no body rewrite needed).
			fmt.Println("migrating frozen layer metadata...")
			layersStore := store.At("layers")
			layerDirs, err := layersStore.ListKeys(ctx, "")
			if err != nil {
				return fmt.Errorf("list layers: %w", err)
			}
			// Collect unique layer IDs from directory listing.
			seen := map[string]bool{}
			for _, obj := range layerDirs {
				parts := strings.SplitN(obj.Key, "/", 2)
				if len(parts) > 0 && parts[0] != "" {
					seen[parts[0]] = true
				}
			}
			for layerID := range seen {
				ls := layersStore.At(layerID)

				// Read meta.json — if it doesn't exist or has no FrozenAt, skip.
				metaRaw, _, err := loophole.ReadBytes(ctx, ls, "meta.json")
				if err != nil {
					continue // no meta.json — nothing to migrate
				}
				var oldMeta struct {
					FrozenAt  string `json:"frozen_at"`
					CreatedAt string `json:"created_at"`
				}
				if err := json.Unmarshal(metaRaw, &oldMeta); err != nil {
					continue
				}

				// Check if index.json already has the metadata.
				existingMeta, err := ls.HeadMeta(ctx, "index.json")
				if err != nil {
					fmt.Fprintf(os.Stderr, "  layer %s: no index.json, skip\n", layerID[:8])
					continue
				}
				if existingMeta["frozen_at"] != "" && existingMeta["created_at"] != "" {
					continue // already migrated
				}

				// Build new metadata from meta.json fields.
				newMeta := make(map[string]string)
				for k, v := range existingMeta {
					newMeta[k] = v
				}
				if oldMeta.CreatedAt != "" && newMeta["created_at"] == "" {
					newMeta["created_at"] = oldMeta.CreatedAt
				}
				if oldMeta.FrozenAt != "" && newMeta["frozen_at"] == "" {
					newMeta["frozen_at"] = oldMeta.FrozenAt
				}

				if len(newMeta) == len(existingMeta) {
					continue // nothing to add
				}

				if dryRun {
					fmt.Printf("  layer %s: would set metadata frozen_at=%s created_at=%s\n",
						layerID[:8], newMeta["frozen_at"], newMeta["created_at"])
					continue
				}

				if err := ls.SetMeta(ctx, "index.json", newMeta); err != nil {
					return fmt.Errorf("set metadata for layer %s: %w", layerID[:8], err)
				}
				fmt.Printf("  layer %s: migrated metadata to index.json object\n", layerID[:8])
			}

			if dryRun {
				fmt.Println("dry run — no changes written")
			} else {
				// Mark completed.
				rec := state.Migrations[migName]
				rec.CompletedAt = time.Now().UTC().Format(time.RFC3339)
				state.Migrations[migName] = rec
				if _, err := saveMigrationState(ctx, store, state, stateEtag); err != nil {
					return fmt.Errorf("save migration completed: %w", err)
				}
				fmt.Printf("migration %s: completed\n", migName)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "show what would change without writing")
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
