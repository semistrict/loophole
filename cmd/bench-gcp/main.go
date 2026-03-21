// Command bench-gcp provisions a GCE instance with GCS storage for running
// loophole fio/fsx benchmarks. Subcommands: init, sync, run, ssh, destroy.
//
// Uses Go client libraries for GCE and go-cloud for GCS bucket management.
// SSH/sync still use gcloud CLI (no native Go SSH to GCE without IAP tunneling setup).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	gcsstorage "cloud.google.com/go/storage"
	"github.com/semistrict/loophole/internal/util"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/protobuf/proto"
)

const (
	defaultZone     = "us-central1-a"
	defaultInstance = "loophole-bench"
	machineType     = "n2-standard-8" // 8 vCPU, 32 GB
	diskSizeGB      = 50
	imageFamily     = "debian-12"
	imageProject    = "debian-cloud"

	remoteSrcDir = "loophole" // relative to $HOME
)

func main() {
	var (
		project  string
		zone     string
		instance string
		bucket   string
	)

	root := &cobra.Command{
		Use:   "bench-gcp",
		Short: "Provision GCE instance and run loophole benchmarks with GCS backend",
	}

	root.PersistentFlags().StringVar(&project, "project", envOrDefault("CLOUDSDK_CORE_PROJECT", ""), "GCP project")
	root.PersistentFlags().StringVar(&zone, "zone", envOrDefault("CLOUDSDK_COMPUTE_ZONE", defaultZone), "GCE zone")
	root.PersistentFlags().StringVar(&instance, "instance", defaultInstance, "GCE instance name")
	root.PersistentFlags().StringVar(&bucket, "bucket", envOrDefault("LOOPHOLE_BENCH_BUCKET", ""), "GCS bucket for loophole storage")

	cfg := func() *benchConfig {
		if project == "" {
			log.Fatal("--project or CLOUDSDK_CORE_PROJECT is required")
		}
		if bucket == "" {
			log.Fatal("--bucket or LOOPHOLE_BENCH_BUCKET is required")
		}
		return &benchConfig{project: project, zone: zone, instance: instance, bucket: bucket}
	}

	root.AddCommand(
		initCmd(cfg),
		syncCmd(cfg),
		runCmd(cfg),
		sshCmd(cfg),
		destroyCmd(cfg),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

type benchConfig struct {
	project  string
	zone     string
	instance string
	bucket   string
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// region extracts the region from a zone (e.g. "us-central1-a" -> "us-central1").
func region(zone string) string {
	parts := strings.Split(zone, "-")
	if len(parts) >= 3 {
		return strings.Join(parts[:len(parts)-1], "-")
	}
	return zone
}

// sshExec runs a command on the remote instance via gcloud compute ssh.
func (c *benchConfig) sshExec(ctx context.Context, remoteCmd string) error {
	return c.sshExecOpts(ctx, remoteCmd, false)
}

// sshExecWithAgent runs a command with SSH agent forwarding.
func (c *benchConfig) sshExecWithAgent(ctx context.Context, remoteCmd string) error {
	return c.sshExecOpts(ctx, remoteCmd, true)
}

func (c *benchConfig) sshExecOpts(ctx context.Context, remoteCmd string, agent bool) error {
	args := []string{"compute", "ssh", c.instance,
		"--project", c.project, "--zone", c.zone,
		"--command", remoteCmd}
	if agent {
		args = append(args, "--ssh-flag=-A")
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// sshInteractive opens an interactive SSH session.
func (c *benchConfig) sshInteractive(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "gcloud", "compute", "ssh", c.instance,
		"--project", c.project, "--zone", c.zone)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// --- init subcommand ---

func initCmd(cfgFn func() *benchConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Create GCE instance, install deps, create GCS bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cfgFn()
			ctx := cmd.Context()
			return cfg.doInit(ctx)
		},
	}
}

func (c *benchConfig) doInit(ctx context.Context) error {
	// Create GCS bucket via go-cloud.
	if err := c.ensureBucket(ctx); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}

	// Create GCE instance via compute API.
	if err := c.createInstance(ctx); err != nil {
		return fmt.Errorf("create instance: %w", err)
	}

	// Wait for SSH readiness.
	fmt.Println("==> Waiting for SSH...")
	for range 30 {
		if err := c.sshExec(ctx, "true"); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Install packages.
	fmt.Println("==> Installing packages...")
	if err := c.sshExec(ctx, setupScript); err != nil {
		return fmt.Errorf("install packages: %w", err)
	}

	// Write loophole config (native GCS client uses ADC, no HMAC keys needed).
	return c.writeRemoteConfig(ctx)
}

func (c *benchConfig) ensureBucket(ctx context.Context) error {
	fmt.Printf("==> Ensuring GCS bucket gs://%s exists...\n", c.bucket)

	client, err := gcsstorage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}
	defer util.SafeClose(client, "close storage client")

	bkt := client.Bucket(c.bucket)
	_, err = bkt.Attrs(ctx)
	if err == gcsstorage.ErrBucketNotExist {
		fmt.Printf("==> Creating bucket gs://%s in %s...\n", c.bucket, region(c.zone))
		err = bkt.Create(ctx, c.project, &gcsstorage.BucketAttrs{
			Location:                 region(c.zone),
			UniformBucketLevelAccess: gcsstorage.UniformBucketLevelAccess{Enabled: true},
		})
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
		fmt.Printf("==> Bucket gs://%s created\n", c.bucket)
		return nil
	}
	if err != nil {
		return fmt.Errorf("check bucket: %w", err)
	}
	fmt.Printf("==> Bucket gs://%s exists\n", c.bucket)
	return nil
}

func (c *benchConfig) createInstance(ctx context.Context) error {
	fmt.Printf("==> Ensuring GCE instance %s (%s in %s)...\n", c.instance, machineType, c.zone)

	client, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return fmt.Errorf("create compute client: %w", err)
	}
	defer util.SafeClose(client, "close compute client")

	// Check if instance already exists.
	existing, err := client.Get(ctx, &computepb.GetInstanceRequest{
		Project:  c.project,
		Zone:     c.zone,
		Instance: c.instance,
	})
	if err == nil {
		status := existing.GetStatus()
		fmt.Printf("==> Instance %s already exists (status=%s)\n", c.instance, status)
		if status == "TERMINATED" {
			fmt.Println("==> Starting terminated instance...")
			op, err := client.Start(ctx, &computepb.StartInstanceRequest{
				Project:  c.project,
				Zone:     c.zone,
				Instance: c.instance,
			})
			if err != nil {
				return fmt.Errorf("start instance: %w", err)
			}
			if err := op.Wait(ctx); err != nil {
				return fmt.Errorf("wait for start: %w", err)
			}
		}
		return nil
	}
	// Only proceed to create if the error is NotFound (404).
	// Any other error (auth, bad project/zone, transient) should be surfaced.
	if !strings.Contains(err.Error(), "404") && !strings.Contains(err.Error(), "notFound") {
		return fmt.Errorf("check instance: %w", err)
	}

	zone := c.zone
	machineTypeURL := fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType)
	sourceImage := fmt.Sprintf("projects/%s/global/images/family/%s", imageProject, imageFamily)

	req := &computepb.InsertInstanceRequest{
		Project: c.project,
		Zone:    zone,
		InstanceResource: &computepb.Instance{
			Name:        proto.String(c.instance),
			MachineType: proto.String(machineTypeURL),
			Disks: []*computepb.AttachedDisk{
				{
					Boot:       proto.Bool(true),
					AutoDelete: proto.Bool(true),
					InitializeParams: &computepb.AttachedDiskInitializeParams{
						DiskSizeGb:  proto.Int64(diskSizeGB),
						DiskType:    proto.String(fmt.Sprintf("zones/%s/diskTypes/pd-ssd", zone)),
						SourceImage: proto.String(sourceImage),
					},
				},
			},
			NetworkInterfaces: []*computepb.NetworkInterface{
				{
					AccessConfigs: []*computepb.AccessConfig{
						{
							Name: proto.String("External NAT"),
							Type: proto.String("ONE_TO_ONE_NAT"),
						},
					},
				},
			},
			ServiceAccounts: []*computepb.ServiceAccount{
				{
					Email:  proto.String("default"),
					Scopes: []string{"https://www.googleapis.com/auth/devstorage.full_control"},
				},
			},
			Metadata: &computepb.Metadata{
				Items: []*computepb.Items{
					{
						Key:   proto.String("enable-oslogin"),
						Value: proto.String("true"),
					},
				},
			},
		},
	}

	op, err := client.Insert(ctx, req)
	if err != nil {
		return fmt.Errorf("insert instance: %w", err)
	}

	fmt.Println("==> Waiting for instance to be ready...")
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("wait for instance: %w", err)
	}

	fmt.Printf("==> Instance %s created\n", c.instance)
	return nil
}

const setupScript = `set -eux
# Go
if ! command -v go &>/dev/null; then
    curl -fsSL https://go.dev/dl/go1.25.2.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
    echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> $HOME/.bashrc
fi
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin

# System packages
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    fio fuse e2fsprogs util-linux make git \
    autoconf automake libtool pkg-config xfslibs-dev libacl1-dev libaio-dev libattr1-dev

# Build fsx from xfstests
if ! command -v fsx &>/dev/null; then
    cd /tmp
    rm -rf xfstests-dev
    git clone --depth=1 https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git
    cd xfstests-dev
    make configure
    ./configure
    make -j$(nproc)
    sudo cp ltp/fsx /usr/local/bin/fsx
    cd /
    rm -rf /tmp/xfstests-dev
fi

# Verify
go version
fio --version
fsx 2>&1 | head -1 || true
echo "==> Setup complete"
`

// --- sync subcommand ---

func syncCmd(cfgFn func() *benchConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "sync",
		Short: "Sync source code to GCE instance and build",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cfgFn()
			ctx := cmd.Context()
			return cfg.doSync(ctx)
		},
	}
}

func (c *benchConfig) doSync(ctx context.Context) error {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}

	// Get local HEAD ref and remote URL.
	headRef, err := execOutput(ctx, repoRoot, "git", "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	remoteURL, err := execOutput(ctx, repoRoot, "git", "remote", "get-url", "origin")
	if err != nil {
		return fmt.Errorf("git remote get-url: %w", err)
	}

	fmt.Printf("==> Syncing to %s at %s\n", c.instance, headRef[:12])

	// Clone (or fetch+checkout) on remote with agent forwarding for private repo access.
	cloneScript := fmt.Sprintf(`set -eux
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
mkdir -p $HOME/.ssh
ssh-keyscan -t ed25519 github.com >> $HOME/.ssh/known_hosts 2>/dev/null
DEST=$HOME/%s
if [ -d "$DEST/.git" ]; then
    cd "$DEST"
    git fetch origin
    git checkout --force %s
else
    git clone %s "$DEST"
    cd "$DEST"
    git checkout %s
fi
echo "==> Remote at $(git rev-parse --short HEAD)"
`, remoteSrcDir, headRef, remoteURL, headRef)

	if err := c.sshExecWithAgent(ctx, cloneScript); err != nil {
		return fmt.Errorf("clone/checkout: %w", err)
	}

	// Find locally modified/untracked files and copy them over.
	dirty, err := execOutputRaw(ctx, repoRoot, "git", "status", "--porcelain")
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}
	if dirty != "" {
		files := parseDirtyFiles(dirty)
		if len(files) > 0 {
			fmt.Printf("==> Copying %d dirty path(s)...\n", len(files))
			for _, f := range files {
				fmt.Printf("  %s\n", f)
			}
			if err := c.tarCopyFiles(ctx, repoRoot, files); err != nil {
				return fmt.Errorf("copy dirty files: %w", err)
			}
		}
	} else {
		fmt.Println("==> No local changes to copy")
	}

	// Build on remote.
	fmt.Println("==> Building on remote...")
	if err := c.sshExec(ctx, fmt.Sprintf(`set -eux
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
cd $HOME/%s
make build
make loophole
ls -la bin/
`, remoteSrcDir)); err != nil {
		return fmt.Errorf("build: %w", err)
	}

	return nil
}

// execOutput runs a command in dir and returns trimmed stdout.
func execOutput(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// execOutputRaw runs a command in dir and returns stdout without trimming leading whitespace.
func execOutputRaw(ctx context.Context, dir string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	return strings.TrimRight(string(out), "\n"), err
}

// parseDirtyFiles extracts file paths from `git status --porcelain` output.
// Format: "XY path" where XY is exactly 2 status chars, then a space, then the path.
func parseDirtyFiles(porcelain string) []string {
	var files []string
	for _, line := range strings.Split(porcelain, "\n") {
		if len(line) < 4 { // "XY " + at least 1 char
			continue
		}
		x, y := line[0], line[1]
		path := line[3:]
		// Skip deleted files.
		if x == 'D' || y == 'D' {
			continue
		}
		// Handle renames: "R  old -> new"
		if idx := strings.Index(path, " -> "); idx >= 0 {
			path = path[idx+4:]
		}
		files = append(files, path)
	}
	return files
}

// tarCopyFiles tars the given paths locally and extracts on the remote via ssh pipe.
func (c *benchConfig) tarCopyFiles(ctx context.Context, repoRoot string, files []string) error {
	// Build tar command with explicit file list.
	// Use --no-recursion for files, but directories need recursion.
	tarArgs := []string{"-C", repoRoot, "-czf", "-"}
	tarArgs = append(tarArgs, files...)

	extractCmd := fmt.Sprintf("cd $HOME/%s && tar xzf -", remoteSrcDir)
	pipe := fmt.Sprintf("gcloud compute ssh %s --project %s --zone %s --command '%s'",
		c.instance, c.project, c.zone, extractCmd)

	cmd := exec.CommandContext(ctx, "bash", "-c", pipe)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Pipe tar output to stdin.
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	tarCmd.Dir = repoRoot
	tarCmd.Stderr = os.Stderr

	tarOut, err := tarCmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stdin = tarOut

	if err := tarCmd.Start(); err != nil {
		return fmt.Errorf("tar start: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ssh start: %w", err)
	}
	if err := tarCmd.Wait(); err != nil {
		return fmt.Errorf("tar: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("ssh extract: %w", err)
	}
	return nil
}

// --- run subcommand ---

func runCmd(cfgFn func() *benchConfig) *cobra.Command {
	var (
		volSize    string
		fioOnly    bool
		fsxOnly    bool
		e2eOnly    bool
		fioRuntime string
		debug      bool
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run fio/fsx/e2e benchmarks on the GCE instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cfgFn()
			ctx := cmd.Context()
			return cfg.doRun(ctx, runOpts{
				volSize:    volSize,
				fioOnly:    fioOnly,
				fsxOnly:    fsxOnly,
				e2eOnly:    e2eOnly,
				fioRuntime: fioRuntime,
				debug:      debug,
			})
		},
	}
	cmd.Flags().StringVar(&volSize, "vol-size", "10GB", "volume size for benchmarks")
	cmd.Flags().BoolVar(&fioOnly, "fio-only", false, "run only fio benchmarks")
	cmd.Flags().BoolVar(&fsxOnly, "fsx-only", false, "run only fsx stress tests")
	cmd.Flags().BoolVar(&e2eOnly, "e2e-only", false, "run only e2e Go benchmarks")
	cmd.Flags().StringVar(&fioRuntime, "fio-runtime", "30", "fio runtime in seconds per job")
	cmd.Flags().BoolVar(&debug, "debug", false, "enable debug logging on the loophole daemon")
	return cmd
}

type runOpts struct {
	volSize    string
	fioOnly    bool
	fsxOnly    bool
	e2eOnly    bool
	fioRuntime string
	debug      bool
}

// writeRemoteConfig writes ~/.loophole/config.toml on the remote.
// The native GCS backend uses application default credentials (GCE metadata),
// so no HMAC keys are needed.
func (c *benchConfig) writeRemoteConfig(ctx context.Context) error {
	fmt.Println("==> Writing loophole config...")
	script, err := renderTemplate(configTmpl, map[string]any{
		"Bucket": c.bucket,
		"Region": region(c.zone),
	})
	if err != nil {
		return err
	}
	return c.sshExec(ctx, script)
}

func (c *benchConfig) doRun(ctx context.Context, opts runOpts) error {
	runAll := !opts.fioOnly && !opts.fsxOnly && !opts.e2eOnly

	script, err := renderTemplate(benchTmpl, map[string]any{
		"SrcDir":     remoteSrcDir,
		"VolSize":    opts.volSize,
		"Debug":      opts.debug,
		"RunFio":     runAll || opts.fioOnly,
		"RunFsx":     runAll || opts.fsxOnly,
		"RunE2E":     runAll || opts.e2eOnly,
		"FioRuntime": opts.fioRuntime,
		"FioJobs":    fioJobs,
		"Bucket":     c.bucket,
	})
	if err != nil {
		return err
	}
	if err := c.sshExec(ctx, script); err != nil {
		return err
	}

	return c.collectResults(ctx, opts)
}

type fioJob struct {
	Name    string
	RW      string
	BS      string
	Size    string
	NumJobs int
	Extra   string // e.g. "--rwmixread=70"
}

var fioJobs = []fioJob{
	{"seq-write", "write", "1M", "256M", 1, ""},
	{"seq-read", "read", "1M", "256M", 1, ""},
	{"rand-write-4k", "randwrite", "4k", "128M", 1, ""},
	{"rand-read-4k", "randread", "4k", "128M", 1, ""},
	{"randrw-4k", "randrw", "4k", "128M", 1, "--rwmixread=70"},
	{"seq-write-64k", "write", "64k", "256M", 1, ""},
	{"rand-write-64k", "randwrite", "64k", "256M", 1, ""},
}

// BenchResult is the structured output saved after a benchmark run.
type BenchResult struct {
	Timestamp    string         `json:"timestamp"`
	HeadSHA      string         `json:"head_sha"`
	Platform     string         `json:"platform"`
	Instance     string         `json:"instance"`
	MachineType  string         `json:"machine_type"`
	Project      string         `json:"project"`
	Zone         string         `json:"zone"`
	Region       string         `json:"region"`
	Bucket       string         `json:"bucket"`
	Endpoint     string         `json:"endpoint"`
	Profile      string         `json:"profile"`
	RemoteSrcDir string         `json:"remote_src_dir"`
	VolumeName   string         `json:"volume_name"`
	Mountpoint   string         `json:"mountpoint"`
	VolSize      string         `json:"vol_size"`
	FioRuntime   string         `json:"fio_runtime"`
	Fio          []FioResult    `json:"fio,omitempty"`
	RunConfig    BenchRunConfig `json:"run_config"`
	Repro        BenchRepro     `json:"repro"`
}

type FioResult struct {
	Name       string  `json:"name"`
	RW         string  `json:"rw"`
	BS         string  `json:"bs"`
	Size       string  `json:"size"`
	ReadMBps   float64 `json:"read_mbps,omitempty"`
	ReadIOPS   float64 `json:"read_iops,omitempty"`
	ReadLatUs  float64 `json:"read_lat_us,omitempty"`
	WriteMBps  float64 `json:"write_mbps,omitempty"`
	WriteIOPS  float64 `json:"write_iops,omitempty"`
	WriteLatUs float64 `json:"write_lat_us,omitempty"`
}

type BenchRunConfig struct {
	Debug   bool          `json:"debug"`
	FioOnly bool          `json:"fio_only"`
	FsxOnly bool          `json:"fsx_only"`
	E2EOnly bool          `json:"e2e_only"`
	FioJobs []BenchFioJob `json:"fio_jobs,omitempty"`
}

type BenchFioJob struct {
	Name    string `json:"name"`
	RW      string `json:"rw"`
	BS      string `json:"bs"`
	Size    string `json:"size"`
	NumJobs int    `json:"num_jobs"`
	Extra   string `json:"extra,omitempty"`
}

type BenchRepro struct {
	SyncCommand string `json:"sync_command"`
	RunCommand  string `json:"run_command"`
}

func (c *benchConfig) collectResults(ctx context.Context, opts runOpts) error {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return err
	}

	headSHA, _ := execOutput(ctx, repoRoot, "git", "rev-parse", "--short", "HEAD")
	now := time.Now().UTC()
	reproArgs := []string{
		"go", "run", "./cmd/bench-gcp",
		"--project", c.project,
		"--zone", c.zone,
		"--bucket", c.bucket,
	}
	fioConfig := make([]BenchFioJob, 0, len(fioJobs))
	for _, job := range fioJobs {
		fioConfig = append(fioConfig, BenchFioJob(job))
	}
	runArgs := append(append([]string{}, reproArgs...), "run", "--vol-size", opts.volSize, "--fio-runtime", opts.fioRuntime)
	if opts.fioOnly {
		runArgs = append(runArgs, "--fio-only")
	}
	if opts.fsxOnly {
		runArgs = append(runArgs, "--fsx-only")
	}
	if opts.e2eOnly {
		runArgs = append(runArgs, "--e2e-only")
	}
	if opts.debug {
		runArgs = append(runArgs, "--debug")
	}

	result := BenchResult{
		Timestamp:    now.Format(time.RFC3339),
		HeadSHA:      headSHA,
		Platform:     "gce",
		Instance:     c.instance,
		MachineType:  machineType,
		Project:      c.project,
		Zone:         c.zone,
		Region:       region(c.zone),
		Bucket:       c.bucket,
		Endpoint:     "https://storage.googleapis.com",
		Profile:      "gcs",
		RemoteSrcDir: remoteSrcDir,
		VolumeName:   "bench-vol",
		Mountpoint:   "/mnt/loophole-bench",
		VolSize:      opts.volSize,
		FioRuntime:   opts.fioRuntime,
		RunConfig: BenchRunConfig{
			Debug:   opts.debug,
			FioOnly: opts.fioOnly,
			FsxOnly: opts.fsxOnly,
			E2EOnly: opts.e2eOnly,
			FioJobs: fioConfig,
		},
		Repro: BenchRepro{
			SyncCommand: strings.Join(append(append([]string{}, reproArgs...), "sync"), " "),
			RunCommand:  strings.Join(runArgs, " "),
		},
	}

	// Download and parse fio results.
	if opts.fioOnly || (!opts.fsxOnly && !opts.e2eOnly) {
		for _, job := range fioJobs {
			raw, err := c.sshOutput(ctx, fmt.Sprintf("cat ~/bench-results/fio-%s.json", job.Name))
			if err != nil {
				slog.Warn("failed to read fio result", "job", job.Name, "error", err)
				continue
			}
			fr, err := parseFioJSON(raw, job)
			if err != nil {
				slog.Warn("failed to parse fio result", "job", job.Name, "error", err)
				continue
			}
			result.Fio = append(result.Fio, fr)
		}
	}

	// Save locally.
	outDir := filepath.Join(repoRoot, "bench-results")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create bench-results dir: %w", err)
	}
	filename := fmt.Sprintf("%s_%s_%s_%s.json",
		now.Format("20060102-150405"),
		headSHA,
		c.zone,
		machineType,
	)
	outPath := filepath.Join(outDir, filename)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	if err := os.WriteFile(outPath, data, 0o644); err != nil {
		return fmt.Errorf("write results: %w", err)
	}
	fmt.Printf("==> Results saved to %s\n", outPath)
	return nil
}

// sshOutput runs a command on the remote and captures stdout.
func (c *benchConfig) sshOutput(ctx context.Context, remoteCmd string) (string, error) {
	args := []string{"compute", "ssh", c.instance,
		"--project", c.project, "--zone", c.zone,
		"--command", remoteCmd}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	out, err := cmd.Output()
	return string(out), err
}

func parseFioJSON(raw string, job fioJob) (FioResult, error) {
	var fioOut struct {
		Jobs []struct {
			Read struct {
				BWBytes float64 `json:"bw_bytes"`
				IOPS    float64 `json:"iops"`
				LatNs   struct {
					Mean float64 `json:"mean"`
				} `json:"lat_ns"`
			} `json:"read"`
			Write struct {
				BWBytes float64 `json:"bw_bytes"`
				IOPS    float64 `json:"iops"`
				LatNs   struct {
					Mean float64 `json:"mean"`
				} `json:"lat_ns"`
			} `json:"write"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(raw), &fioOut); err != nil {
		return FioResult{}, err
	}
	fr := FioResult{
		Name: job.Name,
		RW:   job.RW,
		BS:   job.BS,
		Size: job.Size,
	}
	if len(fioOut.Jobs) > 0 {
		j := fioOut.Jobs[0]
		if j.Read.BWBytes > 0 {
			fr.ReadMBps = j.Read.BWBytes / 1024 / 1024
			fr.ReadIOPS = j.Read.IOPS
			fr.ReadLatUs = j.Read.LatNs.Mean / 1000
		}
		if j.Write.BWBytes > 0 {
			fr.WriteMBps = j.Write.BWBytes / 1024 / 1024
			fr.WriteIOPS = j.Write.IOPS
			fr.WriteLatUs = j.Write.LatNs.Mean / 1000
		}
	}
	return fr, nil
}

func renderTemplate(tmpl string, data any) (string, error) {
	t, err := template.New("script").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf strings.Builder
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}

const configTmpl = `set -eux
mkdir -p $HOME/.loophole
cat > $HOME/.loophole/config.toml << 'TOML'
default_profile = "gcs"

[profiles.gcs]
endpoint = "https://storage.googleapis.com"
bucket = "{{.Bucket}}"
region = "{{.Region}}"
TOML
echo "==> Config written"
`

const benchTmpl = `set -eux
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
{{if .Debug}}export LOOPHOLE_LOG_LEVEL=debug{{end}}
LOOPHOLE=$HOME/{{.SrcDir}}/bin/loophole-linux-amd64
PROFILE=gcs
RESULTS_DIR=$HOME/bench-results
MOUNTPOINT=/mnt/loophole-bench

mkdir -p $RESULTS_DIR

if [ ! -f $HOME/.loophole/config.toml ]; then
    echo "FATAL: config.toml not found. Run 'init' first."
    exit 1
fi

cleanup() {
    echo "==> Cleaning up..."
    {{if .Debug}}
    # Dump daemon log and dmesg on exit
    LOGFILE=$(find $HOME/.loophole -name '*.log' -newer $MOUNTPOINT 2>/dev/null | head -1)
    if [ -n "$LOGFILE" ]; then
        echo "==> Daemon log (last 100 lines): $LOGFILE"
        tail -100 "$LOGFILE"
    fi
    echo "==> dmesg (last 30 lines):"
    sudo dmesg | tail -30
    {{end}}
    sudo -E HOME=$HOME $LOOPHOLE -p $PROFILE shutdown bench-vol 2>/dev/null || true
    sleep 2
    sudo umount -l $MOUNTPOINT 2>/dev/null || true
    sudo rm -rf $MOUNTPOINT
}
trap cleanup EXIT

sudo dmesg -C
sudo mkdir -p $MOUNTPOINT
sudo chown $(id -u):$(id -g) $MOUNTPOINT
sudo -E HOME=$HOME $LOOPHOLE -p $PROFILE delete -y bench-vol 2>/dev/null || true

echo "==> Creating volume (size={{.VolSize}})..."
sudo -E HOME=$HOME $LOOPHOLE -p $PROFILE create --size {{.VolSize}} --mount $MOUNTPOINT bench-vol &
LOOPHOLE_PID=$!

for i in $(seq 1 60); do
    if mountpoint -q $MOUNTPOINT 2>/dev/null; then
        echo "==> Mounted after ${i}s"
        break
    fi
    sleep 1
done
if ! mountpoint -q $MOUNTPOINT; then
    echo "FATAL: mount failed"
    exit 1
fi
echo "==> Volume mounted at $MOUNTPOINT"
df -h $MOUNTPOINT

{{if .RunFio}}
echo ""
echo "============================================"
echo "  FIO BENCHMARKS"
echo "============================================"
{{range .FioJobs}}
echo "==> fio: {{.Name}} {{.BS}}..."
sudo fio --name={{.Name}} \
    --directory=$MOUNTPOINT \
    --ioengine=sync \
    --rw={{.RW}} \
    --bs={{.BS}} \
    --size={{.Size}} \
    --numjobs={{.NumJobs}} \
    --runtime={{$.FioRuntime}} \
    --time_based \
    --fallocate=none \
    --group_reporting{{if .Extra}} \
    {{.Extra}}{{end}} \
    --output=$RESULTS_DIR/fio-{{.Name}}.json \
    --output-format=json
{{end}}

echo ""
echo "==> FIO SUMMARY:"
for f in $RESULTS_DIR/fio-*.json; do
    name=$(basename $f .json)
    echo "--- $name ---"
    python3 -c "
import json, sys
d = json.load(open('$f'))
for job in d['jobs']:
    r = job.get('read', {})
    w = job.get('write', {})
    if r.get('bw_bytes', 0): print(f\"  read:  {r['bw_bytes']/1024/1024:.1f} MB/s  iops={r['iops']:.0f}  lat_avg={r['lat_ns']['mean']/1000:.0f}us\")
    if w.get('bw_bytes', 0): print(f\"  write: {w['bw_bytes']/1024/1024:.1f} MB/s  iops={w['iops']:.0f}  lat_avg={w['lat_ns']['mean']/1000:.0f}us\")
" 2>/dev/null || echo "  (parse error)"
done
{{end}}

{{if .RunFsx}}
echo ""
echo "============================================"
echo "  FSX STRESS TESTS"
echo "============================================"

echo "==> fsx: basic (5000 ops, 1MB file)..."
sudo fsx -N 5000 -l 1048576 -S 42 $MOUNTPOINT/fsx-basic 2>&1 | tail -3

echo "==> fsx: heavy (10000 ops, 4MB file, hole punching)..."
sudo fsx -H -N 10000 -l 4194304 -S 999 $MOUNTPOINT/fsx-heavy 2>&1 | tail -3

echo "==> fsx: extended (25000 ops, 8MB file)..."
sudo fsx -N 25000 -l 8388608 -S 7777 $MOUNTPOINT/fsx-extended 2>&1 | tail -3

echo "==> FSX: all passed"
{{end}}

{{if .RunE2E}}
echo ""
echo "============================================"
echo "  E2E GO BENCHMARKS"
echo "============================================"

cleanup

export S3_ENDPOINT=https://storage.googleapis.com
export BUCKET={{.Bucket}}
export AWS_REGION=auto

cd $HOME/{{.SrcDir}}
LOG_LEVEL=error go test -tags "" -bench=. -run='^$' -benchmem -count=3 -timeout 600s ./e2e/ 2>&1 | tee $RESULTS_DIR/e2e-bench.txt
{{end}}

echo ""
echo "============================================"
echo "  ALL BENCHMARKS COMPLETE"
echo "============================================"
echo "Results in $RESULTS_DIR/"
ls -la $RESULTS_DIR/
`

// --- ssh subcommand ---

func sshCmd(cfgFn func() *benchConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "ssh [-- command...]",
		Short: "SSH into the bench instance",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cfgFn()
			ctx := cmd.Context()
			if len(args) > 0 {
				return cfg.sshExec(ctx, strings.Join(args, " "))
			}
			return cfg.sshInteractive(ctx)
		},
	}
}

// --- destroy subcommand ---

func destroyCmd(cfgFn func() *benchConfig) *cobra.Command {
	var keepBucket bool
	cmd := &cobra.Command{
		Use:   "destroy",
		Short: "Destroy the GCE instance (and optionally the bucket)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := cfgFn()
			ctx := cmd.Context()
			return cfg.doDestroy(ctx, keepBucket)
		},
	}
	cmd.Flags().BoolVar(&keepBucket, "keep-bucket", false, "don't delete the GCS bucket")
	return cmd
}

func (c *benchConfig) doDestroy(ctx context.Context, keepBucket bool) error {
	fmt.Printf("==> Deleting GCE instance %s...\n", c.instance)
	if err := c.deleteInstance(ctx); err != nil {
		fmt.Printf("warning: delete instance: %v\n", err)
	}

	if !keepBucket {
		fmt.Printf("==> Deleting GCS bucket gs://%s...\n", c.bucket)
		if err := c.deleteBucket(ctx); err != nil {
			fmt.Printf("warning: delete bucket: %v\n", err)
		}
	}

	fmt.Println("==> Destroyed")
	return nil
}

func (c *benchConfig) deleteInstance(ctx context.Context) error {
	client, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return err
	}
	defer util.SafeClose(client, "close compute client")

	op, err := client.Delete(ctx, &computepb.DeleteInstanceRequest{
		Project:  c.project,
		Zone:     c.zone,
		Instance: c.instance,
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (c *benchConfig) deleteBucket(ctx context.Context) error {
	bkt, err := blob.OpenBucket(ctx, fmt.Sprintf("gs://%s", c.bucket))
	if err != nil {
		return err
	}
	defer util.SafeClose(bkt, "close bucket")

	// Delete all objects first.
	iter := bkt.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			break
		}
		if err := bkt.Delete(ctx, obj.Key); err != nil {
			slog.Warn("delete object", "key", obj.Key, "error", err)
		}
	}

	// go-cloud doesn't support bucket deletion, fall back to gcloud.
	cmd := exec.CommandContext(ctx, "gcloud", "storage", "buckets", "delete",
		fmt.Sprintf("gs://%s", c.bucket), "--quiet")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// findRepoRoot walks up from cwd to find go.mod.
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find go.mod in any parent directory")
		}
		dir = parent
	}
}
