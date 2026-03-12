//go:build linux

package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/fsbackend"
)

const (
	firecrackerGuestBinaryPath = "sbin/loophole-guest-agent"
	firecrackerVsockPort       = 4040
)

type firecrackerSandboxRuntime struct {
	backend fsbackend.Service
	vmMgr   *firecrackerVMManager
}

type firecrackerRuntimeConfig struct {
	firecrackerBin string
	kernelPath     string
	workRoot       string
	configDir      string
	profile        string
	bootTimeout    time.Duration
	memMiB         int
	vcpuCount      int
	vsockPort      uint32
}

func newFirecrackerSandboxRuntime(d *Daemon) (SandboxRuntime, error) {
	cfg, err := loadFirecrackerRuntimeConfig(d.inst, d.dir)
	if err != nil {
		return nil, err
	}
	return &firecrackerSandboxRuntime{
		backend: d.backend,
		vmMgr:   newFirecrackerVMManager(cfg),
	}, nil
}

func (r *firecrackerSandboxRuntime) Exec(ctx context.Context, volume string, cmd string) (ExecResult, error) {
	if volume == "" {
		result, err := hostExec(ctx, cmd)
		if err != nil {
			return ExecResult{}, err
		}
		return *result, nil
	}
	if r.backend == nil {
		return ExecResult{}, fmt.Errorf("storage backend is not available")
	}

	if !r.vmMgr.HasRunning(volume) {
		slog.Info("firecracker sandbox: preparing volume", "volume", volume)
		if err := r.prepareVolume(ctx, volume); err != nil {
			return ExecResult{}, err
		}
	}

	slog.Info("firecracker sandbox: ensuring VM", "volume", volume)
	vm, err := r.vmMgr.GetOrStart(ctx, volume)
	if err != nil {
		return ExecResult{}, err
	}

	slog.Info("firecracker sandbox: exec over vsock", "volume", volume)
	client := newVsockExecClient(vm.vsockPath, r.vmMgr.cfg.vsockPort)
	result, err := client.Exec(ctx, cmd)
	if err != nil {
		return ExecResult{}, fmt.Errorf("vsock exec failed: %w", err)
	}
	return result, nil
}

func (r *firecrackerSandboxRuntime) Close(ctx context.Context) error {
	return r.vmMgr.Close(ctx)
}

func (r *firecrackerSandboxRuntime) DebugInfo() any {
	return map[string]any{
		"type": "firecracker",
		"firecracker": map[string]any{
			"bin":          r.vmMgr.cfg.firecrackerBin,
			"kernel":       r.vmMgr.cfg.kernelPath,
			"work_root":    r.vmMgr.cfg.workRoot,
			"boot_timeout": r.vmMgr.cfg.bootTimeout.String(),
			"mem_mib":      r.vmMgr.cfg.memMiB,
			"vcpus":        r.vmMgr.cfg.vcpuCount,
			"vsock_port":   r.vmMgr.cfg.vsockPort,
		},
		"vms": r.vmMgr.DebugInfo(),
	}
}

func (r *firecrackerSandboxRuntime) DebugLog(volume string, lines int) (map[string]any, error) {
	return r.vmMgr.DebugLog(volume, lines)
}

func (r *firecrackerSandboxRuntime) SnapshotVM(ctx context.Context, volume string) (*VMSnapshot, error) {
	return r.vmMgr.SnapshotVM(ctx, volume)
}

func (r *firecrackerSandboxRuntime) RestoreVM(ctx context.Context, snap *VMSnapshot, cloneName string) (*VMCloneResult, error) {
	return r.vmMgr.RestoreVM(ctx, snap, cloneName)
}

func (r *firecrackerSandboxRuntime) prepareVolume(_ context.Context, volume string) error {
	slog.Info("firecracker sandbox: skip host-side guest provisioning", "volume", volume)
	slog.Warn("firecracker sandbox runtime expects guest agent to be present in image", "path", firecrackerGuestBinaryPath)
	return nil
}

func loadFirecrackerRuntimeConfig(inst loophole.Instance, dir loophole.Dir) (firecrackerRuntimeConfig, error) {
	workRoot := filepath.Join(string(dir), "firecracker", inst.ProfileName)
	if env := os.Getenv("LOOPHOLE_FIRECRACKER_WORK_ROOT"); env != "" {
		workRoot = env
	}

	firecrackerBin, err := findExistingPath(
		os.Getenv("LOOPHOLE_FIRECRACKER_BIN"),
		filepath.Join(os.Getenv("LOOPHOLE_FIRECRACKER_WORK_DIR"), "firecracker", "build", "cargo_target", "debug", "firecracker"),
		"/usr/local/bin/firecracker",
	)
	if err != nil {
		return firecrackerRuntimeConfig{}, fmt.Errorf("locate firecracker binary: %w", err)
	}

	kernelPath, err := findExistingPath(
		os.Getenv("LOOPHOLE_FIRECRACKER_KERNEL"),
		filepath.Join(os.Getenv("LOOPHOLE_FIRECRACKER_WORK_DIR"), "vmlinux"),
		"/usr/local/share/loophole/vmlinux",
	)
	if err != nil {
		return firecrackerRuntimeConfig{}, fmt.Errorf("locate firecracker kernel: %w", err)
	}

	return firecrackerRuntimeConfig{
		firecrackerBin: firecrackerBin,
		kernelPath:     kernelPath,
		workRoot:       workRoot,
		configDir:      string(dir),
		profile:        inst.ProfileName,
		bootTimeout:    durationEnv("LOOPHOLE_FIRECRACKER_BOOT_TIMEOUT", 20*time.Second),
		memMiB:         intEnv("LOOPHOLE_FIRECRACKER_MEM_MIB", 256),
		vcpuCount:      intEnv("LOOPHOLE_FIRECRACKER_VCPUS", 1),
		vsockPort:      uint32(intEnv("LOOPHOLE_FIRECRACKER_VSOCK_PORT", firecrackerVsockPort)),
	}, nil
}

func findExistingPath(paths ...string) (string, error) {
	for _, candidate := range paths {
		if candidate == "" {
			continue
		}
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no usable path found")
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
