//go:build linux

package sandboxd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage2"
)

type ownerHandle struct {
	Socket     string
	Mountpoint string
	Spawned    bool
	PID        int
}

func (d *Daemon) openManager(ctx context.Context) (*storage2.Manager, func(), error) {
	var store loophole.ObjectStore
	var err error
	if d.inst.LocalDir != "" {
		store, err = loophole.NewFileStore(d.inst.LocalDir)
	} else {
		store, err = loophole.NewS3Store(ctx, d.inst)
	}
	if err != nil {
		return nil, nil, err
	}
	vm := storage2.NewVolumeManager(store, d.dir.Cache(d.inst.ProfileName), storage2.Config{}, nil, nil)
	return vm, func() {
		if err := vm.Close(context.Background()); err != nil {
			slog.Warn("close sandboxd manager", "error", err)
		}
	}, nil
}

func (d *Daemon) validateZygoteSource(ctx context.Context, req RegisterZygoteRequest) error {
	if req.Name == "" || req.Volume == "" {
		return fmt.Errorf("name and volume are required")
	}
	vm, cleanup, err := d.openManager(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if req.Checkpoint != "" {
		checkpoints, err := vm.ListCheckpoints(ctx, req.Volume)
		if err != nil {
			return err
		}
		for _, cp := range checkpoints {
			if cp.ID == req.Checkpoint {
				return nil
			}
		}
		return fmt.Errorf("checkpoint %q not found for volume %q", req.Checkpoint, req.Volume)
	}
	vol, err := vm.OpenVolume(req.Volume)
	if err != nil {
		return err
	}
	if !vol.ReadOnly() {
		return fmt.Errorf("zygote volume %q must be frozen/read-only", req.Volume)
	}
	return nil
}

func (d *Daemon) cloneVolume(ctx context.Context, source SourceSpec, cloneName string) error {
	switch source.Kind {
	case SourceKindCheckpoint:
		vm, cleanup, err := d.openManager(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		return vm.CloneFromCheckpoint(ctx, source.Volume, source.Checkpoint, cloneName)
	case SourceKindZygote:
		d.mu.Lock()
		zygote, ok := d.zygotes[source.Zygote]
		d.mu.Unlock()
		if !ok {
			return fmt.Errorf("unknown zygote %q", source.Zygote)
		}
		if zygote.Checkpoint != "" {
			vm, cleanup, err := d.openManager(ctx)
			if err != nil {
				return err
			}
			defer cleanup()
			return vm.CloneFromCheckpoint(ctx, zygote.Volume, zygote.Checkpoint, cloneName)
		}
		tempMount := filepath.Join(d.dir.SandboxdState(), "zygotes", zygote.Name)
		owner, err := d.ensureMountedRootfs(ctx, zygote.Volume, tempMount, false)
		if err != nil {
			return err
		}
		cleanupOwner := func(h ownerHandle) {
			if !h.Spawned {
				return
			}
			if err := d.stopOwner(context.Background(), h.Socket); err != nil {
				slog.Warn("stop temporary zygote owner", "zygote", zygote.Name, "error", err)
			}
		}
		c := client.NewFromSocket(owner.Socket)
		err = c.Clone(ctx, client.CloneParams{Mountpoint: owner.Mountpoint, Clone: cloneName})
		if err == nil {
			cleanupOwner(owner)
			return nil
		}
		if !owner.Spawned && strings.Contains(err.Error(), "not tracked") {
			if stopErr := d.stopOwner(ctx, owner.Socket); stopErr != nil {
				return err
			}
			owner, mountErr := d.ensureMountedRootfs(ctx, zygote.Volume, tempMount, false)
			if mountErr != nil {
				return mountErr
			}
			err = client.NewFromSocket(owner.Socket).Clone(ctx, client.CloneParams{Mountpoint: owner.Mountpoint, Clone: cloneName})
			cleanupOwner(owner)
			return err
		}
		cleanupOwner(owner)
		return err
	case SourceKindSandbox:
		src, err := d.lookupSandbox(source.SandboxID)
		if err != nil {
			return err
		}
		c := client.NewFromSocket(src.OwnerSocket)
		return c.Clone(ctx, client.CloneParams{Mountpoint: src.Mountpoint, Clone: cloneName})
	case SourceKindVolume:
		if source.Mode == "attach" {
			return fmt.Errorf("attach source cannot be cloned directly")
		}
		socket := d.dir.VolumeSocket(source.Volume)
		c := client.NewFromSocket(socket)
		timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		if status, err := c.Status(timeoutCtx); err == nil && status.Mountpoint != "" {
			return c.Clone(ctx, client.CloneParams{Mountpoint: status.Mountpoint, Clone: cloneName})
		}
		vm, cleanup, err := d.openManager(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		v, err := vm.OpenVolume(source.Volume)
		if err != nil {
			return err
		}
		return v.Clone(cloneName)
	default:
		return fmt.Errorf("unsupported source kind %q", source.Kind)
	}
}

func (d *Daemon) deleteVolume(ctx context.Context, volume string) error {
	vm, cleanup, err := d.openManager(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	return vm.DeleteVolume(ctx, volume)
}

func (d *Daemon) ensureMountedRootfs(ctx context.Context, volume string, preferredMountpoint string, adopt bool) (ownerHandle, error) {
	socket := d.dir.VolumeSocket(volume)
	c := client.NewFromSocket(socket)
	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if status, err := c.Status(timeoutCtx); err == nil {
		if status.Mountpoint != "" {
			return ownerHandle{
				Socket:     socket,
				Mountpoint: status.Mountpoint,
				Spawned:    false,
				PID:        0,
			}, nil
		}
		// A live owner with no tracked mount is stale after freeze/unmount.
		// Stop it before reusing the socket path for a fresh mount owner.
		if err := d.stopOwner(ctx, socket); err != nil {
			return ownerHandle{}, err
		}
	}

	if adopt && preferredMountpoint == "" {
		return ownerHandle{}, fmt.Errorf("volume %q is not currently mounted", volume)
	}
	if preferredMountpoint == "" {
		preferredMountpoint = filepath.Join(d.dir.SandboxdState(), "mounts", volume)
	}
	if err := os.MkdirAll(filepath.Dir(preferredMountpoint), 0o755); err != nil {
		return ownerHandle{}, err
	}

	logPath := filepath.Join(d.dir.SandboxdState(), "owners", volume+".log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return ownerHandle{}, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return ownerHandle{}, fmt.Errorf("open owner log: %w", err)
	}

	args := []string{}
	if d.inst.ProfileName != "" {
		args = append(args, "-p", d.inst.ProfileName)
	}
	args = append(args, "mount", volume, preferredMountpoint)
	cmd := exec.Command(d.loopholeBin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = daemonSysProcAttr()
	if err := cmd.Start(); err != nil {
		util.SafeClose(logFile, "close owner log after start failure")
		return ownerHandle{}, fmt.Errorf("start owner for %s: %w", volume, err)
	}

	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- cmd.Wait()
		util.SafeClose(logFile, "close owner log after owner exit")
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	for waitCtx.Err() == nil {
		status, err := c.Status(waitCtx)
		if err == nil && status.Mountpoint != "" {
			return ownerHandle{
				Socket:     socket,
				Mountpoint: status.Mountpoint,
				Spawned:    true,
				PID:        cmd.Process.Pid,
			}, nil
		}
		select {
		case err := <-ownerDone:
			return ownerHandle{}, fmt.Errorf("owner for %s exited before ready: %w", volume, err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if cmd.Process != nil {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			slog.Warn("kill owner after readiness timeout", "volume", volume, "error", err)
		}
	}
	<-ownerDone
	return ownerHandle{}, fmt.Errorf("owner for %s did not become ready", volume)
}

func (d *Daemon) stopOwner(ctx context.Context, socket string) error {
	c := client.NewFromSocket(socket)
	if err := c.Shutdown(ctx); err != nil {
		if isNoDaemonError(err) {
			return nil
		}
		if !isOwnerShuttingDownError(err) {
			return err
		}
	}
	if err := c.ShutdownWait(ctx); err != nil && !isNoDaemonError(err) && !isOwnerShuttingDownError(err) {
		return err
	}
	return nil
}

func (d *Daemon) forceStopOwner(ctx context.Context, pid int, socket string, mountpoint string) error {
	if pid <= 0 {
		return fmt.Errorf("force stop owner: missing pid")
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find owner process %d: %w", pid, err)
	}
	if err := proc.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill owner process %d: %w", pid, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := os.Stat(socket); err != nil {
			if os.IsNotExist(err) {
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if mountpoint != "" {
		if err := syscall.Unmount(mountpoint, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("lazy unmount %s: %w", mountpoint, err)
		}
	}
	return nil
}

func (d *Daemon) breakLeaseForce(ctx context.Context, volume string) error {
	vm, cleanup, err := d.openManager(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	return vm.ForceClearLease(ctx, volume)
}

func (d *Daemon) lookupSandbox(id string) (SandboxRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sb, ok := d.sandboxes[id]
	if !ok {
		return SandboxRecord{}, fmt.Errorf("unknown sandbox %q", id)
	}
	return sb, nil
}

func (d *Daemon) volumeExists(ctx context.Context, volume string) bool {
	vm, cleanup, err := d.openManager(ctx)
	if err != nil {
		return false
	}
	defer cleanup()
	_, err = vm.VolumeInfo(ctx, volume)
	return err == nil
}

func isNoDaemonError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no daemon running")
}

func isOwnerShuttingDownError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "daemon is shutting down")
}
