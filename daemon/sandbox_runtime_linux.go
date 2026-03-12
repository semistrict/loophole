//go:build linux

package daemon

import (
	"context"
	"fmt"

	"github.com/semistrict/loophole"
)

type sandboxRuntimeCloner interface {
	SnapshotVM(ctx context.Context, volume string) (*VMSnapshot, error)
	RestoreVM(ctx context.Context, snap *VMSnapshot, cloneName string) (*VMCloneResult, error)
}

func newSandboxRuntime(d *Daemon) (SandboxRuntime, error) {
	mode := d.inst.SandboxMode
	if mode == "" {
		mode = loophole.DefaultSandboxMode()
	}

	switch mode {
	case loophole.SandboxModeChroot:
		return newChrootSandboxRuntime(d.backend), nil
	case loophole.SandboxModeFirecracker:
		return newFirecrackerSandboxRuntime(d)
	default:
		return nil, fmt.Errorf("unsupported sandbox_mode %q", mode)
	}
}
