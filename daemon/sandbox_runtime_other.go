//go:build !linux

package daemon

import (
	"context"
	"fmt"
)

type unsupportedSandboxRuntime struct{}

func (unsupportedSandboxRuntime) Exec(_ context.Context, _, _ string) (ExecResult, error) {
	return ExecResult{}, fmt.Errorf("sandbox exec not supported on this platform")
}

func newSandboxRuntime(_ *Daemon) (SandboxRuntime, error) {
	return unsupportedSandboxRuntime{}, nil
}
