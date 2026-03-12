//go:build linux

package daemon

func newSandboxRuntime(d *Daemon) (SandboxRuntime, error) {
	return newChrootSandboxRuntime(d.backend), nil
}
