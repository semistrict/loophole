package loophole

const (
	SandboxModeChroot = "chroot"
)

// DefaultSandboxMode returns the default sandbox runtime for /sandbox/exec.
func DefaultSandboxMode() string {
	return SandboxModeChroot
}
