package cached

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/semistrict/loophole/internal/util"
)

// EnsureDaemon ensures a cache daemon is running for the given directory.
// If one is already listening, this is a no-op. Otherwise it spawns
// loophole-cached as a detached subprocess and waits for it to become ready.
func EnsureDaemon(dir string) error {
	sock := SocketPath(dir)

	// Fast path: daemon already running.
	if conn, err := net.Dial("unix", sock); err == nil {
		util.SafeClose(conn, "close daemon probe conn")
		return nil
	}

	bin, err := findCachedBinary()
	if err != nil {
		return fmt.Errorf("find loophole-cached: %w", err)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	cmd := exec.Command(bin, "--dir", dir)
	cmd.Dir = "/"
	cmd.Stdout = nil
	cmd.Stderr = nil
	// Detach the child so it outlives this process.
	setSysProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start loophole-cached: %w", err)
	}
	// Release the process handle — we don't want to wait for it.
	_ = cmd.Process.Release()

	// Poll for the socket to appear.
	for range 100 { // 10 seconds max
		time.Sleep(100 * time.Millisecond)
		if conn, err := net.Dial("unix", sock); err == nil {
			util.SafeClose(conn, "close daemon poll conn")
			return nil
		}
	}
	return fmt.Errorf("loophole-cached did not start within 10s (dir=%s)", dir)
}

// findCachedBinary locates the loophole-cached binary.
// Priority: LOOPHOLE_CACHED_BIN env → same dir as current executable → PATH.
func findCachedBinary() (string, error) {
	if bin := os.Getenv("LOOPHOLE_CACHED_BIN"); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin, nil
		}
	}

	// Look next to the current executable.
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe),
			fmt.Sprintf("loophole-cached-%s-%s", runtime.GOOS, runtime.GOARCH))
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		candidate = filepath.Join(filepath.Dir(exe), "loophole-cached")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return exec.LookPath("loophole-cached")
}
