//go:build linux

package daemon

import (
	"fmt"
	"log/slog"
	"os"
	"syscall"
)

// tuneProcess applies best-effort performance tuning to the daemon process:
// high scheduling priority, OOM killer protection, and increased file descriptor limit.
func tuneProcess(log *slog.Logger) {
	// Raise file descriptor limit to 100k.
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil {
		want := uint64(100_000)
		if rlim.Cur < want {
			rlim.Cur = want
			if rlim.Max < want {
				rlim.Max = want
			}
			if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
				log.Warn("failed to raise RLIMIT_NOFILE", "want", want, "error", err)
			} else {
				log.Info("raised RLIMIT_NOFILE", "nofile", want)
			}
		}
	}

	// Set high scheduling priority (nice -19). Best-effort, requires root.
	if err := syscall.Setpriority(syscall.PRIO_PROCESS, 0, -19); err != nil {
		log.Debug("failed to set nice priority", "error", err)
	} else {
		log.Info("set process priority", "nice", -19)
	}

	// Protect from OOM killer. Best-effort, requires root.
	if err := os.WriteFile(fmt.Sprintf("/proc/%d/oom_score_adj", os.Getpid()), []byte("-1000"), 0o644); err != nil {
		log.Debug("failed to set OOM score", "error", err)
	} else {
		log.Info("set OOM score adjustment", "oom_score_adj", -1000)
	}
}
