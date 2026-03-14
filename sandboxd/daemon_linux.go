//go:build linux

package sandboxd

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
)

type Options struct {
	SocketPath    string
	LoopholeBin   string
	RunscBin      string
	RunscPlatform string
	RunscDebug    bool
	SelfBin       string
}

type Daemon struct {
	dir                 loophole.Dir
	inst                loophole.Instance
	socketPath          string
	loopholeBin         string
	runscBin            string
	runscPlatform       string
	runscPlatformSource string
	runscDebug          bool
	runscUnsafeNonroot  bool
	selfBin             string
	runscRoot           string
	ln                  net.Listener
	logFile             *os.File

	mu        sync.Mutex
	zygotes   map[string]ZygoteRecord
	sandboxes map[string]SandboxRecord
	sessions  map[string]*session
}

type session struct {
	record         ProcessRecord
	cmd            *execSession
	sandbox        string
	streamAttached bool
}

type execSession struct {
	id     string
	tty    bool
	proc   *os.Process
	events chan processEvent
	stdin  io.Writer
	pty    *os.File
	close  func() error
}

type processEvent struct {
	Stream string `json:"stream"`
	Data   []byte `json:"data,omitempty"`
	Error  string `json:"error,omitempty"`
}

func Start(ctx context.Context, inst loophole.Instance, dir loophole.Dir, opts Options) (*Daemon, error) {
	socketPath := opts.SocketPath
	if socketPath == "" {
		socketPath = dir.SandboxdSocket()
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir.SandboxdState(), 0o755); err != nil {
		return nil, err
	}
	logPath := dir.SandboxdLog()
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open sandboxd log: %w", err)
	}

	log.SetOutput(logFile)
	slog.SetDefault(slog.New(slog.NewJSONHandler(logFile, nil)))

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		util.SafeClose(logFile, "close sandboxd log after socket cleanup failure")
		return nil, fmt.Errorf("remove stale socket %s: %w", socketPath, err)
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		util.SafeClose(logFile, "close sandboxd log after listen failure")
		return nil, fmt.Errorf("listen %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o666); err != nil {
		util.SafeClose(ln, "close sandboxd listener after chmod failure")
		util.SafeClose(logFile, "close sandboxd log after chmod failure")
		return nil, fmt.Errorf("chmod socket %s: %w", socketPath, err)
	}

	state, err := loadState(dir)
	if err != nil {
		util.SafeClose(ln, "close sandboxd listener after load failure")
		util.SafeClose(logFile, "close sandboxd log after load failure")
		return nil, err
	}

	selfBin := opts.SelfBin
	if selfBin == "" {
		selfBin, err = os.Executable()
		if err != nil {
			util.SafeClose(ln, "close sandboxd listener after executable failure")
			util.SafeClose(logFile, "close sandboxd log after executable failure")
			return nil, fmt.Errorf("resolve sandboxd executable: %w", err)
		}
	}

	loopholeBin := opts.LoopholeBin
	if loopholeBin == "" {
		loopholeBin = os.Getenv("LOOPHOLE_BIN")
	}
	if loopholeBin == "" {
		loopholeBin, err = siblingBinary(selfBin, "loophole")
		if err != nil {
			loopholeBin = "loophole"
		}
	}

	runscBin := opts.RunscBin
	if runscBin == "" {
		if env := os.Getenv("RUNSC_BIN"); env != "" {
			runscBin = env
		} else if sibling, siblingErr := siblingBinary(selfBin, "runsc"); siblingErr == nil {
			runscBin = sibling
		} else {
			runscBin = "runsc"
		}
	}

	runscDebug := opts.RunscDebug
	if !runscDebug {
		if env, ok := os.LookupEnv("LOOPHOLE_SANDBOXD_RUNSC_DEBUG"); ok {
			parsed, parseErr := strconv.ParseBool(env)
			if parseErr != nil {
				util.SafeClose(ln, "close sandboxd listener after invalid runsc debug env")
				util.SafeClose(logFile, "close sandboxd log after invalid runsc debug env")
				return nil, fmt.Errorf("parse LOOPHOLE_SANDBOXD_RUNSC_DEBUG: %w", parseErr)
			}
			runscDebug = parsed
		}
	}
	runscUnsafeNonroot := false
	if env, ok := os.LookupEnv("LOOPHOLE_SANDBOXD_RUNSC_UNSAFE_NONROOT"); ok {
		parsed, parseErr := strconv.ParseBool(env)
		if parseErr != nil {
			util.SafeClose(ln, "close sandboxd listener after invalid runsc unsafe env")
			util.SafeClose(logFile, "close sandboxd log after invalid runsc unsafe env")
			return nil, fmt.Errorf("parse LOOPHOLE_SANDBOXD_RUNSC_UNSAFE_NONROOT: %w", parseErr)
		}
		runscUnsafeNonroot = parsed
	}

	runscPlatform := opts.RunscPlatform
	runscPlatformSource := ""
	if runscPlatform == "" {
		if env := os.Getenv("LOOPHOLE_SANDBOXD_RUNSC_PLATFORM"); env != "" {
			runscPlatform = env
			runscPlatformSource = "env"
		} else {
			runscPlatform = defaultRunscPlatform
			runscPlatformSource = "bundled runsc default"
		}
	} else {
		runscPlatformSource = "option"
	}

	d := &Daemon{
		dir:                 dir,
		inst:                inst,
		socketPath:          socketPath,
		loopholeBin:         loopholeBin,
		runscBin:            runscBin,
		runscPlatform:       runscPlatform,
		runscPlatformSource: runscPlatformSource,
		runscDebug:          runscDebug,
		runscUnsafeNonroot:  runscUnsafeNonroot,
		selfBin:             selfBin,
		runscRoot:           filepath.Join(dir.SandboxdState(), "runsc-root"),
		ln:                  ln,
		logFile:             logFile,
		zygotes:             state.Zygotes,
		sandboxes:           state.Sandboxes,
		sessions:            map[string]*session{},
	}
	if err := os.MkdirAll(d.runscRoot, 0o755); err != nil {
		util.SafeClose(ln, "close sandboxd listener after runsc root failure")
		util.SafeClose(logFile, "close sandboxd log after runsc root failure")
		return nil, err
	}
	if err := d.reconcileAll(ctx); err != nil {
		slog.Warn("sandboxd reconcile failed", "error", err)
	}
	return d, nil
}

func (d *Daemon) Serve(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Handler: d.routes()}
	go func() {
		<-ctx.Done()
		if err := srv.Close(); err != nil && err != http.ErrServerClosed {
			slog.Warn("close sandboxd server", "error", err)
		}
	}()

	err := srv.Serve(d.ln)
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	return d.shutdown(context.Background())
}

func (d *Daemon) shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	d.cleanupSandboxes(shutdownCtx)

	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		sessions = append(sessions, s)
	}
	d.mu.Unlock()
	for _, s := range sessions {
		if s.cmd != nil && s.cmd.close != nil {
			if err := s.cmd.close(); err != nil {
				slog.Warn("close session", "session", s.record.ID, "error", err)
			}
		}
	}
	if err := saveState(d.dir, persistedState{Zygotes: d.zygotes, Sandboxes: d.sandboxes}); err != nil {
		slog.Warn("save sandboxd state", "error", err)
	}
	if err := os.Remove(d.socketPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("remove sandboxd socket", "path", d.socketPath, "error", err)
	}
	if d.logFile != nil {
		util.SafeClose(d.logFile, "close sandboxd log file")
	}
	return nil
}

func (d *Daemon) cleanupSandboxes(ctx context.Context) {
	d.mu.Lock()
	ids := make([]string, 0, len(d.sandboxes))
	for id := range d.sandboxes {
		ids = append(ids, id)
	}
	d.mu.Unlock()

	for _, id := range ids {
		if err := d.deleteSandbox(ctx, id); err != nil {
			slog.Warn("delete sandbox on shutdown", "sandbox", id, "error", err)
		}
	}
}

func (d *Daemon) saveLocked() error {
	return saveState(d.dir, persistedState{Zygotes: d.zygotes, Sandboxes: d.sandboxes})
}

func siblingBinary(selfBin string, name string) (string, error) {
	dir := filepath.Dir(selfBin)
	for _, candidate := range []string{
		filepath.Join(dir, name),
		filepath.Join(dir, name+"-"+runtime.GOOS+"-"+runtime.GOARCH),
	} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("no sibling binary %q", name)
}

func newID(prefix string) string {
	return prefix + "_" + uuid.NewString()
}
