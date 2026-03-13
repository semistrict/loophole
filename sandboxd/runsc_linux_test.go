//go:build linux

package sandboxd

import (
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole"
)

func TestRunscArgsDefault(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		dir:       loophole.Dir(t.TempDir()),
		runscRoot: "/tmp/runsc-root",
	}

	args := d.runscArgs("", "state", "runsc-123")
	assertRunscArgsContain(t, args,
		"--root=/tmp/runsc-root",
		"--network="+networkModeHost,
		"--ignore-cgroups=true",
		"--file-access=shared",
		"--overlay2=none",
		"state",
		"runsc-123",
	)
	assertRunscArgsAbsent(t, args, "--debug")
	assertRunscArgsAbsentPrefix(t, args, "--debug-log=")
	assertRunscArgsAbsentPrefix(t, args, "--panic-log=")
}

func TestRunscArgsWithDebug(t *testing.T) {
	t.Parallel()

	dir := loophole.Dir(t.TempDir())
	d := &Daemon{
		dir:        dir,
		runscRoot:  "/tmp/runsc-root",
		runscDebug: true,
	}

	sandboxID := "sbx_test"
	args := d.runscArgs(sandboxID, "exec", "runsc-123", "/bin/true")
	assertRunscArgsContain(t, args,
		"--root=/tmp/runsc-root",
		"--network="+networkModeHost,
		"--ignore-cgroups=true",
		"--file-access=shared",
		"--overlay2=none",
		"--debug",
		"--debug-log="+filepath.Join(string(dir), "sandboxd", "sandboxes", sandboxID, "runsc-debug")+"/",
		"--panic-log="+filepath.Join(string(dir), "sandboxd", "sandboxes", sandboxID, "runsc-panic.log"),
		"exec",
		"runsc-123",
		"/bin/true",
	)
}

func assertRunscArgsContain(t *testing.T, args []string, want ...string) {
	t.Helper()
	for _, target := range want {
		found := false
		for _, arg := range args {
			if arg == target {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing arg %q in %q", target, args)
		}
	}
}

func assertRunscArgsAbsent(t *testing.T, args []string, target string) {
	t.Helper()
	for _, arg := range args {
		if arg == target {
			t.Fatalf("unexpected arg %q in %q", target, args)
		}
	}
}

func assertRunscArgsAbsentPrefix(t *testing.T, args []string, prefix string) {
	t.Helper()
	for _, arg := range args {
		if len(arg) >= len(prefix) && arg[:len(prefix)] == prefix {
			t.Fatalf("unexpected arg prefix %q in %q", prefix, args)
		}
	}
}
