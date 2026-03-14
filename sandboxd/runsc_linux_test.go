//go:build linux

package sandboxd

import (
	"os"
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

func TestBundleSpecRootlessAddsUserNamespaceMappings(t *testing.T) {
	t.Parallel()

	d := &Daemon{runscRootless: true}
	spec, err := d.bundleSpec(SandboxRecord{
		Name:       "sandbox-1",
		Mountpoint: "/tmp/rootfs",
	})
	if err != nil {
		t.Fatalf("bundleSpec() error = %v", err)
	}

	linuxSpec := specMap(t, spec["linux"])
	assertNamespacePresent(t, linuxSpec, "user")

	uidMappings := mappingSlice(t, linuxSpec["uidMappings"])
	if len(uidMappings) != 1 {
		t.Fatalf("uidMappings len = %d, want 1", len(uidMappings))
	}
	assertMapping(t, uidMappings[0], os.Getuid())

	gidMappings := mappingSlice(t, linuxSpec["gidMappings"])
	if len(gidMappings) != 1 {
		t.Fatalf("gidMappings len = %d, want 1", len(gidMappings))
	}
	assertMapping(t, gidMappings[0], os.Getgid())

	enabled, ok := linuxSpec["gidMappingsEnableSetgroups"].(bool)
	if !ok {
		t.Fatalf("gidMappingsEnableSetgroups type = %T, want bool", linuxSpec["gidMappingsEnableSetgroups"])
	}
	if enabled {
		t.Fatalf("gidMappingsEnableSetgroups = true, want false")
	}
}

func TestBundleSpecDefaultOmitsUserNamespaceMappings(t *testing.T) {
	t.Parallel()

	d := &Daemon{}
	spec, err := d.bundleSpec(SandboxRecord{
		Name:       "sandbox-1",
		Mountpoint: "/tmp/rootfs",
	})
	if err != nil {
		t.Fatalf("bundleSpec() error = %v", err)
	}

	linuxSpec := specMap(t, spec["linux"])
	assertNamespaceAbsent(t, linuxSpec, "user")
	if _, ok := linuxSpec["uidMappings"]; ok {
		t.Fatalf("uidMappings unexpectedly present")
	}
	if _, ok := linuxSpec["gidMappings"]; ok {
		t.Fatalf("gidMappings unexpectedly present")
	}
}

func TestBundleSpecBindMounts(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	sock := filepath.Join(tmpDir, "owner.sock")
	if err := os.WriteFile(sock, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	d := &Daemon{
		dir:           loophole.Dir(tmpDir),
		runscRootless: true,
	}
	spec, err := d.bundleSpec(SandboxRecord{
		Name:        "sandbox-1",
		Mountpoint:  "/tmp/rootfs",
		OwnerSocket: sock,
	})
	if err != nil {
		t.Fatalf("bundleSpec() error = %v", err)
	}

	mounts, ok := spec["mounts"].([]map[string]any)
	if !ok {
		t.Fatalf("mounts type = %T, want []map[string]any", spec["mounts"])
	}

	assertMountExists(t, mounts, "/.loophole/bin", d.guestBinDir())
	assertMountExists(t, mounts, "/.loophole/api.sock", sock)
}

func assertMountExists(t *testing.T, mounts []map[string]any, dest, source string) {
	t.Helper()
	for _, m := range mounts {
		if m["destination"] == dest {
			if m["source"] != source {
				t.Fatalf("mount %s source = %v, want %v", dest, m["source"], source)
			}
			return
		}
	}
	t.Fatalf("mount %s not found in mounts", dest)
}

func specMap(t *testing.T, value any) map[string]any {
	t.Helper()
	spec, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value type = %T, want map[string]any", value)
	}
	return spec
}

func mappingSlice(t *testing.T, value any) []map[string]any {
	t.Helper()
	items, ok := value.([]map[string]any)
	if ok {
		return items
	}
	raw, ok := value.([]any)
	if !ok {
		t.Fatalf("mapping slice type = %T, want []map[string]any or []any", value)
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		mapping, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("mapping item type = %T, want map[string]any", item)
		}
		out = append(out, mapping)
	}
	return out
}

func assertMapping(t *testing.T, mapping map[string]any, hostID int) {
	t.Helper()
	if got := intValue(t, mapping["containerID"]); got != 0 {
		t.Fatalf("containerID = %d, want 0", got)
	}
	if got := intValue(t, mapping["hostID"]); got != hostID {
		t.Fatalf("hostID = %d, want %d", got, hostID)
	}
	if got := intValue(t, mapping["size"]); got != 1 {
		t.Fatalf("size = %d, want 1", got)
	}
}

func intValue(t *testing.T, value any) int {
	t.Helper()
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		t.Fatalf("numeric value type = %T, want integer", value)
		return 0
	}
}

func assertNamespacePresent(t *testing.T, linuxSpec map[string]any, want string) {
	t.Helper()
	for _, ns := range namespaceTypes(t, linuxSpec) {
		if ns == want {
			return
		}
	}
	t.Fatalf("namespace %q missing from %#v", want, linuxSpec["namespaces"])
}

func assertNamespaceAbsent(t *testing.T, linuxSpec map[string]any, unwanted string) {
	t.Helper()
	for _, ns := range namespaceTypes(t, linuxSpec) {
		if ns == unwanted {
			t.Fatalf("namespace %q unexpectedly present in %#v", unwanted, linuxSpec["namespaces"])
		}
	}
}

func namespaceTypes(t *testing.T, linuxSpec map[string]any) []string {
	t.Helper()
	raw, ok := linuxSpec["namespaces"].([]map[string]any)
	if ok {
		out := make([]string, 0, len(raw))
		for _, ns := range raw {
			typ, ok := ns["type"].(string)
			if !ok {
				t.Fatalf("namespace type = %T, want string", ns["type"])
			}
			out = append(out, typ)
		}
		return out
	}
	items, ok := linuxSpec["namespaces"].([]any)
	if !ok {
		t.Fatalf("namespaces type = %T, want []map[string]any or []any", linuxSpec["namespaces"])
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		ns, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("namespace item type = %T, want map[string]any", item)
		}
		typ, ok := ns["type"].(string)
		if !ok {
			t.Fatalf("namespace type = %T, want string", ns["type"])
		}
		out = append(out, typ)
	}
	return out
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
