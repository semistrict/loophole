//go:build linux

package e2e

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

// TestE2E_Torture is a multi-phase torture test that exercises snapshot, clone,
// remount, overwrite, delete, and data integrity across a branching tree of
// volumes. The scenario:
//
//	root ──snapshot "s1"──► clone-a ──snapshot "s2"──► clone-b
//	  │                       │                          │
//	  │ (continues writing)   │ (modifies clone-a)       │ (modifies clone-b)
//	  ▼                       ▼                          ▼
//	unmount/remount        verify isolation           verify isolation
//
// At every step we verify data integrity, ownership, and isolation between branches.
func TestE2E_Torture(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()

	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())

	// --- Phase 1: Create root volume, populate with diverse data ---

	rootMP := mountpoint(t, "tort-root")
	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "tort-root"}))
	require.NoError(t, b.Mount(ctx, "tort-root", rootMP))
	rootFS := newTestFS(t, b, rootMP)

	// Write files of various sizes and types.
	rootFS.MkdirAll(t, "deep/nested/path/here")
	rootFS.WriteFile(t, "tiny.txt", []byte("tiny\n"))
	rootFS.WriteFile(t, "deep/nested/path/here/buried.txt", []byte("buried treasure\n"))

	// 1MB random file — we'll track its hash across snapshots.
	blob1 := make([]byte, 1*1024*1024)
	_, err := rand.Read(blob1)
	require.NoError(t, err)
	rootFS.WriteFile(t, "blob1.bin", blob1)
	blob1Hash := sha256hex(blob1)

	// 5MB random file.
	blob5 := make([]byte, 5*1024*1024)
	_, err = rand.Read(blob5)
	require.NoError(t, err)
	rootFS.WriteFile(t, "blob5.bin", blob5)
	blob5Hash := sha256hex(blob5)

	// File we'll delete later — should survive in snapshot.
	rootFS.WriteFile(t, "doomed.txt", []byte("I will be deleted\n"))

	// File we'll overwrite later — snapshot should have original.
	rootFS.WriteFile(t, "overwrite-me.txt", []byte("original content\n"))

	// Symlink.
	require.NoError(t, os.Symlink("tiny.txt", filepath.Join(rootMP, "link-to-tiny")))

	// 100 small files in a directory.
	rootFS.MkdirAll(t, "many")
	for i := range 100 {
		rootFS.WriteFile(t, fmt.Sprintf("many/file-%03d.txt", i), []byte(fmt.Sprintf("file %d\n", i)))
	}

	// Verify ownership on a few items.
	assertOwnership(t, rootMP, uid, gid)
	assertOwnership(t, filepath.Join(rootMP, "deep"), uid, gid)
	assertOwnership(t, filepath.Join(rootMP, "tiny.txt"), uid, gid)

	// --- Phase 2: Snapshot + Clone-A ---

	cloneAMP := mountpoint(t, "tort-clone-a")
	require.NoError(t, b.Clone(ctx, rootMP, "tort-clone-a", cloneAMP))
	cloneAFS := newTestFS(t, b, cloneAMP)

	// Verify clone-a has all root data.
	require.Equal(t, "tiny\n", string(cloneAFS.ReadFile(t, "tiny.txt")))
	require.Equal(t, "buried treasure\n", string(cloneAFS.ReadFile(t, "deep/nested/path/here/buried.txt")))
	require.Equal(t, blob1Hash, sha256hex(cloneAFS.ReadFile(t, "blob1.bin")))
	require.Equal(t, blob5Hash, sha256hex(cloneAFS.ReadFile(t, "blob5.bin")))
	require.Equal(t, "I will be deleted\n", string(cloneAFS.ReadFile(t, "doomed.txt")))
	require.Equal(t, "original content\n", string(cloneAFS.ReadFile(t, "overwrite-me.txt")))
	verifyManyFiles(t, cloneAFS, 100)

	// Verify symlink survived clone.
	target, err := os.Readlink(filepath.Join(cloneAMP, "link-to-tiny"))
	require.NoError(t, err)
	require.Equal(t, "tiny.txt", target)

	// --- Phase 3: Mutate root — delete, overwrite, add new ---

	rootFS.Remove(t, "doomed.txt")
	require.False(t, rootFS.Exists(t, "doomed.txt"))

	rootFS.WriteFile(t, "overwrite-me.txt", []byte("mutated in root\n"))
	rootFS.WriteFile(t, "root-only.txt", []byte("only in root\n"))

	// Overwrite blob1 with new random data.
	blob1v2 := make([]byte, 1*1024*1024)
	_, err = rand.Read(blob1v2)
	require.NoError(t, err)
	rootFS.WriteFile(t, "blob1.bin", blob1v2)
	blob1v2Hash := sha256hex(blob1v2)
	require.NotEqual(t, blob1Hash, blob1v2Hash)

	// --- Phase 4: Verify clone-a is isolated from root mutations ---

	require.Equal(t, "I will be deleted\n", string(cloneAFS.ReadFile(t, "doomed.txt")),
		"clone-a should still have doomed.txt")
	require.Equal(t, "original content\n", string(cloneAFS.ReadFile(t, "overwrite-me.txt")),
		"clone-a should have original overwrite-me.txt")
	require.Equal(t, blob1Hash, sha256hex(cloneAFS.ReadFile(t, "blob1.bin")),
		"clone-a should have original blob1")
	require.False(t, cloneAFS.Exists(t, "root-only.txt"),
		"clone-a should not have root-only.txt")

	// --- Phase 5: Mutate clone-a ---

	cloneAFS.WriteFile(t, "clone-a-only.txt", []byte("exclusive to clone-a\n"))
	cloneAFS.Remove(t, "many/file-050.txt")
	cloneAFS.WriteFile(t, "many/file-050.txt", []byte("replaced in clone-a\n"))
	cloneAFS.MkdirAll(t, "clone-a-dir/sub")
	cloneAFS.WriteFile(t, "clone-a-dir/sub/new.txt", []byte("new in clone-a\n"))

	// --- Phase 6: Second snapshot — clone clone-a → clone-b ---

	cloneBMP := mountpoint(t, "tort-clone-b")
	require.NoError(t, b.Clone(ctx, cloneAMP, "tort-clone-b", cloneBMP))
	cloneBFS := newTestFS(t, b, cloneBMP)

	// clone-b should have everything from clone-a's current state.
	require.Equal(t, "exclusive to clone-a\n", string(cloneBFS.ReadFile(t, "clone-a-only.txt")))
	require.Equal(t, "replaced in clone-a\n", string(cloneBFS.ReadFile(t, "many/file-050.txt")))
	require.Equal(t, "new in clone-a\n", string(cloneBFS.ReadFile(t, "clone-a-dir/sub/new.txt")))
	require.Equal(t, "I will be deleted\n", string(cloneBFS.ReadFile(t, "doomed.txt")),
		"clone-b inherits doomed.txt from clone-a")
	require.Equal(t, blob1Hash, sha256hex(cloneBFS.ReadFile(t, "blob1.bin")),
		"clone-b should have original blob1 (inherited from clone-a)")

	// --- Phase 7: Mutate clone-b, verify isolation from clone-a ---

	cloneBFS.WriteFile(t, "clone-b-only.txt", []byte("exclusive to clone-b\n"))
	cloneBFS.Remove(t, "doomed.txt")
	// Truncate and rewrite blob5.
	cloneBFS.WriteFile(t, "blob5.bin", []byte("small now\n"))

	// clone-a should not see clone-b's changes.
	require.False(t, cloneAFS.Exists(t, "clone-b-only.txt"))
	require.Equal(t, "I will be deleted\n", string(cloneAFS.ReadFile(t, "doomed.txt")),
		"clone-a should still have doomed.txt after clone-b deleted it")
	require.Equal(t, blob5Hash, sha256hex(cloneAFS.ReadFile(t, "blob5.bin")),
		"clone-a should have original blob5")

	// --- Phase 8: Unmount root, remount, verify root state ---

	require.NoError(t, b.Unmount(ctx, rootMP))
	require.NoError(t, b.Mount(ctx, "tort-root", rootMP))
	rootFS2 := newTestFS(t, b, rootMP)

	require.False(t, rootFS2.Exists(t, "doomed.txt"), "root deleted doomed.txt")
	require.Equal(t, "mutated in root\n", string(rootFS2.ReadFile(t, "overwrite-me.txt")))
	require.Equal(t, "only in root\n", string(rootFS2.ReadFile(t, "root-only.txt")))
	require.Equal(t, blob1v2Hash, sha256hex(rootFS2.ReadFile(t, "blob1.bin")),
		"root should have v2 blob1 after remount")
	require.Equal(t, blob5Hash, sha256hex(rootFS2.ReadFile(t, "blob5.bin")),
		"root blob5 was never modified")
	require.False(t, rootFS2.Exists(t, "clone-a-only.txt"),
		"root should not have clone-a files")

	// Verify 100 files in root — all still original.
	verifyManyFiles(t, rootFS2, 100)

	// --- Phase 9: Unmount clone-a, remount, full verification ---

	require.NoError(t, b.Unmount(ctx, cloneAMP))
	require.NoError(t, b.Mount(ctx, "tort-clone-a", cloneAMP))
	cloneAFS2 := newTestFS(t, b, cloneAMP)

	require.Equal(t, "I will be deleted\n", string(cloneAFS2.ReadFile(t, "doomed.txt")))
	require.Equal(t, "original content\n", string(cloneAFS2.ReadFile(t, "overwrite-me.txt")))
	require.Equal(t, blob1Hash, sha256hex(cloneAFS2.ReadFile(t, "blob1.bin")))
	require.Equal(t, blob5Hash, sha256hex(cloneAFS2.ReadFile(t, "blob5.bin")))
	require.Equal(t, "exclusive to clone-a\n", string(cloneAFS2.ReadFile(t, "clone-a-only.txt")))
	require.Equal(t, "replaced in clone-a\n", string(cloneAFS2.ReadFile(t, "many/file-050.txt")))
	require.Equal(t, "new in clone-a\n", string(cloneAFS2.ReadFile(t, "clone-a-dir/sub/new.txt")))
	require.False(t, cloneAFS2.Exists(t, "clone-b-only.txt"))

	// Verify the 100 files — only file-050 was replaced.
	for i := range 100 {
		name := fmt.Sprintf("many/file-%03d.txt", i)
		data := string(cloneAFS2.ReadFile(t, name))
		if i == 50 {
			require.Equal(t, "replaced in clone-a\n", data)
		} else {
			require.Equal(t, fmt.Sprintf("file %d\n", i), data)
		}
	}

	// --- Phase 10: Unmount clone-b, remount, verify ---

	require.NoError(t, b.Unmount(ctx, cloneBMP))
	require.NoError(t, b.Mount(ctx, "tort-clone-b", cloneBMP))
	cloneBFS2 := newTestFS(t, b, cloneBMP)

	require.Equal(t, "exclusive to clone-b\n", string(cloneBFS2.ReadFile(t, "clone-b-only.txt")))
	require.False(t, cloneBFS2.Exists(t, "doomed.txt"), "clone-b deleted doomed.txt")
	require.Equal(t, "small now\n", string(cloneBFS2.ReadFile(t, "blob5.bin")),
		"clone-b truncated blob5")
	require.Equal(t, blob1Hash, sha256hex(cloneBFS2.ReadFile(t, "blob1.bin")),
		"clone-b inherits original blob1")
	require.Equal(t, "exclusive to clone-a\n", string(cloneBFS2.ReadFile(t, "clone-a-only.txt")),
		"clone-b inherits clone-a-only.txt")

	// --- Phase 11: Stress the directory layer — mass create/delete/rename ---

	rootFS2.MkdirAll(t, "stress")
	for i := range 200 {
		rootFS2.WriteFile(t, fmt.Sprintf("stress/s-%04d.dat", i),
			[]byte(strings.Repeat(fmt.Sprintf("%04d", i), 256)))
	}
	// Delete odd-numbered files.
	for i := range 200 {
		if i%2 == 1 {
			rootFS2.Remove(t, fmt.Sprintf("stress/s-%04d.dat", i))
		}
	}
	// Rename even files.
	for i := range 200 {
		if i%2 == 0 {
			old := filepath.Join(rootMP, fmt.Sprintf("stress/s-%04d.dat", i))
			renamed := filepath.Join(rootMP, fmt.Sprintf("stress/r-%04d.dat", i))
			require.NoError(t, os.Rename(old, renamed))
		}
	}
	// Verify only renamed even files exist.
	entries := rootFS2.ReadDir(t, "stress")
	require.Equal(t, 100, len(entries), "should have exactly 100 renamed files")
	for _, e := range entries {
		require.True(t, strings.HasPrefix(e, "r-"), "all files should be renamed: %s", e)
	}

	// --- Phase 12: Deep clone chain — clone-b → clone-c → clone-d ---

	cloneCMP := mountpoint(t, "tort-clone-c")
	require.NoError(t, b.Clone(ctx, cloneBMP, "tort-clone-c", cloneCMP))
	cloneCFS := newTestFS(t, b, cloneCMP)

	cloneDMP := mountpoint(t, "tort-clone-d")
	require.NoError(t, b.Clone(ctx, cloneCMP, "tort-clone-d", cloneDMP))
	cloneDFS := newTestFS(t, b, cloneDMP)

	// Write unique data at each level.
	cloneCFS.WriteFile(t, "clone-c.txt", []byte("level-c\n"))
	cloneDFS.WriteFile(t, "clone-d.txt", []byte("level-d\n"))

	// clone-d should see everything from the full chain.
	require.Equal(t, "tiny\n", string(cloneDFS.ReadFile(t, "tiny.txt")),
		"clone-d inherits from root via b→c→d")
	require.Equal(t, "exclusive to clone-b\n", string(cloneDFS.ReadFile(t, "clone-b-only.txt")),
		"clone-d inherits from clone-b")
	require.Equal(t, "level-d\n", string(cloneDFS.ReadFile(t, "clone-d.txt")))
	// clone-d should NOT see clone-c.txt yet — it was written after the clone.
	require.False(t, cloneDFS.Exists(t, "clone-c.txt"),
		"clone-d should not see clone-c.txt written after snapshot")

	// Unmount the whole chain in reverse order, remount clone-d, verify.
	require.NoError(t, b.Unmount(ctx, cloneDMP))
	require.NoError(t, b.Unmount(ctx, cloneCMP))
	require.NoError(t, b.Unmount(ctx, cloneBMP))

	require.NoError(t, b.Mount(ctx, "tort-clone-d", cloneDMP))
	cloneDFS2 := newTestFS(t, b, cloneDMP)

	require.Equal(t, "level-d\n", string(cloneDFS2.ReadFile(t, "clone-d.txt")))
	require.Equal(t, "tiny\n", string(cloneDFS2.ReadFile(t, "tiny.txt")))
	require.Equal(t, "exclusive to clone-b\n", string(cloneDFS2.ReadFile(t, "clone-b-only.txt")))
	require.Equal(t, "exclusive to clone-a\n", string(cloneDFS2.ReadFile(t, "clone-a-only.txt")))
	require.Equal(t, blob1Hash, sha256hex(cloneDFS2.ReadFile(t, "blob1.bin")),
		"clone-d still has original blob1 through full chain")

	t.Log("torture test passed — all 12 phases verified")
}

func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func assertOwnership(t *testing.T, path string, uid, gid uint32) {
	t.Helper()
	var st syscall.Stat_t
	require.NoError(t, syscall.Lstat(path, &st))
	require.Equal(t, uid, st.Uid, "uid mismatch for %s", path)
	require.Equal(t, gid, st.Gid, "gid mismatch for %s", path)
}

func verifyManyFiles(t *testing.T, tfs testFS, n int) {
	t.Helper()
	for i := range n {
		name := fmt.Sprintf("many/file-%03d.txt", i)
		data := string(tfs.ReadFile(t, name))
		require.Equal(t, fmt.Sprintf("file %d\n", i), data, "mismatch in %s", name)
	}
}
