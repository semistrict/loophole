package filecmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/semistrict/loophole/fsbackend"
	"github.com/semistrict/loophole/lwext4"
	"github.com/stretchr/testify/require"
)

type memDev struct {
	data []byte
}

func (m *memDev) Read(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= uint64(len(m.data)) {
		return 0, io.EOF
	}
	return copy(buf, m.data[offset:]), nil
}

func (m *memDev) Write(_ context.Context, data []byte, offset uint64) error {
	copy(m.data[offset:], data)
	return nil
}

func (m *memDev) ZeroRange(_ context.Context, offset, length uint64) error {
	clear(m.data[offset : offset+length])
	return nil
}

func newTestLwext4FS(t *testing.T) fsbackend.FS {
	t.Helper()
	const size = 128 * 1024 * 1024
	dev := &memDev{data: make([]byte, size)}
	ext4fs, err := lwext4.Format(dev, size, nil)
	require.NoError(t, err)
	t.Cleanup(func() { ext4fs.Close() })
	return fsbackend.NewLwext4FS(ext4fs)
}

// TestDotGitInitLwext4 reproduces the go-git filesystem storage initialization
// against lwext4. go-git's DotGit.Initialize calls Stat on paths like
// "objects/info" and expects os.ErrNotExist for missing paths.
func TestDotGitInitLwext4(t *testing.T) {
	fsys := newTestLwext4FS(t)

	worktree := newBillyFS(fsys, "/repo")
	dotgit, err := worktree.Chroot(".git")
	require.NoError(t, err)

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})

	// Init calls DotGit.Initialize which stats objects/info, objects/pack,
	// refs/heads, refs/tags and creates them if missing.
	err = storage.Init()
	require.NoError(t, err)

	// Verify the directories were created.
	_, err = fsys.Stat("/repo/.git/objects/info")
	require.NoError(t, err)
	_, err = fsys.Stat("/repo/.git/objects/pack")
	require.NoError(t, err)
	_, err = fsys.Stat("/repo/.git/refs/heads")
	require.NoError(t, err)
	_, err = fsys.Stat("/repo/.git/refs/tags")
	require.NoError(t, err)
}

// TestGitCloneLocalToLwext4 clones a local git repo into an lwext4-backed billyFS.
func TestGitCloneLocalToLwext4(t *testing.T) {
	// Create a small test repo with real git so we have a valid packfile.
	srcDir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = srcDir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "cmd %v: %s", args, out)
	}
	run("git", "init")
	run("git", "config", "user.email", "test@test.com")
	run("git", "config", "user.name", "Test")
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello world\n"), 0o644))
	run("git", "add", ".")
	run("git", "commit", "-m", "initial")

	fsys := newTestLwext4FS(t)
	worktree := newBillyFS(fsys, "/repo")
	dotgit, err := worktree.Chroot(".git")
	require.NoError(t, err)

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})
	_, err = git.Clone(storage, worktree, &git.CloneOptions{URL: srcDir})
	require.NoError(t, err)

	// Verify the file was checked out.
	data, err := fsys.ReadFile("/repo/hello.txt")
	require.NoError(t, err)
	require.Equal(t, "hello world\n", string(data))

	// Verify we can open the repo and read the log.
	repo, err := git.Open(storage, worktree)
	require.NoError(t, err)
	logIter, err := repo.Log(&git.LogOptions{})
	require.NoError(t, err)
	commit, err := logIter.Next()
	require.NoError(t, err)
	require.Equal(t, "initial\n", commit.Message)
}

// TestGitCloneLargeRepoToLwext4 clones a repo with enough objects to produce
// a multi-object packfile, reproducing the "zlib: invalid checksum" bug.
func TestGitCloneLargeRepoToLwext4(t *testing.T) {
	srcDir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = srcDir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "cmd %v: %s", args, out)
	}
	run("git", "init")
	run("git", "config", "user.email", "test@test.com")
	run("git", "config", "user.name", "Test")

	// Create 100 commits with unique files to generate a sizable packfile.
	for i := range 100 {
		name := filepath.Join(srcDir, fmt.Sprintf("file%03d.txt", i))
		content := fmt.Sprintf("content for file %d\nline 2\nline 3\n", i)
		require.NoError(t, os.WriteFile(name, []byte(content), 0o644))
		run("git", "add", ".")
		run("git", "commit", "-m", fmt.Sprintf("commit %d", i))
	}

	fsys := newTestLwext4FS(t)
	worktree := newBillyFS(fsys, "/repo")
	dotgit, err := worktree.Chroot(".git")
	require.NoError(t, err)

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})
	_, err = git.Clone(storage, worktree, &git.CloneOptions{URL: srcDir})
	require.NoError(t, err)

	// Verify last file exists.
	data, err := fsys.ReadFile("/repo/file099.txt")
	require.NoError(t, err)
	require.Contains(t, string(data), "content for file 99")

	// Verify we can read the log.
	repo, err := git.Open(storage, worktree)
	require.NoError(t, err)
	logIter, err := repo.Log(&git.LogOptions{})
	require.NoError(t, err)
	commit, err := logIter.Next()
	require.NoError(t, err)
	require.Equal(t, "commit 99\n", commit.Message)
}

// TestGitCloneRealRepoToLwext4 clones the loophole repo itself into lwext4.
// This reproduces the "zlib: invalid checksum" seen with large packfiles.
func TestGitCloneRealRepoToLwext4(t *testing.T) {
	repoRoot := findRepoRoot(t)

	const size = 512 * 1024 * 1024 // 512MB for the real repo
	dev := &memDev{data: make([]byte, size)}
	ext4fs, err := lwext4.Format(dev, size, nil)
	require.NoError(t, err)
	t.Cleanup(func() { ext4fs.Close() })
	fsys := fsbackend.NewLwext4FS(ext4fs)

	worktree := newBillyFS(fsys, "/repo")
	dotgit, err := worktree.Chroot(".git")
	require.NoError(t, err)

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})
	_, err = git.Clone(storage, worktree, &git.CloneOptions{
		URL:   repoRoot,
		Depth: 1,
	})
	require.NoError(t, err)

	// Verify a known file exists.
	data, err := fsys.ReadFile("/repo/go.mod")
	require.NoError(t, err)
	require.Contains(t, string(data), "github.com/semistrict/loophole")
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root")
		}
		dir = parent
	}
}

// TestGitCloneExecutableModeClean verifies that after cloning a repo with
// executable files, git status reports a clean worktree (no mode mismatches).
func TestGitCloneExecutableModeClean(t *testing.T) {
	srcDir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = srcDir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "cmd %v: %s", args, out)
	}
	run("git", "init")
	run("git", "config", "user.email", "test@test.com")
	run("git", "config", "user.name", "Test")

	// Create a regular file, an executable script, and a symlink.
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "readme.txt"), []byte("hello\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o755))
	require.NoError(t, os.Symlink("readme.txt", filepath.Join(srcDir, "link.md")))
	run("git", "add", ".")
	run("git", "commit", "-m", "initial")

	fsys := newTestLwext4FS(t)
	worktree := newBillyFS(fsys, "/repo")
	dotgit, err := worktree.Chroot(".git")
	require.NoError(t, err)

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})
	_, err = git.Clone(storage, worktree, &git.CloneOptions{URL: srcDir})
	require.NoError(t, err)

	// Re-open and check status — should be clean.
	repo, err := git.Open(storage, worktree)
	require.NoError(t, err)
	wt, err := repo.Worktree()
	require.NoError(t, err)
	status, err := wt.Status()
	require.NoError(t, err)
	require.True(t, status.IsClean(), "worktree should be clean, got:\n%s", status)
}

// TestBillyStatNotFound verifies that billyFS.Stat returns os.ErrNotExist
// for missing files, which go-git depends on.
func TestBillyStatNotFound(t *testing.T) {
	fsys := newTestLwext4FS(t)
	bfs := newBillyFS(fsys, "/")

	_, err := bfs.Stat("nonexistent")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.True(t, os.IsNotExist(err), "os.IsNotExist should be true, got: %v", err)

	require.NoError(t, bfs.MkdirAll("a/b", 0o755))
	_, err = bfs.Stat("a/b/c")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.True(t, os.IsNotExist(err), "os.IsNotExist should be true, got: %v", err)

	// Reproduce the exact go-git scenario: chroot + nested missing path.
	require.NoError(t, bfs.MkdirAll("repo/.git", 0o755))
	chrootFS, err := bfs.Chroot("repo/.git")
	require.NoError(t, err)
	_, err = chrootFS.Stat("objects/info")
	t.Logf("stat error: %v", err)
	t.Logf("os.IsNotExist: %v", os.IsNotExist(err))
	require.True(t, os.IsNotExist(err), "os.IsNotExist should be true for chroot stat, got: %v", err)
}
