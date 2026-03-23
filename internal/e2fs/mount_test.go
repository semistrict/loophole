//go:build linux

package e2fs

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// loopMount mounts an ext4 image file via mount -o loop and returns the
// mountpoint. The caller must call the returned cleanup function.
func loopMount(t *testing.T, imgPath string) string {
	t.Helper()

	mp := t.TempDir()
	out, err := exec.Command("mount", "-t", "ext4", "-o", "loop", imgPath, mp).CombinedOutput()
	require.NoError(t, err, "mount -o loop: %s", out)
	t.Cleanup(func() {
		exec.Command("umount", mp).Run()
	})

	return mp
}

func skipIfNotRoot(t *testing.T) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("requires root for loop mount")
	}
}

func TestMount_FullTarRoundTrip(t *testing.T) {
	skipIfNotRoot(t)

	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	type entry struct {
		hdr     tar.Header
		content []byte // for regular files
	}

	entries := []entry{
		// ---- directories with various permissions ----
		{hdr: tar.Header{
			Typeflag: tar.TypeDir, Name: "root-owned/",
			Mode: 0755, Uid: 0, Gid: 0, ModTime: now,
		}},
		{hdr: tar.Header{
			Typeflag: tar.TypeDir, Name: "user-dir/",
			Mode: 0700, Uid: 1000, Gid: 1000, ModTime: now,
		}},
		{hdr: tar.Header{
			Typeflag: tar.TypeDir, Name: "sticky-dir/",
			Mode: 01777, Uid: 0, Gid: 0, ModTime: now,
		}},
		{hdr: tar.Header{
			Typeflag: tar.TypeDir, Name: "setgid-dir/",
			Mode: 02755, Uid: 0, Gid: 100, ModTime: now,
		}},
		{hdr: tar.Header{
			Typeflag: tar.TypeDir, Name: "deep/nested/sub/dir/",
			Mode: 0755, Uid: 0, Gid: 0, ModTime: now,
		}},

		// ---- regular files with various permissions ----
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/hello.txt",
				Mode: 0644, Uid: 0, Gid: 0, ModTime: now,
			},
			content: []byte("hello world\n"),
		},
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "user-dir/private.txt",
				Mode: 0600, Uid: 1000, Gid: 1000, ModTime: now,
			},
			content: []byte("secret data\n"),
		},
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/executable.sh",
				Mode: 0755, Uid: 0, Gid: 0, ModTime: now,
			},
			content: []byte("#!/bin/sh\necho ok\n"),
		},
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/setuid-bin",
				Mode: 04755, Uid: 0, Gid: 0, ModTime: now,
			},
			content: []byte("fake-setuid-binary"),
		},
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/setgid-bin",
				Mode: 02755, Uid: 0, Gid: 50, ModTime: now,
			},
			content: []byte("fake-setgid-binary"),
		},
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "deep/nested/sub/dir/deep-file.txt",
				Mode: 0644, Uid: 0, Gid: 0, ModTime: now,
			},
			content: []byte("deep content\n"),
		},
		// Empty file.
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/empty",
				Mode: 0444, Uid: 0, Gid: 0, ModTime: now,
			},
			content: []byte{},
		},
		// Large file (1 MB) to exercise multi-block writes.
		{
			hdr: tar.Header{
				Typeflag: tar.TypeReg, Name: "root-owned/large.bin",
				Mode: 0644, Uid: 0, Gid: 0, ModTime: now,
			},
			content: func() []byte {
				b := make([]byte, 1024*1024)
				for i := range b {
					b[i] = byte(i % 251)
				}
				return b
			}(),
		},

		// ---- symlinks ----
		{hdr: tar.Header{
			Typeflag: tar.TypeSymlink, Name: "root-owned/link-to-hello",
			Linkname: "hello.txt",
			Uid:      0, Gid: 0, ModTime: now,
		}},
		{hdr: tar.Header{
			Typeflag: tar.TypeSymlink, Name: "root-owned/abs-link",
			Linkname: "/etc/passwd",
			Uid:      0, Gid: 0, ModTime: now,
		}},

		// ---- hard link ----
		{hdr: tar.Header{
			Typeflag: tar.TypeLink, Name: "root-owned/hardlink-to-hello",
			Linkname: "root-owned/hello.txt",
			ModTime:  now,
		}},

		// ---- character device (null) ----
		{hdr: tar.Header{
			Typeflag: tar.TypeChar, Name: "root-owned/dev-null",
			Mode: 0666, Uid: 0, Gid: 0, ModTime: now,
			Devmajor: 1, Devminor: 3,
		}},

		// ---- block device (loop0) ----
		{hdr: tar.Header{
			Typeflag: tar.TypeBlock, Name: "root-owned/dev-loop0",
			Mode: 0660, Uid: 0, Gid: 6, ModTime: now,
			Devmajor: 7, Devminor: 0,
		}},

		// ---- FIFO ----
		{hdr: tar.Header{
			Typeflag: tar.TypeFifo, Name: "root-owned/my-fifo",
			Mode: 0644, Uid: 0, Gid: 0, ModTime: now,
		}},
	}

	// Build tar archive.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		h := e.hdr
		if h.Typeflag == tar.TypeReg || h.Typeflag == tar.TypeRegA {
			h.Size = int64(len(e.content))
		}
		require.NoError(t, tw.WriteHeader(&h), "write header for %s", h.Name)
		if len(e.content) > 0 {
			_, err := tw.Write(e.content)
			require.NoError(t, err, "write content for %s", h.Name)
		}
	}
	require.NoError(t, tw.Close())

	// Create ext4 image.
	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	err := BuildExt4FromTarReader(imgPath, &buf, 128*1024*1024)
	require.NoError(t, err)

	// Mount it.
	mp := loopMount(t, imgPath)

	// Verify every entry.
	for _, e := range entries {
		name := filepath.Clean(e.hdr.Name)
		full := filepath.Join(mp, name)

		// Stat (lstat to not follow symlinks).
		fi, err := os.Lstat(full)
		require.NoError(t, err, "lstat %s", name)

		st := fi.Sys().(*syscall.Stat_t)

		switch e.hdr.Typeflag {
		case tar.TypeDir:
			require.True(t, fi.IsDir(), "%s should be dir", name)
			require.Equal(t, unixModeToGo(e.hdr.Mode)|os.ModeDir, fi.Mode(),
				"mode mismatch for dir %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for %s", name)

		case tar.TypeReg, tar.TypeRegA:
			require.True(t, fi.Mode().IsRegular(), "%s should be regular", name)

			// Check permission bits (including setuid/setgid/sticky).
			require.Equal(t, unixModeToGo(e.hdr.Mode), fi.Mode(),
				"mode mismatch for file %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for %s", name)

			// Check content.
			got, err := os.ReadFile(full)
			require.NoError(t, err, "read %s", name)
			require.Equal(t, e.content, got,
				"content mismatch for %s", name)

		case tar.TypeSymlink:
			require.Equal(t, os.ModeSymlink, fi.Mode()&os.ModeType,
				"%s should be symlink", name)
			target, err := os.Readlink(full)
			require.NoError(t, err, "readlink %s", name)
			require.Equal(t, e.hdr.Linkname, target,
				"symlink target mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for symlink %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for symlink %s", name)

		case tar.TypeLink:
			require.True(t, fi.Mode().IsRegular(), "%s (hardlink) should be regular", name)
			// Verify it shares an inode with the target.
			targetPath := filepath.Join(mp, filepath.Clean(e.hdr.Linkname))
			tfi, err := os.Stat(targetPath)
			require.NoError(t, err, "stat hardlink target %s", e.hdr.Linkname)
			tst := tfi.Sys().(*syscall.Stat_t)
			require.Equal(t, st.Ino, tst.Ino,
				"hardlink %s and %s should share inode", name, e.hdr.Linkname)
			require.True(t, st.Nlink >= 2,
				"hardlink %s should have nlink >= 2, got %d", name, st.Nlink)

		case tar.TypeChar:
			require.Equal(t, os.ModeCharDevice|os.ModeDevice, fi.Mode()&os.ModeType,
				"%s should be char device", name)
			require.Equal(t, unixModeToGo(e.hdr.Mode), fi.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky),
				"mode mismatch for char dev %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for %s", name)
			// Verify major/minor.
			require.Equal(t, uint32(e.hdr.Devmajor), unix_major(st.Rdev),
				"major mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Devminor), unix_minor(st.Rdev),
				"minor mismatch for %s", name)

		case tar.TypeBlock:
			require.Equal(t, os.ModeDevice, fi.Mode()&os.ModeType,
				"%s should be block device", name)
			require.Equal(t, unixModeToGo(e.hdr.Mode), fi.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky),
				"mode mismatch for block dev %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Devmajor), unix_major(st.Rdev),
				"major mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Devminor), unix_minor(st.Rdev),
				"minor mismatch for %s", name)

		case tar.TypeFifo:
			require.Equal(t, os.ModeNamedPipe, fi.Mode()&os.ModeType,
				"%s should be FIFO", name)
			require.Equal(t, unixModeToGo(e.hdr.Mode), fi.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky),
				"mode mismatch for fifo %s", name)
			require.Equal(t, uint32(e.hdr.Uid), st.Uid,
				"uid mismatch for %s", name)
			require.Equal(t, uint32(e.hdr.Gid), st.Gid,
				"gid mismatch for %s", name)
		}

		// Verify mtime for all types (except hardlinks which inherit from target).
		if e.hdr.Typeflag != tar.TypeLink {
			require.Equal(t, e.hdr.ModTime.Unix(), fi.ModTime().Unix(),
				"mtime mismatch for %s", name)
		}
	}
}

func TestMount_ManyFiles(t *testing.T) {
	skipIfNotRoot(t)

	// Create 500 files across 10 directories to stress inode allocation.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	now := time.Now().Truncate(time.Second)

	for d := 0; d < 10; d++ {
		dir := fmt.Sprintf("dir%d/", d)
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeDir, Name: dir,
			Mode: 0755, ModTime: now,
		}))
		for f := 0; f < 50; f++ {
			name := fmt.Sprintf("dir%d/file%d.txt", d, f)
			content := []byte(fmt.Sprintf("content of %s\n", name))
			require.NoError(t, tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg, Name: name,
				Mode: 0644, Size: int64(len(content)), ModTime: now,
			}))
			_, err := tw.Write(content)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())

	imgPath := filepath.Join(t.TempDir(), "many.ext4")
	require.NoError(t, BuildExt4FromTarReader(imgPath, &buf, 128*1024*1024))

	mp := loopMount(t, imgPath)

	// Verify all files.
	for d := 0; d < 10; d++ {
		for f := 0; f < 50; f++ {
			name := fmt.Sprintf("dir%d/file%d.txt", d, f)
			content := []byte(fmt.Sprintf("content of %s\n", name))
			got, err := os.ReadFile(filepath.Join(mp, name))
			require.NoError(t, err, "read %s", name)
			require.Equal(t, content, got, "content mismatch %s", name)
		}
	}
}

func TestMount_LargeSparseFile(t *testing.T) {
	skipIfNotRoot(t)

	// 4 MB file that is mostly zeros (sparse-friendly).
	data := make([]byte, 4*1024*1024)
	// Write non-zero data at the beginning and end.
	copy(data[:4096], bytes.Repeat([]byte("HEAD"), 1024))
	copy(data[len(data)-4096:], bytes.Repeat([]byte("TAIL"), 1024))

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg, Name: "sparse.bin",
		Mode: 0644, Size: int64(len(data)), ModTime: time.Now(),
	}))
	_, err := io.Copy(tw, bytes.NewReader(data))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	imgPath := filepath.Join(t.TempDir(), "sparse.ext4")
	require.NoError(t, BuildExt4FromTarReader(imgPath, &buf, 128*1024*1024))

	mp := loopMount(t, imgPath)

	got, err := os.ReadFile(filepath.Join(mp, "sparse.bin"))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// unixModeToGo converts a Unix mode_t (with setuid/setgid/sticky in the
// high bits of the 12-bit permission field) to Go's os.FileMode.
func unixModeToGo(mode int64) os.FileMode {
	m := os.FileMode(mode & 0777)
	if mode&04000 != 0 {
		m |= os.ModeSetuid
	}
	if mode&02000 != 0 {
		m |= os.ModeSetgid
	}
	if mode&01000 != 0 {
		m |= os.ModeSticky
	}
	return m
}

// Linux major/minor extraction from dev_t.
func unix_major(dev uint64) uint32 {
	return uint32((dev >> 8) & 0xfff)
}

func unix_minor(dev uint64) uint32 {
	return uint32((dev & 0xff) | ((dev >> 12) & 0xfff00))
}
