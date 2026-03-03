package lwext4

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRealTarExtractRemount extracts a real rootfs tar (e.g. the Debian
// bookworm image used for VM testing) into an lwext4 filesystem, closes it,
// remounts, and verifies every inode is accessible.
//
// Set ROOTFS_TAR to the path of the tar file. Skips if not set or not found.
// Example: make test-lwext4 RUN=TestRealTarExtractRemount ROOTFS_TAR=/app/rootfs.tar
func TestRealTarExtractRemount(t *testing.T) {
	tarPath := os.Getenv("ROOTFS_TAR")
	if tarPath == "" {
		tarPath = "/app/rootfs.tar"
	}
	if _, err := os.Stat(tarPath); err != nil {
		t.Skipf("skipping: tar not found at %s (set ROOTFS_TAR)", tarPath)
	}

	const devSize = 2 * 1024 * 1024 * 1024 // 2 GB
	dev := newMemDev(devSize)

	// Phase 1: Format and extract tar.
	fs1, err := Format(dev, int64(devSize), nil)
	require.NoError(t, err)

	extractTar(t, fs1, tarPath)

	require.NoError(t, fs1.CacheFlush())
	require.NoError(t, fs1.Close())

	// Phase 2: Remount and walk the entire tree.
	fs2, err := Mount(dev, int64(devSize))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, fs2.Close())
	}()

	// Recursively walk and verify every entry is accessible.
	var walk func(dirIno Ino, dirPath string)
	totalEntries := 0
	walk = func(dirIno Ino, dirPath string) {
		entries, err := fs2.Readdir(dirIno)
		require.NoError(t, err, "readdir %s (ino %d)", dirPath, dirIno)

		for _, de := range entries {
			totalEntries++
			entryPath := dirPath + "/" + de.Name

			attr, err := fs2.GetAttr(de.Inode)
			require.NoError(t, err, "getattr %s (ino %d)", entryPath, de.Inode)

			switch attr.Mode & syscall.S_IFMT {
			case syscall.S_IFDIR:
				walk(de.Inode, entryPath)
			case syscall.S_IFLNK:
				_, err := fs2.Readlink(de.Inode)
				require.NoError(t, err, "readlink %s (ino %d)", entryPath, de.Inode)
			case syscall.S_IFREG:
				// Verify we can open and read the file.
				f, err := fs2.Open(de.Inode)
				require.NoError(t, err, "open %s (ino %d)", entryPath, de.Inode)
				_, err = io.Copy(io.Discard, f)
				require.NoError(t, err, "read %s (ino %d)", entryPath, de.Inode)
				require.NoError(t, f.Close())
			}
		}
	}
	walk(RootIno, "")
	t.Logf("verified %d entries", totalEntries)
}

// extractTar extracts a tar archive into an lwext4 filesystem, mimicking
// what tar does through the FUSE layer: mkdir, mknod, symlink, write data,
// then chmod (SetAttr with permission-only mode).
func extractTar(t *testing.T, fs *FS, tarPath string) {
	t.Helper()

	f, err := os.Open(tarPath)
	require.NoError(t, err)
	defer f.Close()

	tr := tar.NewReader(f)
	dirInos := map[string]Ino{".": RootIno, "": RootIno}

	// lookupParent returns the parent inode and base name for a path.
	lookupParent := func(name string) (Ino, string) {
		dir := path.Dir(name)
		base := path.Base(name)
		ino, ok := dirInos[dir]
		if !ok {
			t.Fatalf("parent dir %q not found for %q", dir, name)
		}
		return ino, base
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		name := strings.TrimPrefix(hdr.Name, "./")
		name = strings.TrimSuffix(name, "/")
		if name == "" || name == "." {
			continue
		}

		parentIno, baseName := lookupParent(name)

		switch hdr.Typeflag {
		case tar.TypeDir:
			ino, err := fs.Mkdir(parentIno, baseName, uint32(hdr.Mode)|syscall.S_IFDIR)
			require.NoError(t, err, "mkdir %s", name)
			dirInos[name] = ino
			// Tar sets permissions after creating — FUSE sends perm-only mode.
			require.NoError(t, fs.SetAttr(ino, &Attr{
				Mode:  uint32(hdr.Mode & 0o7777),
				Uid:   uint32(hdr.Uid),
				Gid:   uint32(hdr.Gid),
				Mtime: uint32(hdr.ModTime.Unix()),
			}, AttrMode|AttrUid|AttrGid|AttrMtime))

		case tar.TypeReg:
			ino, err := fs.Mknod(parentIno, baseName, uint32(hdr.Mode)|syscall.S_IFREG)
			require.NoError(t, err, "mknod %s", name)
			if hdr.Size > 0 {
				ef, err := fs.OpenFile(ino, os.O_WRONLY)
				require.NoError(t, err, "open for write %s", name)
				_, err = io.Copy(ef, tr)
				require.NoError(t, err, "write %s", name)
				require.NoError(t, ef.Close())
			}
			require.NoError(t, fs.SetAttr(ino, &Attr{
				Mode:  uint32(hdr.Mode & 0o7777),
				Uid:   uint32(hdr.Uid),
				Gid:   uint32(hdr.Gid),
				Mtime: uint32(hdr.ModTime.Unix()),
			}, AttrMode|AttrUid|AttrGid|AttrMtime))

		case tar.TypeSymlink:
			_, err := fs.Symlink(parentIno, baseName, hdr.Linkname)
			require.NoError(t, err, "symlink %s -> %s", name, hdr.Linkname)

		case tar.TypeLink:
			// Hard link — find the target inode and link it.
			target := strings.TrimPrefix(hdr.Linkname, "./")
			targetDir := path.Dir(target)
			targetBase := path.Base(target)
			targetParent, ok := dirInos[targetDir]
			if !ok {
				t.Logf("skipping hardlink %s -> %s: parent dir not found", name, target)
				continue
			}
			targetIno, err := fs.Lookup(targetParent, targetBase)
			if err != nil {
				t.Logf("skipping hardlink %s -> %s: lookup failed: %v", name, target, err)
				continue
			}
			err = fs.Link(targetIno, parentIno, baseName)
			if err != nil {
				t.Logf("skipping hardlink %s -> %s: link failed: %v", name, target, err)
			}

		default:
			// Skip block/char devices, fifos, etc.
			t.Logf("skipping %s (type %d)", name, hdr.Typeflag)
		}
	}
}
