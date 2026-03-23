package e2fs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreateAndClose(t *testing.T) {
	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	f, err := os.Create(imgPath)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(64*1024*1024))
	require.NoError(t, f.Close())

	fs, err := Create(imgPath, 64*1024*1024)
	require.NoError(t, err)
	require.NoError(t, fs.Close())

	// Verify the file is non-zero size (has ext4 structures).
	info, err := os.Stat(imgPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0))
}

func TestBuildExt4FromTarReader(t *testing.T) {
	// Build a tar archive in memory.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Directory
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir,
		Name:     "mydir/",
		Mode:     0755,
		Uid:      1000,
		Gid:      1000,
		ModTime:  now,
	}))

	// Regular file
	fileContent := []byte("hello world\n")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "mydir/hello.txt",
		Mode:     0644,
		Size:     int64(len(fileContent)),
		Uid:      1000,
		Gid:      1000,
		ModTime:  now,
	}))
	_, err := tw.Write(fileContent)
	require.NoError(t, err)

	// Symlink
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeSymlink,
		Name:     "mydir/link",
		Linkname: "hello.txt",
		Mode:     0777,
		Uid:      1000,
		Gid:      1000,
		ModTime:  now,
	}))

	// Nested directory
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir,
		Name:     "mydir/sub/deep/",
		Mode:     0700,
		Uid:      0,
		Gid:      0,
		ModTime:  now,
	}))

	// File in nested dir
	nestedContent := []byte("nested file content")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "mydir/sub/deep/nested.txt",
		Mode:     0600,
		Size:     int64(len(nestedContent)),
		Uid:      0,
		Gid:      0,
		ModTime:  now,
	}))
	_, err = tw.Write(nestedContent)
	require.NoError(t, err)

	require.NoError(t, tw.Close())

	// Create ext4 image from tar.
	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	err = BuildExt4FromTarReader(imgPath, &buf, 64*1024*1024)
	require.NoError(t, err)

	// Verify the file exists and has reasonable size.
	info, err := os.Stat(imgPath)
	require.NoError(t, err)
	require.Equal(t, int64(64*1024*1024), info.Size())
}

func TestBuildExt4FromTarReader_Gzip(t *testing.T) {
	// Build a gzipped tar in memory.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	content := []byte("gzipped content\n")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "gzfile.txt",
		Mode:     0644,
		Size:     int64(len(content)),
		ModTime:  time.Now(),
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())

	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	err = BuildExt4FromTarReader(imgPath, &buf, 64*1024*1024)
	require.NoError(t, err)

	info, err := os.Stat(imgPath)
	require.NoError(t, err)
	require.Equal(t, int64(64*1024*1024), info.Size())
}

func TestLargeFile(t *testing.T) {
	// Test with a 1MB file to exercise the write loop.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 251) // non-zero pattern
	}

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "bigfile.bin",
		Mode:     0644,
		Size:     int64(len(data)),
		ModTime:  time.Now(),
	}))
	_, err := tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	err = BuildExt4FromTarReader(imgPath, &buf, 64*1024*1024)
	require.NoError(t, err)
}

func TestHardlink(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	now := time.Now()

	// Original file
	content := []byte("original content")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "original.txt",
		Mode:     0644,
		Size:     int64(len(content)),
		ModTime:  now,
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)

	// Hard link
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeLink,
		Name:     "hardlink.txt",
		Linkname: "original.txt",
		ModTime:  now,
	}))

	require.NoError(t, tw.Close())

	imgPath := filepath.Join(t.TempDir(), "test.ext4")
	err = BuildExt4FromTarReader(imgPath, &buf, 64*1024*1024)
	require.NoError(t, err)
}
