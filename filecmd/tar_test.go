package filecmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole/fsbackend"
	"github.com/stretchr/testify/require"
)

func TestParseTarFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantOpts *tarOpts
		wantErr  string
	}{
		{
			name: "extract gzip",
			args: []string{"-xz", "-C", "myvolume:/"},
			wantOpts: &tarOpts{
				volume: "myvolume", extract: true, gzip: true, dir: "/",
			},
		},
		{
			name: "create gzip verbose",
			args: []string{"-czv", "-C", "myvolume:/app"},
			wantOpts: &tarOpts{
				volume: "myvolume", create: true, gzip: true, verbose: true, dir: "/app",
			},
		},
		{
			name: "extract with -f combined",
			args: []string{"-xzf", "archive.tgz", "-C", "myvolume:/"},
			wantOpts: &tarOpts{
				volume: "myvolume", extract: true, gzip: true, dir: "/", file: "archive.tgz",
			},
		},
		{
			name: "extract with -f separated",
			args: []string{"-xz", "-f", "archive.tgz", "-C", "myvolume:/"},
			wantOpts: &tarOpts{
				volume: "myvolume", extract: true, gzip: true, dir: "/", file: "archive.tgz",
			},
		},
		{
			name:    "missing -x or -c",
			args:    []string{"-z", "-C", "myvolume:/"},
			wantErr: "must specify -x (extract) or -c (create)",
		},
		{
			name:    "both -x and -c",
			args:    []string{"-xc", "-C", "myvolume:/"},
			wantErr: "cannot specify both -x and -c",
		},
		{
			name:    "missing -C",
			args:    []string{"-xz"},
			wantErr: "-C volume:/path is required",
		},
		{
			name:    "unknown flag",
			args:    []string{"-xQ", "-C", "myvolume:/"},
			wantErr: "unknown flag -Q",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseTarFlags(tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantOpts, opts)
		})
	}
}

// newOSFS creates a temp dir and returns an fsbackend.FS backed by the real OS.
func newOSFS(t *testing.T) (fsbackend.FS, string) {
	t.Helper()
	dir := t.TempDir()
	return fsbackend.NewOSFS(dir), dir
}

func TestTarCreateAndExtract(t *testing.T) {
	srcFS, srcDir := newOSFS(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello world\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "subdir", "nested.txt"), []byte("nested content\n"), 0o644))
	require.NoError(t, os.Symlink("hello.txt", filepath.Join(srcDir, "link.txt")))

	// Create tar.
	var buf bytes.Buffer
	err := runTarCreate(t.Context(), srcFS, &tarOpts{create: true, dir: "/"}, &buf)
	require.NoError(t, err)

	// Extract into a new FS.
	dstFS, dstDir := newOSFS(t)
	err = extractTar(t.Context(), dstFS, &tarOpts{extract: true, dir: "/"}, &buf, io.Discard)
	require.NoError(t, err)

	// Verify.
	got, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello world\n", string(got))

	got, err = os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	require.NoError(t, err)
	require.Equal(t, "nested content\n", string(got))

	target, err := os.Readlink(filepath.Join(dstDir, "link.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello.txt", target)
}

func TestTarGzipRoundTrip(t *testing.T) {
	srcFS, srcDir := newOSFS(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data.bin"), []byte("compressed data\n"), 0o644))

	// Create tgz.
	var buf bytes.Buffer
	err := runTarCreate(t.Context(), srcFS, &tarOpts{create: true, gzip: true, dir: "/"}, &buf)
	require.NoError(t, err)

	// Verify it's valid gzip.
	gr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NoError(t, gr.Close())

	// Extract tgz.
	dstFS, dstDir := newOSFS(t)
	err = extractTar(t.Context(), dstFS, &tarOpts{extract: true, gzip: true, dir: "/"}, &buf, io.Discard)
	require.NoError(t, err)

	got, err := os.ReadFile(filepath.Join(dstDir, "data.bin"))
	require.NoError(t, err)
	require.Equal(t, "compressed data\n", string(got))
}

func TestTarVerbose(t *testing.T) {
	srcFS, srcDir := newOSFS(t)
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "d", "f.txt"), []byte("x"), 0o644))

	// Create without verbose to get clean tar data.
	var tarBuf bytes.Buffer
	err := runTarCreate(t.Context(), srcFS, &tarOpts{create: true, dir: "/"}, &tarBuf)
	require.NoError(t, err)

	// Extract with verbose.
	dstFS, _ := newOSFS(t)
	var verboseBuf bytes.Buffer
	err = extractTar(t.Context(), dstFS, &tarOpts{extract: true, verbose: true, dir: "/"}, &tarBuf, &verboseBuf)
	require.NoError(t, err)
	require.Contains(t, verboseBuf.String(), "d")
	require.Contains(t, verboseBuf.String(), "d/f.txt")
}

func TestTarExtractPreservesPermissions(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir, Name: "usr/", Mode: 0o755,
	}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeDir, Name: "usr/bin/", Mode: 0o755,
	}))
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg, Name: "usr/bin/sh", Size: 4, Mode: 0o755,
	}))
	_, err := tw.Write([]byte("#!/b"))
	require.NoError(t, err)

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg, Name: "etc/passwd", Size: 5, Mode: 0o644,
	}))
	_, err = tw.Write([]byte("root:"))
	require.NoError(t, err)

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg, Name: "sbin/init", Size: 4, Mode: 0o755,
	}))
	_, err = tw.Write([]byte("init"))
	require.NoError(t, err)

	require.NoError(t, tw.Close())

	dstFS, dstDir := newOSFS(t)
	err = extractTar(t.Context(), dstFS, &tarOpts{extract: true, dir: "/"}, &buf, io.Discard)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dstDir, "usr/bin/sh"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "usr/bin/sh should be 0755")

	info, err = os.Stat(filepath.Join(dstDir, "sbin/init"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "sbin/init should be 0755")

	info, err = os.Stat(filepath.Join(dstDir, "etc/passwd"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm(), "etc/passwd should be 0644")
}

func TestTarExtractSubdir(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg, Name: "file.txt", Size: 5, Mode: 0o644,
	}))
	_, err := tw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	dstFS, dstDir := newOSFS(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dstDir, "sub"), 0o755))

	err = extractTar(context.Background(), dstFS, &tarOpts{extract: true, dir: "/sub"}, &buf, io.Discard)
	require.NoError(t, err)

	got, err := os.ReadFile(filepath.Join(dstDir, "sub", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestTarExtractRealTar(t *testing.T) {
	tarPath := os.Getenv("TAR_FILE")
	if tarPath == "" {
		t.Skip("TAR_FILE not set")
	}

	f, err := os.Open(tarPath)
	require.NoError(t, err)
	defer f.Close()

	dstFS, _ := newOSFS(t)
	err = extractTar(t.Context(), dstFS, &tarOpts{extract: true, verbose: true, dir: "/"}, f, os.Stderr)
	require.NoError(t, err)
}
