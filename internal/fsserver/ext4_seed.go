package fsserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/util"
)

var mke2fsLookPath = exec.LookPath

func resolveMKE2FS() (string, error) {
	for _, name := range []string{"mke2fs", "mkfs.ext4"} {
		path, err := mke2fsLookPath(name)
		if err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("e2fsprogs not found in PATH; install mke2fs/mkfs.ext4 to use --from-dir")
}

// BuildExt4ImageFromDir creates a temporary ext4 image file populated from dir.
// The caller must remove the returned file when done.
func BuildExt4ImageFromDir(ctx context.Context, dir string, size uint64) (string, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("stat source dir: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("source path %q is not a directory", dir)
	}
	if size == 0 {
		size = storage.DefaultVolumeSize
	}

	mke2fsPath, err := resolveMKE2FS()
	if err != nil {
		return "", err
	}

	img, err := os.CreateTemp("", "loophole-seed-*.ext4")
	if err != nil {
		return "", fmt.Errorf("create temp ext4 image: %w", err)
	}
	imgPath := img.Name()
	if err := img.Truncate(int64(size)); err != nil {
		util.SafeClose(img, "close temp ext4 image after truncate failure")
		_ = os.Remove(imgPath)
		return "", fmt.Errorf("truncate temp ext4 image: %w", err)
	}
	if err := img.Close(); err != nil {
		_ = os.Remove(imgPath)
		return "", fmt.Errorf("close temp ext4 image: %w", err)
	}

	args := []string{
		"-q",
		"-t", "ext4",
		"-d", dir,
		"-F",
		"-O", mkfsExt4Features,
		"-E", "lazy_itable_init=1,nodiscard",
		imgPath,
	}
	cmd := exec.CommandContext(ctx, mke2fsPath, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.Remove(imgPath)
		return "", fmt.Errorf("%s %s: %w: %s", filepath.Base(mke2fsPath), strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return imgPath, nil
}
