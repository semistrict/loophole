//go:build cgo

package fsserver

import (
	"context"
	"fmt"
	"os"

	"github.com/semistrict/loophole/internal/e2fs"
	"github.com/semistrict/loophole/internal/storage"
)

// BuildExt4ImageFromPath creates a temporary ext4 image file populated from a
// host directory or tarball using the embedded libext2fs (no external mke2fs).
// The caller must remove the returned file when done.
func BuildExt4ImageFromPath(_ context.Context, srcPath string, size uint64) (string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return "", fmt.Errorf("stat mkfs source: %w", err)
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return "", fmt.Errorf("mkfs source %q must be a directory or regular tarball", srcPath)
	}
	if size == 0 {
		size = storage.DefaultVolumeSize
	}

	img, err := os.CreateTemp("", "loophole-seed-*.ext4")
	if err != nil {
		return "", fmt.Errorf("create temp ext4 image: %w", err)
	}
	imgPath := img.Name()
	if err := img.Close(); err != nil {
		_ = os.Remove(imgPath)
		return "", fmt.Errorf("close temp ext4 image: %w", err)
	}

	if info.IsDir() {
		err = e2fs.BuildExt4FromDir(imgPath, srcPath, size)
	} else {
		err = e2fs.BuildExt4FromTar(imgPath, srcPath, size)
	}
	if err != nil {
		_ = os.Remove(imgPath)
		return "", err
	}

	return imgPath, nil
}
