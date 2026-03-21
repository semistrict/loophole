package fsserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage"
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

func buildExt4ImageFromDir(ctx context.Context, dir string, size uint64) (string, error) {
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

func importImageFileDirect(vol *storage.Volume, imagePath string) error {
	f, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("open image file: %w", err)
	}
	defer util.SafeClose(f, "close source image")

	if err := vol.EnableDirectWriteback(); err != nil {
		return fmt.Errorf("enable direct writeback: %w", err)
	}
	defer func() {
		if err := vol.DisableDirectWriteback(); err != nil {
			slog.Warn("disable direct writeback failed", "volume", vol.Name(), "error", err)
		}
	}()

	buf := make([]byte, storage.BlockSize)
	var offset uint64
	for {
		n, readErr := io.ReadFull(f, buf)
		if n > 0 {
			data := buf[:n]
			if rem := len(data) % storage.PageSize; rem != 0 {
				padded := make([]byte, len(data)+(storage.PageSize-rem))
				copy(padded, data)
				data = padded
			} else {
				data = append([]byte(nil), data...)
			}
			if err := vol.WritePagesDirect([]storage.DirectPage{{
				Offset: offset,
				Data:   data,
			}}); err != nil {
				return fmt.Errorf("direct write image at offset %d: %w", offset, err)
			}
			offset += uint64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read image file: %w", readErr)
		}
	}

	if err := vol.Flush(); err != nil {
		return fmt.Errorf("flush imported image: %w", err)
	}
	return nil
}

func importImageIntoNewVolume(vm *storage.Manager, p storage.CreateParams, imagePath string) error {
	vol, err := vm.NewVolume(p)
	if err != nil {
		return err
	}
	if err := importImageFileDirect(vol, imagePath); err != nil {
		if releaseErr := vol.ReleaseRef(); releaseErr != nil {
			slog.Warn("release create ref after import failure", "volume", p.Volume, "error", releaseErr)
		}
		return err
	}
	if err := vol.ReleaseRef(); err != nil {
		return fmt.Errorf("release create ref: %w", err)
	}
	return nil
}

func (b *Backend) createExt4FromDir(ctx context.Context, p storage.CreateParams) error {
	if p.Type != storage.VolumeTypeExt4 {
		return fmt.Errorf("--from-dir only supports ext4 volumes")
	}

	size := p.Size
	if size == 0 {
		size = storage.DefaultVolumeSize
	}
	imgPath, err := buildExt4ImageFromDir(ctx, p.FromDir, size)
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(imgPath); err != nil && !os.IsNotExist(err) {
			slog.Warn("remove temp ext4 image failed", "path", imgPath, "error", err)
		}
	}()

	return importImageIntoNewVolume(b.vm, p, imgPath)
}

func (b *Backend) createFromRawImage(p storage.CreateParams) error {
	info, err := os.Stat(p.FromRaw)
	if err != nil {
		return fmt.Errorf("stat raw image: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("raw image path %q is a directory", p.FromRaw)
	}
	if p.Size == 0 {
		p.Size = uint64(info.Size())
	}
	if uint64(info.Size()) > p.Size {
		return fmt.Errorf("raw image %q is larger than target volume size", p.FromRaw)
	}
	return importImageIntoNewVolume(b.vm, p, p.FromRaw)
}
