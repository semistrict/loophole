package e2fs

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/semistrict/loophole/internal/util"
)

// BuildExt4FromDir creates an ext4 filesystem image at imgPath populated from
// the host directory at dirPath. The image file is created with the given size.
func BuildExt4FromDir(imgPath string, dirPath string, sizeBytes uint64) error {
	img, err := os.Create(imgPath)
	if err != nil {
		return fmt.Errorf("create image: %w", err)
	}
	if err := img.Truncate(int64(sizeBytes)); err != nil {
		util.SafeClose(img, "close temp ext4 image after truncate failure")
		return fmt.Errorf("truncate image: %w", err)
	}
	if err := img.Close(); err != nil {
		return fmt.Errorf("close image: %w", err)
	}

	e2, err := Create(imgPath, sizeBytes)
	if err != nil {
		return err
	}

	if err := populateFromDir(e2, dirPath); err != nil {
		util.SafeClose(e2, "close ext4 image after populate failure")
		return err
	}
	return e2.Close()
}

func populateFromDir(e2 *FS, root string) error {
	var dirs, files, symlinks, devs, skipped int

	err := filepath.WalkDir(root, func(hostPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(root, hostPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %s: %w", rel, err)
		}

		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("no syscall.Stat_t for %s", rel)
		}

		uid := uint32(st.Uid)
		gid := uint32(st.Gid)
		mtime := info.ModTime().Unix()
		mode := uint32(st.Mode & 07777)

		switch {
		case info.IsDir():
			if err := e2.Mkdir(rel, mode, uid, gid, mtime); err != nil {
				return fmt.Errorf("mkdir %q: %w", rel, err)
			}
			dirs++

		case info.Mode().IsRegular():
			data, err := os.ReadFile(hostPath)
			if err != nil {
				return fmt.Errorf("read %q: %w", rel, err)
			}
			if err := e2.WriteFile(rel, mode, uid, gid, mtime, data); err != nil {
				return fmt.Errorf("write %q: %w", rel, err)
			}
			files++

		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(hostPath)
			if err != nil {
				return fmt.Errorf("readlink %q: %w", rel, err)
			}
			if err := e2.Symlink(rel, target, uid, gid, mtime); err != nil {
				return fmt.Errorf("symlink %q: %w", rel, err)
			}
			symlinks++

		case info.Mode()&fs.ModeDevice != 0:
			major := uint32((st.Rdev >> 8) & 0xfff)
			minor := uint32((st.Rdev & 0xff) | ((st.Rdev >> 12) & 0xfff00))
			devMode := mode
			if info.Mode()&fs.ModeCharDevice != 0 {
				devMode |= 0020000 // S_IFCHR
			} else {
				devMode |= 0060000 // S_IFBLK
			}
			if err := e2.Mknod(rel, devMode, uid, gid, mtime, major, minor); err != nil {
				return fmt.Errorf("mknod %q: %w", rel, err)
			}
			devs++

		case info.Mode()&fs.ModeNamedPipe != 0:
			if err := e2.Mknod(rel, 0010000|mode, uid, gid, mtime, 0, 0); err != nil {
				return fmt.Errorf("mkfifo %q: %w", rel, err)
			}
			devs++

		case info.Mode()&fs.ModeSocket != 0:
			if err := e2.Mknod(rel, 0140000|mode, uid, gid, mtime, 0, 0); err != nil {
				return fmt.Errorf("mksock %q: %w", rel, err)
			}
			devs++

		default:
			slog.Warn("e2fs: skipping unsupported file", "path", rel, "mode", info.Mode())
			skipped++
		}

		return nil
	})
	if err != nil {
		return err
	}

	slog.Info("e2fs: dir import complete",
		"dirs", dirs, "files", files, "symlinks", symlinks,
		"devs", devs, "skipped", skipped)
	return nil
}
