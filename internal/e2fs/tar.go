package e2fs

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"

	"github.com/semistrict/loophole/internal/util"
)

// BuildExt4FromTar creates an ext4 filesystem image at imgPath populated from
// the tar archive at tarPath. The image file is created with the given size.
// If tarPath is "-", reads from stdin.
func BuildExt4FromTar(imgPath string, tarPath string, sizeBytes uint64) error {
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

	// Open tar (auto-detect gzip).
	var r io.Reader
	if tarPath == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(tarPath)
		if err != nil {
			util.SafeClose(e2, "close ext4 after tar open failure")
			return fmt.Errorf("open tar: %w", err)
		}
		defer f.Close() //nolint:errcheck // best-effort close on read-only file
		r = f
	}

	r, err = maybeGunzip(r)
	if err != nil {
		util.SafeClose(e2, "close ext4 after gunzip failure")
		return err
	}

	if err := populateFromTar(e2, tar.NewReader(r)); err != nil {
		util.SafeClose(e2, "close ext4 after populate failure")
		return err
	}
	return e2.Close()
}

// BuildExt4FromTarReader creates an ext4 filesystem image at imgPath populated
// from the given tar reader. The image file is created with the given size.
func BuildExt4FromTarReader(imgPath string, r io.Reader, sizeBytes uint64) error {
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

	r, err = maybeGunzip(r)
	if err != nil {
		util.SafeClose(e2, "close ext4 after gunzip failure")
		return err
	}

	if err := populateFromTar(e2, tar.NewReader(r)); err != nil {
		util.SafeClose(e2, "close ext4 after populate failure")
		return err
	}
	return e2.Close()
}

// maybeGunzip peeks at the first two bytes to detect gzip magic (\x1f\x8b)
// and wraps the reader in a gzip.Reader if found.
func maybeGunzip(r io.Reader) (io.Reader, error) {
	br := bufio.NewReader(r)
	peek, err := br.Peek(2)
	if err != nil {
		// Too short to be gzip; return as-is (might be empty tar).
		return br, nil
	}
	if peek[0] == 0x1f && peek[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		return gz, nil
	}
	return br, nil
}

func populateFromTar(e2 *FS, tr *tar.Reader) error {
	var dirs, files, symlinks, hardlinks, devs, skipped int

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar header: %w", err)
		}

		// Clean the path: strip leading "/" and "./" prefixes.
		name := path.Clean(hdr.Name)
		name = strings.TrimPrefix(name, "/")
		name = strings.TrimPrefix(name, "./")
		if name == "" || name == "." {
			continue
		}

		uid := uint32(hdr.Uid)
		gid := uint32(hdr.Gid)
		mtime := hdr.ModTime.Unix()
		mode := uint32(hdr.Mode & 07777)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := e2.Mkdir(name, mode, uid, gid, mtime); err != nil {
				return fmt.Errorf("mkdir %q: %w", name, err)
			}
			dirs++

		case tar.TypeReg:
			data, err := io.ReadAll(tr)
			if err != nil {
				return fmt.Errorf("read %q: %w", name, err)
			}
			if err := e2.WriteFile(name, mode, uid, gid, mtime, data); err != nil {
				return fmt.Errorf("write %q: %w", name, err)
			}
			files++

		case tar.TypeSymlink:
			if err := e2.Symlink(name, hdr.Linkname, uid, gid, mtime); err != nil {
				return fmt.Errorf("symlink %q -> %q: %w", name, hdr.Linkname, err)
			}
			symlinks++

		case tar.TypeLink:
			target := path.Clean(hdr.Linkname)
			target = strings.TrimPrefix(target, "/")
			target = strings.TrimPrefix(target, "./")
			if err := e2.Hardlink(name, target); err != nil {
				return fmt.Errorf("hardlink %q -> %q: %w", name, hdr.Linkname, err)
			}
			hardlinks++

		case tar.TypeChar:
			if err := e2.Mknod(name, 0020000|mode, uid, gid, mtime,
				uint32(hdr.Devmajor), uint32(hdr.Devminor)); err != nil {
				return fmt.Errorf("mknod char %q: %w", name, err)
			}
			devs++

		case tar.TypeBlock:
			if err := e2.Mknod(name, 0060000|mode, uid, gid, mtime,
				uint32(hdr.Devmajor), uint32(hdr.Devminor)); err != nil {
				return fmt.Errorf("mknod block %q: %w", name, err)
			}
			devs++

		case tar.TypeFifo:
			if err := e2.Mknod(name, 0010000|mode, uid, gid, mtime, 0, 0); err != nil {
				return fmt.Errorf("mknod fifo %q: %w", name, err)
			}
			devs++

		default:
			slog.Warn("e2fs: skipping tar entry", "name", name, "type", hdr.Typeflag)
			skipped++
		}
	}

	slog.Info("e2fs: tar import complete",
		"dirs", dirs, "files", files, "symlinks", symlinks,
		"hardlinks", hardlinks, "devs", devs, "skipped", skipped)
	return nil
}
