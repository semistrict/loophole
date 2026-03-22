package fsserver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/semistrict/loophole/internal/objstore"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveMKE2FS(t *testing.T) {
	orig := mke2fsLookPath
	t.Cleanup(func() { mke2fsLookPath = orig })

	t.Run("prefers mke2fs", func(t *testing.T) {
		mke2fsLookPath = func(name string) (string, error) {
			switch name {
			case "mke2fs":
				return "/opt/homebrew/bin/mke2fs", nil
			default:
				return "", errors.New("not found")
			}
		}
		path, err := resolveMKE2FS()
		require.NoError(t, err)
		require.Equal(t, "/opt/homebrew/bin/mke2fs", path)
	})

	t.Run("falls back to mkfs.ext4", func(t *testing.T) {
		mke2fsLookPath = func(name string) (string, error) {
			switch name {
			case "mke2fs":
				return "", errors.New("not found")
			case "mkfs.ext4":
				return "/usr/local/bin/mkfs.ext4", nil
			default:
				return "", errors.New("not found")
			}
		}
		path, err := resolveMKE2FS()
		require.NoError(t, err)
		require.Equal(t, "/usr/local/bin/mkfs.ext4", path)
	})

	t.Run("returns helpful error", func(t *testing.T) {
		mke2fsLookPath = func(string) (string, error) {
			return "", errors.New("not found")
		}
		_, err := resolveMKE2FS()
		require.Error(t, err)
		require.Contains(t, err.Error(), "e2fsprogs")
	})
}

func TestImportImageFileDirect(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	_, _, err := storage.FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	vm := &storage.Manager{ObjectStore: store}
	t.Cleanup(func() { require.NoError(t, vm.Close()) })

	vol, err := vm.NewVolume(storage.CreateParams{
		Volume: "seeded",
		Size:   2 * storage.BlockSize,
		Type:   storage.VolumeTypeExt4,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vol.ReleaseRef())
	}()

	imagePath := filepath.Join(t.TempDir(), "image.ext4")
	imageData := bytes.Repeat([]byte("abcd"), storage.BlockSize/4)
	imageData = append(imageData, bytes.Repeat([]byte("wxyz"), storage.BlockSize/4)...)
	require.NoError(t, os.WriteFile(imagePath, imageData, 0o644))

	require.NoError(t, importImageFileDirect(vol, imagePath))

	got, err := vol.ReadAt(ctx, 0, len(imageData))
	require.NoError(t, err)
	require.Equal(t, imageData, got)
}

func TestImportImageFileDirectPadsTailToPageSize(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	_, _, err := storage.FormatVolumeSet(ctx, store)
	require.NoError(t, err)

	vm := &storage.Manager{ObjectStore: store}
	t.Cleanup(func() { require.NoError(t, vm.Close()) })

	vol, err := vm.NewVolume(storage.CreateParams{
		Volume: "seeded-tail",
		Size:   2 * storage.PageSize,
		Type:   storage.VolumeTypeExt4,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, vol.ReleaseRef())
	}()

	imagePath := filepath.Join(t.TempDir(), "tail.img")
	imageData := bytes.Repeat([]byte("z"), storage.PageSize+123)
	require.NoError(t, os.WriteFile(imagePath, imageData, 0o644))

	require.NoError(t, importImageFileDirect(vol, imagePath))

	got, err := vol.ReadAt(ctx, 0, len(imageData))
	require.NoError(t, err)
	require.Equal(t, imageData, got)

	paddedTail, err := vol.ReadAt(ctx, uint64(len(imageData)), storage.PageSize-123)
	require.NoError(t, err)
	require.Equal(t, make([]byte, storage.PageSize-123), paddedTail)
}
