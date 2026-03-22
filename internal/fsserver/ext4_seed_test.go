package fsserver

import (
	"errors"
	"testing"

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
