package fsserver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildExt4ImageFromPath_Dir(t *testing.T) {
	srcDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "sub", "nested.txt"), []byte("nested\n"), 0644))

	imgPath, err := BuildExt4ImageFromPath(context.Background(), srcDir, 64*1024*1024)
	require.NoError(t, err)
	defer os.Remove(imgPath)

	info, err := os.Stat(imgPath)
	require.NoError(t, err)
	require.Equal(t, int64(64*1024*1024), info.Size())
}

func TestBuildExt4ImageFromPath_InvalidSource(t *testing.T) {
	_, err := BuildExt4ImageFromPath(context.Background(), "/nonexistent", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stat mkfs source")
}
