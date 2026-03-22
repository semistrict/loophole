package blob

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileDriverRejectsEscapingKeys(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "store")
	drv, err := NewFileDriver(root)
	require.NoError(t, err)

	_, err = drv.Put(context.Background(), "../escaped.txt", bytes.NewReader([]byte("nope")), PutOpts{Size: 4})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "escapes store root")

	_, statErr := os.Stat(filepath.Join(parent, "escaped.txt"))
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestFileDriverAllowsSafeNestedKeys(t *testing.T) {
	root := t.TempDir()
	drv, err := NewFileDriver(root)
	require.NoError(t, err)

	_, err = drv.Put(context.Background(), "volumes/safe/index.json", bytes.NewReader([]byte("ok")), PutOpts{Size: 2})
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(root, "volumes", "safe", "index.json"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), data)
}
