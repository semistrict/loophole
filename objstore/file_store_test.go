package objstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStoreRejectsEscapingKeys(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "store")
	store, err := NewFileStore(root)
	require.NoError(t, err)

	err = store.PutReader(context.Background(), "../escaped.txt", bytes.NewReader([]byte("nope")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "escapes store root")

	_, statErr := os.Stat(filepath.Join(parent, "escaped.txt"))
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestFileStoreAllowsSafeNestedKeys(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileStore(root)
	require.NoError(t, err)

	require.NoError(t, store.PutReader(context.Background(), "volumes/safe/index.json", bytes.NewReader([]byte("ok"))))

	data, err := os.ReadFile(filepath.Join(root, "volumes", "safe", "index.json"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), data)
}
