package loophole

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentReadWriteDifferentFiles(t *testing.T) {
	store := NewMemStore()
	vm := newTestVM(t, store)
	defer func() {
		if err := vm.Close(t.Context()); err != nil {
			t.Logf("close failed: %v", err)
		}
	}()

	vol, err := vm.NewVolume(t.Context(), "concurrent")
	require.NoError(t, err)

	fs := NewSimpleFS(vol, 64)

	const numFiles = 10
	const iterations = 50

	for i := 0; i < numFiles; i++ {
		require.NoError(t, fs.Create(fmt.Sprintf("file-%d", i)))
	}

	g, ctx := errgroup.WithContext(t.Context())

	// Each goroutine owns one file and repeatedly writes then reads it.
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file-%d", i)
		marker := byte(i + 1)

		g.Go(func() error {
			for iter := 0; iter < iterations; iter++ {
				data := bytes.Repeat([]byte{marker}, 32)
				if err := fs.WriteFile(ctx, name, 0, data); err != nil {
					return fmt.Errorf("write %s iter %d: %w", name, iter, err)
				}

				buf := make([]byte, 32)
				if _, err := fs.ReadFile(ctx, name, 0, buf); err != nil {
					return fmt.Errorf("read %s iter %d: %w", name, iter, err)
				}

				if !bytes.Equal(buf, data) {
					return fmt.Errorf("file %s iter %d: got %x, want %x", name, iter, buf, data)
				}
			}
			return nil
		})
	}

	assert.NoError(t, g.Wait())
}
