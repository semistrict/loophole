package storage2

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemtableReadWaitsForConcurrentWriteToFinish(t *testing.T) {
	memDir := filepath.Join(t.TempDir(), "mem")
	require.NoError(t, os.MkdirAll(memDir, 0o755))
	mt, err := newMemtable(memDir, 1, 1)
	require.NoError(t, err)
	t.Cleanup(mt.cleanup)

	pageIdx := PageIdx(0)
	slot := 0

	original := bytes.Repeat([]byte{0xAA}, PageSize)
	require.NoError(t, mt.put(pageIdx, original))

	slot, ok := mt.get(pageIdx)
	require.True(t, ok)

	replacement := bytes.Repeat([]byte{0xBB}, PageSize)
	half := PageSize / 2

	writerStarted := make(chan struct{})
	finishWrite := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		mt.mu.Lock()
		copy(mt.mmap[:half], replacement[:half])
		close(writerStarted)
		<-finishWrite
		copy(mt.mmap[half:PageSize], replacement[half:])
		mt.mu.Unlock()
		close(writerDone)
	}()

	<-writerStarted

	readDone := make(chan []byte, 1)
	errDone := make(chan error, 1)
	go func() {
		read, err := mt.readData(slot)
		if err != nil {
			errDone <- err
			return
		}
		readDone <- read
	}()

	select {
	case <-readDone:
		t.Fatal("read should block until concurrent write releases the memtable lock")
	case err := <-errDone:
		t.Fatalf("read should block, got error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(finishWrite)
	<-writerDone

	select {
	case err := <-errDone:
		require.NoError(t, err)
	case read := <-readDone:
		require.Equal(t, replacement, read)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for read")
	}
}
