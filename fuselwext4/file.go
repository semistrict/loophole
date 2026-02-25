package fuselwext4

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/lwext4"
)

// ext4FileHandle wraps an lwext4.File for FUSE file operations.
type ext4FileHandle struct {
	f  *lwext4.File
	mu *sync.Mutex
}

var (
	_ fs.FileReader   = (*ext4FileHandle)(nil)
	_ fs.FileWriter   = (*ext4FileHandle)(nil)
	_ fs.FileFlusher  = (*ext4FileHandle)(nil)
	_ fs.FileReleaser = (*ext4FileHandle)(nil)
	_ fs.FileGetattrer = (*ext4FileHandle)(nil)
)

func (fh *ext4FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.mu.Lock()
	n, err := fh.f.ReadAt(dest, off)
	fh.mu.Unlock()
	if err != nil && n == 0 {
		return nil, errToStatus(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *ext4FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fh.mu.Lock()
	n, err := fh.f.WriteAt(data, off)
	fh.mu.Unlock()
	if err != nil {
		return uint32(n), errToStatus(err)
	}
	return uint32(n), 0
}

func (fh *ext4FileHandle) Flush(ctx context.Context) syscall.Errno {
	return 0
}

func (fh *ext4FileHandle) Release(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	err := fh.f.Close()
	fh.mu.Unlock()
	return errToStatus(err)
}

func (fh *ext4FileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fh.mu.Lock()
	out.Size = uint64(fh.f.Size())
	fh.mu.Unlock()
	return 0
}
