package fuselwext4

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/semistrict/loophole/lwext4"
	"github.com/semistrict/loophole/metrics"
)

// ext4FileHandle wraps an lwext4.File for FUSE file operations.
type ext4FileHandle struct {
	f       *lwext4.File
	mu      *sync.Mutex
	ino     lwext4.Ino
	ext4fs  *lwext4.FS
	orphans *orphanTracker
}

var (
	_ fs.FileReader    = (*ext4FileHandle)(nil)
	_ fs.FileWriter    = (*ext4FileHandle)(nil)
	_ fs.FileFlusher   = (*ext4FileHandle)(nil)
	_ fs.FileReleaser  = (*ext4FileHandle)(nil)
	_ fs.FileGetattrer = (*ext4FileHandle)(nil)
)

func (fh *ext4FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	done := metrics.FuseOp("read")
	fh.mu.Lock()
	n, err := fh.f.ReadAt(dest, off)
	fh.mu.Unlock()
	if err != nil {
		if err == io.EOF {
			clear(dest[n:])
			metrics.FuseBytes.WithLabelValues("read").Add(float64(len(dest)))
			done(0)
			return fuse.ReadResultData(dest), 0
		}
		if n == 0 {
			errno := errToStatus(err)
			done(errno)
			return nil, errno
		}
	}
	metrics.FuseBytes.WithLabelValues("read").Add(float64(n))
	done(0)
	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *ext4FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	done := metrics.FuseOp("write")
	fh.mu.Lock()
	n, err := fh.f.WriteAt(data, off)
	fh.mu.Unlock()
	if err != nil {
		errno := errToStatus(err)
		done(errno)
		return uint32(n), errno
	}
	metrics.FuseBytes.WithLabelValues("write").Add(float64(n))
	done(0)
	return uint32(n), 0
}

func (fh *ext4FileHandle) Flush(ctx context.Context) syscall.Errno {
	done := metrics.FuseOp("flush")
	done(0)
	return 0
}

func (fh *ext4FileHandle) Release(ctx context.Context) syscall.Errno {
	done := metrics.FuseOp("release")
	fh.mu.Lock()
	err := fh.f.Close()
	if fh.orphans != nil {
		fh.orphans.closeFD(fh.ext4fs, fh.ino)
	}
	fh.mu.Unlock()
	errno := errToStatus(err)
	done(errno)
	return errno
}

func (fh *ext4FileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	fh.mu.Lock()
	out.Size = uint64(fh.f.Size())
	fh.mu.Unlock()
	return 0
}
