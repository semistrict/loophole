// Copyright 2018 Axel Wagner
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Error combines the normal error interface with an Errno method, that returns
// an NBD error number. All of Device's methods should return an Error -
// otherwise, EIO is assumed as the error number.
type Error interface {
	Error() string
	Errno() Errno
}

// Device is the interface that should be implemented to expose an NBD device
// to the network or the kernel. Errors returned should implement Error -
// otherwise, EIO is assumed as the error number.
type Device interface {
	io.ReaderAt
	io.WriterAt
	// Sync should block until all previous writes where written to persistent
	// storage and return any errors that occured.
	Sync() error
}

// ListenAndServe starts listening on the given network/address and serves the
// given exports, the first of which will serve as the default. It starts a new
// goroutine for each connection. ListenAndServe only returns when ctx is
// cancelled or an unrecoverable error occurs. Either way, it will wait for all
// connections to terminate first.
func ListenAndServe(ctx context.Context, network, addr string, exp ...Export) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	l, err := net.Listen(network, addr)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctxCancel.Done()
		l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			Serve(ctx, c, exp...)
			c.Close()
		}()
	}
}

// Serve serves the given exports on c. The first export is used as a default.
// Serve returns after ctx is cancelled or an error occurs.
func Serve(ctx context.Context, c net.Conn, exp ...Export) error {
	parms, err := serverHandshake(c, exp)
	if err != nil {
		return err
	}
	return serve(ctx, c, parms)
}

// ListenAndServeDynamic is like ListenAndServe but resolves exports dynamically
// using the given ExportProvider instead of a static list.
func ListenAndServeDynamic(ctx context.Context, network, addr string, provider ExportProvider) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	l, err := net.Listen(network, addr)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctxCancel.Done()
		l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ServeDynamic(ctx, c, provider)
			c.Close()
		}()
	}
}

// ServeDynamic is like Serve but resolves exports dynamically using the given
// ExportProvider instead of a static list.
func ServeDynamic(ctx context.Context, c net.Conn, provider ExportProvider) error {
	parms, err := serverHandshakeDynamic(ctx, c, provider)
	if err != nil {
		return err
	}
	return serve(ctx, c, parms)
}

// serve serves nbd requests for a connection in transmission mode using p.
// Requests are read sequentially but dispatched to goroutines for concurrent
// processing. Replies are written back in any order (the NBD protocol allows
// this — the kernel matches replies to requests via the handle field).
//
// On context cancellation, serve enters lame-duck mode: it replies ESHUTDOWN
// to the next request and then returns, giving the kernel a clean signal that
// the device is going away rather than an abrupt socket close.
func serve(ctx context.Context, c net.Conn, p connParameters) error {
	d := p.Export.Device
	trimmer, _ := d.(Trimmer)
	wzeroer, _ := d.(WriteZeroer)
	fuaWriter, _ := d.(FUAWriter)

	shutdownCh := ctx.Done()

	// replyWriter serializes reply writes to the connection.
	rw := &replyWriter{w: c}

	// wg tracks in-flight request handlers.
	var wg sync.WaitGroup
	defer wg.Wait()

	return do(c, func(e *encoder) {
		for {
			select {
			case <-shutdownCh:
				c.SetDeadline(time.Now().Add(time.Second))
				var req request
				if err := req.decode(e); err != nil {
					return
				}
				rw.sendError(req.handle, ESHUTDOWN)
				return
			default:
			}

			var req request
			if err := req.decode(e); err != nil {
				rw.sendError(req.handle, err)
				continue
			}

			switch req.typ {
			case cmdRead:
				if req.length == 0 {
					rw.sendError(req.handle, EINVAL)
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					buf := make([]byte, req.length)
					_, err := d.ReadAt(buf, int64(req.offset))
					if err != nil {
						rw.sendError(req.handle, err)
						return
					}
					rw.sendData(req.handle, buf)
				}()

			case cmdWrite:
				if req.length == 0 {
					rw.sendError(req.handle, EINVAL)
					continue
				}
				// req.data is already read from the socket by decode.
				data := req.data
				handle := req.handle
				fua := req.flags&cmdFlagFUA != 0
				wg.Add(1)
				go func() {
					defer wg.Done()
					if fua && fuaWriter != nil {
						if _, err := fuaWriter.WriteAtFUA(data, int64(req.offset)); err != nil {
							rw.sendError(handle, err)
							return
						}
					} else {
						if _, err := d.WriteAt(data, int64(req.offset)); err != nil {
							rw.sendError(handle, err)
							return
						}
						if fua {
							if err := d.Sync(); err != nil {
								rw.sendError(handle, err)
								return
							}
						}
					}
					rw.sendOK(handle)
				}()

			case cmdDisc:
				return

			case cmdFlush:
				if req.length != 0 || req.offset != 0 {
					rw.sendError(req.handle, EINVAL)
					continue
				}
				handle := req.handle
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := d.Sync(); err != nil {
						rw.sendError(handle, err)
						return
					}
					rw.sendOK(handle)
				}()

			case cmdTrim:
				if trimmer == nil {
					rw.sendError(req.handle, EINVAL)
					continue
				}
				handle := req.handle
				offset, length := req.offset, req.length
				fua := req.flags&cmdFlagFUA != 0
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := trimmer.Trim(int64(offset), int64(length)); err != nil {
						rw.sendError(handle, err)
						return
					}
					if fua {
						if err := d.Sync(); err != nil {
							rw.sendError(handle, err)
							return
						}
					}
					rw.sendOK(handle)
				}()

			case cmdWriteZeroes:
				if wzeroer == nil {
					rw.sendError(req.handle, EINVAL)
					continue
				}
				handle := req.handle
				offset, length := req.offset, req.length
				punch := req.flags&cmdFlagNoHole == 0
				fua := req.flags&cmdFlagFUA != 0
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := wzeroer.WriteZeroes(int64(offset), int64(length), punch); err != nil {
						rw.sendError(handle, err)
						return
					}
					if fua {
						if err := d.Sync(); err != nil {
							rw.sendError(handle, err)
							return
						}
					}
					rw.sendOK(handle)
				}()

			default:
				rw.sendError(req.handle, EINVAL)
			}
		}
	})
}

// replyWriter serializes NBD simple reply writes to a connection.
// All methods are safe for concurrent use.
type replyWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// sendOK writes a success reply with no data payload.
func (rw *replyWriter) sendOK(handle uint64) {
	rw.writeReply(0, handle, nil)
}

// sendData writes a success reply with a data payload (for reads).
func (rw *replyWriter) sendData(handle uint64, data []byte) {
	rw.writeReply(0, handle, data)
}

// sendError writes an error reply.
func (rw *replyWriter) sendError(handle uint64, err error) {
	code := uint32(EIO)
	if e, ok := err.(Error); ok {
		code = uint32(e.Errno())
	}
	rw.writeReply(code, handle, nil)
}

// writeReply writes a complete simple reply as a single Write call.
// Header: magic(4) + errno(4) + handle(8) = 16 bytes, followed by data.
func (rw *replyWriter) writeReply(errno uint32, handle uint64, data []byte) {
	buf := make([]byte, 16+len(data))
	binary.BigEndian.PutUint32(buf[0:4], simpleReplyMagic)
	binary.BigEndian.PutUint32(buf[4:8], errno)
	binary.BigEndian.PutUint64(buf[8:16], handle)
	copy(buf[16:], data)

	rw.mu.Lock()
	rw.w.Write(buf)
	rw.mu.Unlock()
}

// respondErr writes an error response to e, based on handle and err.
// Used only during the sequential phase (shutdown).
func respondErr(e *encoder, handle uint64, err error) {
	code := EIO
	if e, ok := err.(Error); ok {
		code = e.Errno()
	}
	rep := simpleReply{
		errno:  uint32(code),
		handle: handle,
		length: 0,
	}
	rep.encode(e)
}

// ctxRW wraps a net.Conn to respect context cancellation. It does so by
// starting a goroutine that sets the connection's read/write deadline in the
// past whenever the context is cancelled.
type ctxRW struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	c      net.Conn
	done   <-chan struct{}
}

// wrapConn wraps a connection in a ctxRW.
func wrapConn(ctx context.Context, c net.Conn) io.ReadWriteCloser {
	// Note: cancel is called by Close().
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		c.SetDeadline(time.Now())
	}()
	return &ctxRW{ctx, cancel, c, done}
}

// Read implements io.Reader. It returns context.Cause(ctx) if the read was
// aborted due to context cancellation.
func (rw *ctxRW) Read(p []byte) (n int, err error) {
	n, err = rw.c.Read(p)
	if e := context.Cause(rw.ctx); e != nil {
		err = e
	}
	return n, err
}

// Write implements io.Writer. It returns context.Cause(ctx) if the write was
// aborted due to context cancellation.
func (rw *ctxRW) Write(p []byte) (n int, err error) {
	n, err = rw.c.Write(p)
	if e := context.Cause(rw.ctx); e != nil {
		err = e
	}
	return n, err
}

// Close implements io.Closer. It cleans up the resources associated with the
// ctxRW, but not the wrapped net.Conn. The wrapped net.Conn must be closed by
// the caller separately, otherwise any pending read/write operation may be left
// running indefinitely.
func (rw *ctxRW) Close() error {
	rw.cancel(errors.New("wrapped connection was closed"))
	<-rw.done
	return nil
}
