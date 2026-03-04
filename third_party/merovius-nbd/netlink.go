//go:build linux

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
	"fmt"
	"net"
	"os"

	"github.com/Merovius/nbd/nbdnl"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// Configure passes the given set of sockets to the kernel to provide them as
// an NBD device. socks must be connected to the same server (which must
// support multiple connections) and be in transmission phase. It returns the
// device-numbers that was chosen by the kernel or any error. You can then use
// /dev/nbdX as a block device. Use nbdnl.Disconnect to disconnect the device
// once you're done with it.
//
// This is a Linux-only API.
func Configure(e Export, socks ...*os.File) (uint32, error) {
	var opts []nbdnl.ConnectOption
	if e.BlockSizes != nil {
		opts = append(opts, nbdnl.WithBlockSize(uint64(e.BlockSizes.Preferred)))
	}
	return nbdnl.Connect(nbdnl.IndexAny, socks, e.Size, 0, nbdnl.ServerFlags(e.Flags), opts...)
}

// LoopbackOption configures a Loopback call.
type LoopbackOption func(*loopbackConfig)

type loopbackConfig struct {
	clientFlags    nbdnl.ClientFlags
	serverFlags    nbdnl.ServerFlags
	blockSize      uint64
	numConnections int
}

// WithClientFlags sets the client flags passed to the kernel.
// Use nbdnl.FlagDisconnectOnClose to auto-disconnect when the socket closes
// (prevents unkillable D-state processes on crash).
func WithClientFlags(cf nbdnl.ClientFlags) LoopbackOption {
	return func(c *loopbackConfig) { c.clientFlags = cf }
}

// WithServerFlags overrides the server flags passed to the kernel.
// By default, Loopback sets FlagHasFlags | FlagSendFlush plus any flags
// implied by the Device's optional interfaces (Trimmer, WriteZeroer, etc.).
func WithServerFlags(sf nbdnl.ServerFlags) LoopbackOption {
	return func(c *loopbackConfig) { c.serverFlags = sf }
}

// WithLoopbackBlockSize sets the block size for the kernel block device.
func WithLoopbackBlockSize(n uint64) LoopbackOption {
	return func(c *loopbackConfig) { c.blockSize = n }
}

// WithNumConnections sets the number of parallel socket pairs between the
// kernel and userspace. The kernel round-robins I/O requests across
// connections, giving N-way serve parallelism. Requires FlagCanMulticonn
// (automatically added when n > 1). Default is 1.
func WithNumConnections(n int) LoopbackOption {
	return func(c *loopbackConfig) { c.numConnections = n }
}

// Loopback serves d on private sockets, passing the other ends to the kernel
// to connect to an NBD device. It returns the device-number that the kernel
// chose. wait should be called to check for errors from serving the device. It
// blocks until ctx is cancelled or an error occurs (so it behaves like Serve).
// When ctx is cancelled, the device will be disconnected, and any error
// encountered while disconnecting will be returned by wait.
//
// This is a Linux-only API.
func Loopback(ctx context.Context, d Device, size uint64, opts ...LoopbackOption) (idx uint32, wait func() error, err error) {
	cfg := loopbackConfig{
		serverFlags:    nbdnl.FlagHasFlags | nbdnl.FlagSendFlush,
		blockSize:      4096,
		numConnections: 1,
	}
	// Auto-detect capabilities from optional interfaces.
	if _, ok := d.(Trimmer); ok {
		cfg.serverFlags |= nbdnl.FlagSendTrim
	}
	if _, ok := d.(WriteZeroer); ok {
		cfg.serverFlags |= nbdnl.FlagSendWriteZeroes
	}
	if _, ok := d.(FUAWriter); ok {
		cfg.serverFlags |= nbdnl.FlagSendFUA
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.numConnections < 1 {
		cfg.numConnections = 1
	}
	if cfg.numConnections > 1 {
		cfg.serverFlags |= nbdnl.FlagCanMulticonn
	}

	nconn := cfg.numConnections
	exp := Export{
		Size:       size,
		Device:     d,
		BlockSizes: &BlockSizeConstraints{Min: 512, Preferred: uint32(cfg.blockSize), Max: 0xffffffff},
		Flags:      uint16(cfg.serverFlags),
	}

	// Create N socket pairs: client fds go to the kernel, server fds we serve on.
	clients := make([]*os.File, nconn)
	servers := make([]net.Conn, nconn)
	cleanup := func(upTo int) {
		for j := range upTo {
			if clients[j] != nil {
				clients[j].Close()
			}
			if servers[j] != nil {
				servers[j].Close()
			}
		}
	}

	for i := range nconn {
		sp, serr := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
		if serr != nil {
			cleanup(i)
			return 0, nil, serr
		}
		clientFile := os.NewFile(uintptr(sp[0]), fmt.Sprintf("client-%d", i))
		serverFile := os.NewFile(uintptr(sp[1]), fmt.Sprintf("server-%d", i))
		serverConn, serr := net.FileConn(serverFile)
		serverFile.Close()
		if serr != nil {
			clientFile.Close()
			cleanup(i)
			return 0, nil, serr
		}
		clients[i] = clientFile
		servers[i] = serverConn
	}

	var connectOpts []nbdnl.ConnectOption
	if cfg.blockSize != 0 {
		connectOpts = append(connectOpts, nbdnl.WithBlockSize(cfg.blockSize))
	}
	idx, err = nbdnl.Connect(nbdnl.IndexAny, clients, size, cfg.clientFlags, cfg.serverFlags, connectOpts...)
	if err != nil {
		cleanup(nconn)
		return 0, nil, err
	}

	parms := connParameters{exp, *exp.BlockSizes}
	var eg errgroup.Group
	for i := range nconn {
		sc := servers[i]
		eg.Go(func() error {
			return serve(ctx, sc, parms)
		})
	}
	wait = func() error {
		err := eg.Wait()
		// canceling the context is the only way for Loopback to return, so do
		// not consider them errors.
		if err == context.Canceled || err == context.DeadlineExceeded {
			err = nil
		}
		if e := nbdnl.Disconnect(idx); e != nil && err == nil {
			err = fmt.Errorf("failed to disconnect device: %w", e)
		}
		for _, c := range clients {
			if e := c.Close(); e != nil && err == nil {
				err = fmt.Errorf("failed to close client socket: %w", e)
			}
		}
		for _, s := range servers {
			if e := s.Close(); e != nil && err == nil {
				err = fmt.Errorf("failed to close server connection: %w", e)
			}
		}
		return err
	}
	return idx, wait, nil
}

// DevicePath returns the /dev/nbdN path for a device index.
func DevicePath(idx uint32) string {
	return fmt.Sprintf("/dev/nbd%d", idx)
}
