//go:build linux

package nbd_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	nbd "github.com/Merovius/nbd"
	"github.com/Merovius/nbd/nbdnl"
)

// memDevice is a minimal in-memory nbd.Device for testing.
type memDevice struct {
	data []byte
}

func newMemDevice(size int) *memDevice {
	return &memDevice{data: make([]byte, size)}
}

func (d *memDevice) ReadAt(p []byte, off int64) (int, error) {
	copy(p, d.data[off:])
	return len(p), nil
}

func (d *memDevice) WriteAt(p []byte, off int64) (int, error) {
	copy(d.data[off:], p)
	return len(p), nil
}

func (d *memDevice) Sync() error { return nil }

func TestLoopbackDeviceSize(t *testing.T) {
	const size = 128 * 1024 * 1024 // 128 MB
	dev := newMemDevice(size)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	idx, wait, err := nbd.Loopback(ctx, dev, size,
		nbd.WithClientFlags(nbdnl.FlagDisconnectOnClose),
	)
	if err != nil {
		t.Fatal("Loopback:", err)
	}

	// Read the kernel-reported size (in 512-byte sectors).
	sysPath := fmt.Sprintf("/sys/block/nbd%d/size", idx)
	data, err := os.ReadFile(sysPath)
	if err != nil {
		cancel()
		wait()
		t.Fatal("read sysfs size:", err)
	}

	var sectors uint64
	fmt.Sscanf(string(data), "%d", &sectors)
	gotBytes := sectors * 512

	if gotBytes != size {
		cancel()
		wait()
		t.Fatalf("kernel reports %d bytes (%d sectors), want %d bytes", gotBytes, sectors, size)
	}

	t.Logf("device /dev/nbd%d: %d sectors = %d bytes (correct)", idx, sectors, gotBytes)

	cancel()
	if err := wait(); err != nil {
		t.Fatal("wait:", err)
	}
}

func TestLoopbackReadWrite(t *testing.T) {
	const size = 64 * 1024 * 1024 // 64 MB
	dev := newMemDevice(size)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	idx, wait, err := nbd.Loopback(ctx, dev, size,
		nbd.WithClientFlags(nbdnl.FlagDisconnectOnClose),
	)
	if err != nil {
		t.Fatal("Loopback:", err)
	}

	devPath := nbd.DevicePath(idx)

	// Write through the block device.
	f, err := os.OpenFile(devPath, os.O_RDWR, 0)
	if err != nil {
		cancel()
		wait()
		t.Fatal("open device:", err)
	}
	payload := []byte("hello from nbd test")
	if _, err := f.WriteAt(payload, 4096); err != nil {
		f.Close()
		cancel()
		wait()
		t.Fatal("write:", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		cancel()
		wait()
		t.Fatal("sync:", err)
	}

	// Read back through the block device.
	buf := make([]byte, len(payload))
	if _, err := f.ReadAt(buf, 4096); err != nil {
		f.Close()
		cancel()
		wait()
		t.Fatal("read:", err)
	}
	f.Close()

	if string(buf) != string(payload) {
		cancel()
		wait()
		t.Fatalf("read back %q, want %q", buf, payload)
	}

	// Verify the in-memory device got the write.
	if string(dev.data[4096:4096+len(payload)]) != string(payload) {
		cancel()
		wait()
		t.Fatal("in-memory device doesn't contain the written data")
	}

	cancel()
	if err := wait(); err != nil {
		t.Fatal("wait:", err)
	}
}
