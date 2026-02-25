//go:build linux

package nbdvm

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	nbd "github.com/Merovius/nbd"
	"github.com/Merovius/nbd/nbdnl"
)

func TestLoopbackWithMemDevice(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root for NBD netlink operations")
	}

	const size = 128 * 1024 * 1024 // 128 MB

	ctx := t.Context()
	mem := &memDevice{data: make([]byte, size)}

	idx, wait, err := nbd.Loopback(ctx, mem, size,
		nbd.WithClientFlags(nbdnl.FlagDisconnectOnClose),
	)
	if err != nil {
		t.Fatal("Loopback:", err)
	}

	assertDeviceSize(t, idx, size)

	t.Logf("device /dev/nbd%d: %d bytes (correct)", idx, size)
	wait()
}

func TestLoopbackWithTrimmerDevice(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root for NBD netlink operations")
	}

	// Same as above but the device implements Trimmer + WriteZeroer,
	// matching our real nbdvm.device. Ensures extra server flags
	// (FlagSendTrim, FlagSendWriteZeroes) don't break the connect.
	const size = 128 * 1024 * 1024

	ctx := t.Context()
	mem := &memDevice{data: make([]byte, size)}

	idx, wait, err := nbd.Loopback(ctx, mem, size,
		nbd.WithClientFlags(nbdnl.FlagDisconnectOnClose),
	)
	if err != nil {
		t.Fatal("Loopback:", err)
	}

	assertDeviceSize(t, idx, size)

	// Run mkfs.ext4 to prove the device is usable.
	devPath := nbd.DevicePath(idx)
	out, mkfsErr := exec.Command("mkfs.ext4", "-q", devPath).CombinedOutput()
	if mkfsErr != nil {
		wait()
		t.Fatalf("mkfs.ext4 %s: %v: %s", devPath, mkfsErr, out)
	}

	t.Logf("mkfs.ext4 succeeded on /dev/nbd%d", idx)
	wait()
}

func assertDeviceSize(t *testing.T, idx uint32, wantBytes uint64) {
	t.Helper()
	sysPath := fmt.Sprintf("/sys/block/nbd%d/size", idx)
	data, err := os.ReadFile(sysPath)
	if err != nil {
		t.Fatalf("read sysfs: %v", err)
	}
	var sectors uint64
	fmt.Sscanf(string(data), "%d", &sectors)
	gotBytes := sectors * 512
	if gotBytes != wantBytes {
		t.Fatalf("kernel reports %d bytes, want %d", gotBytes, wantBytes)
	}
}

// memDevice is a minimal in-memory Device for testing.
type memDevice struct{ data []byte }

func (d *memDevice) ReadAt(p []byte, off int64) (int, error) {
	copy(p, d.data[off:])
	return len(p), nil
}
func (d *memDevice) WriteAt(p []byte, off int64) (int, error) {
	copy(d.data[off:], p)
	return len(p), nil
}
func (d *memDevice) Sync() error                            { return nil }
func (d *memDevice) Trim(offset, length int64) error        { return nil }
func (d *memDevice) WriteZeroes(off, l int64, _ bool) error { return nil }
