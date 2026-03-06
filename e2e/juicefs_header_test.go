package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
	"github.com/semistrict/loophole/daemon"
	svfs "github.com/semistrict/loophole/sqlitevfs"
)

type testVolumeRef struct {
	TimelineID string `json:"timeline_id"`
}

type testLayersJSON struct {
	Deltas []testDeltaMeta `json:"deltas"`
}

type testDeltaMeta struct {
	Key string `json:"key"`
}

type testDeltaHeader struct {
	Magic       [4]byte
	Version     uint16
	StartSeq    uint64
	EndSeq      uint64
	PageRange   [2]uint64
	NumEntries  uint32
	IndexOffset uint64
}

type testDeltaIndexEntry struct {
	PageAddr    uint64
	Seq         uint64
	ValueOffset uint64
	ValueLen    uint32
	CRC32       uint32
}

func readLatestDeltaPage0(t *testing.T, store loophole.ObjectStore, volume string) []byte {
	t.Helper()

	ctx := t.Context()
	ref, _, err := loophole.ReadJSON[testVolumeRef](ctx, store.At("volumes"), volume)
	require.NoError(t, err)

	layers, _, err := loophole.ReadJSON[testLayersJSON](ctx, store.At("timelines").At(ref.TimelineID), "layers.json")
	require.NoError(t, err)
	require.NotEmpty(t, layers.Deltas)

	latest := layers.Deltas[len(layers.Deltas)-1]
	body, _, err := store.At("timelines").At(ref.TimelineID).Get(ctx, latest.Key)
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	data, err := io.ReadAll(body)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 50)

	var hdr testDeltaHeader
	err = binary.Read(bytes.NewReader(data[:50]), binary.LittleEndian, &hdr)
	require.NoError(t, err)
	require.Equal(t, [4]byte{'L', 'D', 'L', 'T'}, hdr.Magic)

	indexStart := int(hdr.IndexOffset)
	require.LessOrEqual(t, indexStart, len(data))

	r := bytes.NewReader(data[indexStart:])
	for range hdr.NumEntries {
		var ie testDeltaIndexEntry
		err := binary.Read(r, binary.LittleEndian, &ie)
		require.NoError(t, err)
		if ie.PageAddr != 0 {
			continue
		}
		if ie.ValueLen == 0 {
			return make([]byte, 4096)
		}
		end := int(ie.ValueOffset) + int(ie.ValueLen)
		require.LessOrEqual(t, end, len(data))

		dec, err := zstd.NewReader(nil)
		require.NoError(t, err)
		defer dec.Close()

		page, err := dec.DecodeAll(data[ie.ValueOffset:end], nil)
		require.NoError(t, err)
		return page
	}

	t.Fatal("page 0 not found in latest delta")
	return nil
}

func TestE2E_JuiceFSFuseRetainsMetaHeaderAfterSnapshotAndUnmount(t *testing.T) {
	if mode() != loophole.ModeFuseFS || fsType() != loophole.FSJuiceFS {
		t.Skip("test covers the Linux juicefs+fusefs path only")
	}

	ctx := t.Context()
	trackMetrics(t)

	inst := uniqueInstance(t)
	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)

	vm, cfg := newVolumeManager(t, store)
	b := newBackendForMode(t, vm, inst, cfg)
	t.Cleanup(func() {
		_ = b.Close(ctx)
	})

	const (
		parentName = "parent"
		snapName   = "snap"
	)
	parentMP := mountpoint(t, parentName)

	require.NoError(t, b.Create(ctx, client.CreateParams{
		Type:   defaultVolumeType(),
		Volume: parentName,
	}))
	require.NoError(t, b.Mount(ctx, parentName, parentMP))

	tfs := newTestFS(t, b, parentMP)
	tfs.WriteFile(t, "hello.txt", []byte("hello from fuse\n"))
	tfs.MkdirAll(t, "subdir/nested")
	tfs.WriteFile(t, "subdir/nested/deep.txt", []byte("deep file\n"))

	require.NoError(t, b.Snapshot(ctx, parentMP, snapName))
	require.NoError(t, b.Unmount(ctx, parentMP))
	require.NoError(t, b.Close(ctx))

	vm2, _ := newVolumeManager(t, store)

	for _, name := range []string{parentName, snapName} {
		vol, err := vm2.OpenVolume(ctx, name)
		require.NoError(t, err, name)

		var page0 [8]byte
		_, err = vol.Read(ctx, page0[:], 0)
		require.NoError(t, err, name)
		require.Equal(t, "SQVFS002", string(page0[:]), name)

		_, err = svfs.ReadHeader(ctx, vol)
		require.NoError(t, err, name)
		require.NoError(t, vol.ReleaseRef(ctx), name)
	}
}

func TestE2E_JuiceFSFuseDaemonRetainsMetaHeaderAfterSnapshotAndUnmount(t *testing.T) {
	if mode() != loophole.ModeFuseFS || fsType() != loophole.FSJuiceFS {
		t.Skip("test covers the Linux juicefs+fusefs daemon path only")
	}

	ctx := t.Context()
	trackMetrics(t)

	inst := uniqueInstance(t)
	inst.Mode = loophole.ModeFuseFS
	inst.DefaultFSType = loophole.FSJuiceFS

	dir := loophole.Dir(t.TempDir())
	daemonCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := daemon.Start(ctx, inst, dir, true, 0)
	require.NoError(t, err)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- d.Serve(daemonCtx)
	}()

	c := client.New(dir, inst)
	require.Eventually(t, func() bool {
		_, err := c.Status(ctx)
		return err == nil
	}, 5*time.Second, 50*time.Millisecond)

	const (
		parentName = "daemon-parent"
		snapName   = "daemon-snap"
	)
	parentMP := t.TempDir()

	require.NoError(t, c.Create(ctx, client.CreateParams{
		Type:   loophole.VolumeTypeJuiceFS,
		Volume: parentName,
	}))
	require.NoError(t, c.Mount(ctx, parentName, parentMP))

	require.NoError(t, os.WriteFile(filepath.Join(parentMP, "hello.txt"), []byte("hello from daemon\n"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(parentMP, "subdir", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(parentMP, "subdir", "nested", "deep.txt"), []byte("deep file\n"), 0o644))

	require.NoError(t, c.Snapshot(ctx, parentMP, snapName))
	require.NoError(t, c.Unmount(ctx, parentMP))
	require.NoError(t, c.Shutdown(ctx))

	select {
	case err := <-serveErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after shutdown")
	}

	store, err := loophole.NewS3Store(ctx, inst)
	require.NoError(t, err)
	vm, _ := newVolumeManager(t, store)

	rawPage0 := readLatestDeltaPage0(t, store, parentName)

	for _, name := range []string{parentName, snapName} {
		vol, err := vm.OpenVolume(ctx, name)
		require.NoError(t, err, name)

		var page0 [8]byte
		_, err = vol.Read(ctx, page0[:], 0)
		require.NoError(t, err, name)
		require.Equal(t, "SQVFS002", string(page0[:]), name)
		if name == parentName {
			require.Equal(t, page0[:], rawPage0[:8], name)
		}

		_, err = svfs.ReadHeader(ctx, vol)
		require.NoError(t, err, name)
		require.NoError(t, vol.ReleaseRef(ctx), name)
	}
}
