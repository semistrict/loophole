//go:build linux

package e2e

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2E_CloneMountStaysReadableDuringAggressiveFlush(t *testing.T) {
	skipE2E(t)

	b, parentMP := setupBusyboxVolume(t, "cln-stress-parent")
	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "16384")

	ctx := t.Context()
	cloneMP := mountpoint(t, "cln-stress-clone")
	require.NoError(t, b.Clone(ctx, parentMP, "cln-stress-clone"))
	require.NoError(t, b.Mount(ctx, "cln-stress-clone", cloneMP))

	cloneFS := newTestFS(t, b, cloneMP)
	cloneFS.MkdirAll(t, "stress")
	wantMarker := "chrooted\n"
	wantBusyboxMD5 := cloneFS.MD5(t, "bin/busybox")
	owner := b.ownerByVolume("cln-stress-clone")
	require.NotNil(t, owner)

	for i := 0; i < 40; i++ {
		payload := bytes.Repeat([]byte{byte(i), 0x5a, 0xc3, 0x11}, 2048)
		cloneFS.WriteFile(t, fmt.Sprintf("stress/file-%03d.bin", i), payload)
		cloneFS.WriteFile(t, fmt.Sprintf("stress/file-%03d.txt", i), []byte(fmt.Sprintf("round=%03d\n", i)))
		if needsKernelExt4() {
			syncFS(t, cloneMP)
		}

		gotMarker := string(cloneFS.ReadFile(t, "marker.txt"))
		gotBusyboxMD5 := cloneFS.MD5(t, "bin/busybox")
		if gotMarker != wantMarker || gotBusyboxMD5 != wantBusyboxMD5 {
			t.Fatalf("clone mount corrupted after batch %d: marker=%q want=%q busybox_md5=%s want=%s\nowner log:\n%s",
				i, gotMarker, wantMarker, gotBusyboxMD5, wantBusyboxMD5, tailFile(owner.logPath, 200))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
