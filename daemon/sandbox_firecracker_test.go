//go:build linux

package daemon

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFirecrackerPrepareVolumeNoop(t *testing.T) {
	t.Parallel()

	runtime := &firecrackerSandboxRuntime{
		vmMgr: newFirecrackerVMManager(firecrackerRuntimeConfig{bootTimeout: 1}),
	}
	require.NoError(t, runtime.prepareVolume(context.Background(), "exec-firecracker"))
}
