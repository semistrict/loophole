//go:build linux

package sandboxd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/env"
)

func TestSandboxdStateRoundTrip(t *testing.T) {
	dir := env.Dir(t.TempDir())
	state := persistedState{
		Zygotes: map[string]ZygoteRecord{
			"ubuntu": {
				Name:      "ubuntu",
				Volume:    "zygote-ubuntu",
				CreatedAt: time.Unix(1, 0).UTC(),
			},
		},
		Sandboxes: map[string]SandboxRecord{
			"sbx_1": {
				ID:           "sbx_1",
				Name:         "sandbox-1",
				State:        StateRunning,
				RootfsVolume: "sandbox-1",
				Mountpoint:   "/tmp/sandbox-1",
				OwnerSocket:  "/tmp/sandbox-1.sock",
				RunscID:      "runsc-sandbox-1",
				CreatedAt:    time.Unix(2, 0).UTC(),
			},
		},
	}

	require.NoError(t, saveState(dir, state))

	loaded, err := loadState(dir)
	require.NoError(t, err)
	require.Equal(t, state, loaded)
}
