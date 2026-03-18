//go:build linux

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/env"
)

func TestSandboxdStateRoundTrip(t *testing.T) {
	dir := env.Dir(t.TempDir())
	state := persistedState{
		Zygotes: map[string]zygoteRecord{
			"ubuntu": {
				Name:      "ubuntu",
				Volume:    "zygote-ubuntu",
				CreatedAt: time.Unix(1, 0).UTC(),
			},
		},
		Sandboxes: map[string]sandboxRecord{
			"sbx_1": {
				ID:           "sbx_1",
				Name:         "sandbox-1",
				State:        stateRunning,
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
