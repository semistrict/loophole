package env

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveStoreHTTPS(t *testing.T) {
	inst, err := ResolveStore("https://storage.googleapis.com/my-bucket/vms/test", "debug")
	require.NoError(t, err)
	require.Equal(t, "https://storage.googleapis.com/my-bucket/vms/test", inst.StoreURL)
	require.Equal(t, "https://storage.googleapis.com", inst.Endpoint)
	require.Equal(t, "my-bucket", inst.Bucket)
	require.Equal(t, "vms/test", inst.Prefix)
	require.Equal(t, "debug", inst.LogLevel)
	require.False(t, inst.IsLocal())
	require.True(t, inst.IsGCS())
}

func TestResolveStoreFile(t *testing.T) {
	inst, err := ResolveStore("file:///tmp/loophole-store", "")
	require.NoError(t, err)
	require.Equal(t, "file:///tmp/loophole-store", inst.StoreURL)
	require.Equal(t, "/tmp/loophole-store", inst.LocalDir)
	require.True(t, inst.IsLocal())
	require.False(t, inst.IsGCS())
}

func TestResolveStoreRejectsMissingBucket(t *testing.T) {
	_, err := ResolveStore("https://storage.googleapis.com", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "/<bucket>")
}

func TestResolveStoreRejectsUnsupportedScheme(t *testing.T) {
	_, err := ResolveStore("s3://bucket/prefix", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported store URL scheme")
}
