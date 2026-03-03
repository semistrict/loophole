package loophole

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfigProfiles(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.toml")
	err := os.WriteFile(configPath, []byte(`
[profiles.rustfs]
endpoint = "http://localhost:9000"
bucket = "testbucket"
access_key = "rustfsadmin"
secret_key = "rustfsadmin"

[profiles.tigris]
endpoint = "https://fly.storage.tigris.dev"
bucket = "my-images"
prefix = "vms"
access_key = "tid_xxx"
secret_key = "tsec_xxx"
region = "auto"
mode = "fuse"
log_level = "debug"
`), 0o644)
	require.NoError(t, err)

	cfg, err := LoadConfig(Dir(dir))
	require.NoError(t, err)
	require.Len(t, cfg.Profiles, 2)

	inst, err := cfg.Resolve("rustfs")
	require.NoError(t, err)
	require.Equal(t, "rustfs", inst.ProfileName)
	require.Equal(t, "testbucket", inst.Bucket)
	require.Equal(t, "http://localhost:9000", inst.Endpoint)
	require.Equal(t, "", inst.Prefix)
	require.Equal(t, "rustfsadmin", inst.AccessKey)
	require.Equal(t, "rustfsadmin", inst.SecretKey)

	inst, err = cfg.Resolve("tigris")
	require.NoError(t, err)
	require.Equal(t, "tigris", inst.ProfileName)
	require.Equal(t, "my-images", inst.Bucket)
	require.Equal(t, "vms", inst.Prefix)
	require.Equal(t, "https://fly.storage.tigris.dev", inst.Endpoint)
	require.Equal(t, "tid_xxx", inst.AccessKey)
	require.Equal(t, "tsec_xxx", inst.SecretKey)
	require.Equal(t, "auto", inst.Region)
	require.Equal(t, Mode("fuse"), inst.Mode)
	require.Equal(t, "debug", inst.LogLevel)
}

func TestLoadConfigMissing(t *testing.T) {
	dir := t.TempDir()
	cfg, err := LoadConfig(Dir(dir))
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Profiles, 0)
}

func TestResolveUnknownProfile(t *testing.T) {
	cfg := &Config{
		Profiles: map[string]Profile{
			"rustfs": {Bucket: "test"},
		},
	}
	_, err := cfg.Resolve("nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown profile")
}

func TestResolveMissingBucket(t *testing.T) {
	cfg := &Config{
		Profiles: map[string]Profile{
			"bad": {Endpoint: "http://localhost:9000"},
		},
	}
	_, err := cfg.Resolve("bad")
	require.Error(t, err)
	require.Contains(t, err.Error(), "bucket is required")
}
