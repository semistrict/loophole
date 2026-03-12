package loophole

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/BurntSushi/toml"
)

// Config is the top-level ~/.loophole/config.toml structure.
type Config struct {
	DefaultProfile string             `toml:"default_profile"`
	Profiles       map[string]Profile `toml:"profiles"`
}

// Profile is a named set of connection parameters.
type Profile struct {
	Endpoint  string `toml:"endpoint"`
	Bucket    string `toml:"bucket"`
	Prefix    string `toml:"prefix"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Region    string `toml:"region"`
	LogLevel  string `toml:"log_level"`
	LocalDir  string `toml:"local_dir"`
	DaemonURL string `toml:"daemon_url"` // remote daemon base URL (e.g. https://cf-demo.ramon3525.workers.dev)
}

// LoadConfig reads ~/.loophole/config.toml. Returns an empty Config (not an
// error) if the file doesn't exist.
func LoadConfig(dir Dir) (*Config, error) {
	path := filepath.Join(string(dir), "config.toml")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &cfg, nil
}

// Resolve looks up a profile by name and returns the Instance.
// If name is empty, it uses DefaultProfile from the config, or falls back
// to the first profile (sorted alphabetically).
func (c *Config) Resolve(name string) (Instance, error) {
	if len(c.Profiles) == 0 {
		return Instance{}, fmt.Errorf("no profiles defined in config")
	}
	if name == "" {
		name = c.DefaultProfile
	}
	if name == "" {
		// Fall back to first profile alphabetically.
		var names []string
		for k := range c.Profiles {
			names = append(names, k)
		}
		sort.Strings(names)
		name = names[0]
	}
	p, ok := c.Profiles[name]
	if !ok {
		var names []string
		for k := range c.Profiles {
			names = append(names, k)
		}
		sort.Strings(names)
		return Instance{}, fmt.Errorf("unknown profile %q (available: %v)", name, names)
	}
	if p.Bucket == "" && p.LocalDir == "" && p.DaemonURL == "" {
		return Instance{}, fmt.Errorf("profile %q: bucket, local_dir, or daemon_url is required", name)
	}
	return Instance{
		ProfileName: name,
		Bucket:      p.Bucket,
		Prefix:      p.Prefix,
		Endpoint:    p.Endpoint,
		LocalDir:    p.LocalDir,
		AccessKey:   p.AccessKey,
		SecretKey:   p.SecretKey,
		Region:      p.Region,
		LogLevel:    p.LogLevel,
		DaemonURL:   p.DaemonURL,
	}, nil
}
