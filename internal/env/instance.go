package env

import "strings"

// ResolvedStore identifies a loophole backing store and its local runtime state.
type ResolvedStore struct {
	StoreURL string
	Bucket   string
	Prefix   string
	Endpoint string // object-store endpoint URL
	LocalDir string // non-empty => local FileStore
	LogLevel string // "debug", "info", "warn", "error"; empty = "info"
	VolsetID string // populated after reading loophole.json
}

// URL returns the canonical store URL string.
func (inst ResolvedStore) URL() string {
	return inst.StoreURL
}

// IsLocal reports whether the store is backed by the local filesystem.
func (inst ResolvedStore) IsLocal() bool {
	return inst.LocalDir != ""
}

// IsGCS reports whether the store URL points at Google Cloud Storage.
func (inst ResolvedStore) IsGCS() bool {
	return strings.Contains(inst.Endpoint, "storage.googleapis.com") ||
		strings.Contains(inst.Endpoint, "storage.cloud.google.com")
}
