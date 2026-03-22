package env

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// firstNonEmpty returns the first non-empty string.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ResolveStore parses a user-supplied store URL and applies runtime defaults.
func ResolveStore(rawURL, logLevel string) (ResolvedStore, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ResolvedStore{}, fmt.Errorf("store URL is required")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return ResolvedStore{}, fmt.Errorf("parse store URL: %w", err)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return ResolvedStore{}, fmt.Errorf("store URL must not contain query params or fragments")
	}

	inst := ResolvedStore{
		StoreURL: rawURL,
		LogLevel: firstNonEmpty(logLevel, os.Getenv("LOOPHOLE_LOG_LEVEL")),
	}

	switch u.Scheme {
	case "file":
		if u.Host != "" && u.Host != "localhost" {
			return ResolvedStore{}, fmt.Errorf("file URL host must be empty or localhost")
		}
		if u.Path == "" {
			return ResolvedStore{}, fmt.Errorf("file URL path is required")
		}
		if !filepath.IsAbs(u.Path) {
			return ResolvedStore{}, fmt.Errorf("file URL path must be absolute")
		}
		inst.LocalDir = filepath.Clean(u.Path)
		inst.StoreURL = (&url.URL{Scheme: "file", Path: inst.LocalDir}).String()
		return inst, nil
	case "http", "https":
		if u.Host == "" {
			return ResolvedStore{}, fmt.Errorf("store URL host is required")
		}
		parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			return ResolvedStore{}, fmt.Errorf("store URL path must start with /<bucket>")
		}
		bucket, err := url.PathUnescape(parts[0])
		if err != nil {
			return ResolvedStore{}, fmt.Errorf("decode bucket: %w", err)
		}
		inst.Endpoint = (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
		inst.Bucket = bucket
		if len(parts) > 1 {
			prefixParts := make([]string, 0, len(parts)-1)
			for _, part := range parts[1:] {
				decoded, err := url.PathUnescape(part)
				if err != nil {
					return ResolvedStore{}, fmt.Errorf("decode prefix: %w", err)
				}
				prefixParts = append(prefixParts, decoded)
			}
			inst.Prefix = path.Clean(strings.Join(prefixParts, "/"))
			if inst.Prefix == "." {
				inst.Prefix = ""
			}
		}
		inst.StoreURL = canonicalHTTPSURL(u.Scheme, u.Host, inst.Bucket, inst.Prefix)
		return inst, nil
	default:
		return ResolvedStore{}, fmt.Errorf("unsupported store URL scheme %q", u.Scheme)
	}
}

func canonicalHTTPSURL(scheme, host, bucket, prefix string) string {
	u := &url.URL{Scheme: scheme, Host: host, Path: "/" + url.PathEscape(bucket)}
	if prefix != "" {
		for _, part := range strings.Split(prefix, "/") {
			u.Path += "/" + url.PathEscape(part)
		}
	}
	return u.String()
}
