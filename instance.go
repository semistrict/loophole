package loophole

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
)

// Instance identifies a logical loophole store: a specific S3 bucket + prefix
// at a specific endpoint. Two instances with different endpoints but the same
// bucket/prefix are distinct.
type Instance struct {
	Bucket   string
	Prefix   string
	Endpoint string // S3 endpoint URL; empty = AWS default
}

// ParseInstance parses an s3://bucket/prefix URL into an Instance.
// The endpoint is not part of the URL — set it separately if needed.
func ParseInstance(raw string) (Instance, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Instance{}, fmt.Errorf("parse S3 URL: %w", err)
	}
	if u.Scheme != "s3" {
		return Instance{}, fmt.Errorf("expected s3:// URL, got %q", raw)
	}
	if u.Host == "" {
		return Instance{}, fmt.Errorf("missing bucket in S3 URL %q", raw)
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	prefix = strings.TrimSuffix(prefix, "/")
	return Instance{
		Bucket: u.Host,
		Prefix: prefix,
	}, nil
}

// Hash returns a short deterministic identifier derived from the bucket,
// prefix, and endpoint. Used for naming sockets, cache dirs, etc.
func (inst Instance) Hash() string {
	s := inst.Bucket + "/" + inst.Prefix + "@" + inst.Endpoint
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h[:6])
}

// S3URL returns the s3://bucket/prefix URL string.
func (inst Instance) S3URL() string {
	if inst.Prefix == "" {
		return "s3://" + inst.Bucket
	}
	return "s3://" + inst.Bucket + "/" + inst.Prefix
}
