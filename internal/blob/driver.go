package blob

import (
	"context"
	"io"
)

// GetOpts controls Get behavior.
type GetOpts struct {
	Offset int64 // 0 for start; only used with Length > 0
	Length int64 // 0 means full object
}

// PutOpts controls Put behavior.
type PutOpts struct {
	Size        int64             // content length; -1 if unknown
	IfMatch     string            // CAS: etag must match
	IfNotExists bool              // create-only semantics
	Metadata    map[string]string // user-defined metadata
}

// Driver is the raw backend for object storage operations.
// Implementations handle only cloud SDK calls — no path prefixing,
// no metrics, no retries.
type Driver interface {
	Get(ctx context.Context, key string, opts GetOpts) (body io.ReadCloser, size int64, etag string, err error)
	Put(ctx context.Context, key string, body io.Reader, opts PutOpts) (etag string, err error)
	Delete(ctx context.Context, keys []string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	Head(ctx context.Context, key string) (meta map[string]string, err error)
	SetMeta(ctx context.Context, key string, meta map[string]string) error
}
