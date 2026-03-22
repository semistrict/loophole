package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/semistrict/loophole/internal/env"
	"github.com/semistrict/loophole/internal/util"
)

// GCSDriver implements Driver backed by Google Cloud Storage.
type GCSDriver struct {
	client *storage.Client
	bucket string
}

// NewGCSDriver creates a GCSDriver from a resolved store URL.
func NewGCSDriver(ctx context.Context, inst env.ResolvedStore) (*GCSDriver, error) {
	var opts []option.ClientOption
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	return &GCSDriver{client: client, bucket: inst.Bucket}, nil
}

func (d *GCSDriver) obj(key string) *storage.ObjectHandle {
	return d.client.Bucket(d.bucket).Object(key)
}

func (d *GCSDriver) Get(ctx context.Context, key string, opts GetOpts) (io.ReadCloser, int64, string, error) {
	obj := d.obj(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, 0, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, 0, "", fmt.Errorf("get %s: %w", key, err)
	}

	pinned := obj.Generation(attrs.Generation)
	var r *storage.Reader
	var size int64
	if opts.Length > 0 {
		r, err = pinned.NewRangeReader(ctx, opts.Offset, opts.Length)
		size = opts.Length
	} else {
		r, err = pinned.NewReader(ctx)
		size = attrs.Size
	}
	if err != nil {
		return nil, 0, "", fmt.Errorf("get %s: %w", key, err)
	}
	return r, size, attrs.Etag, nil
}

func (d *GCSDriver) Put(ctx context.Context, key string, body io.Reader, opts PutOpts) (string, error) {
	obj := d.obj(key)

	if opts.IfMatch != "" {
		// CAS: get generation for this etag, then use GenerationMatch.
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			return "", fmt.Errorf("put %s: attrs: %w", key, err)
		}
		if attrs.Etag != opts.IfMatch {
			return "", fmt.Errorf("put %s: etag mismatch", key)
		}
		obj = obj.If(storage.Conditions{GenerationMatch: attrs.Generation})
	} else if opts.IfNotExists {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}

	w := obj.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if opts.Metadata != nil {
		w.Metadata = opts.Metadata
	}

	if _, err := io.Copy(w, body); err != nil {
		util.SafeClose(w, "close GCS writer")
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		if opts.IfNotExists && isPreconditionFailed(err) {
			return "", ErrExists
		}
		return "", fmt.Errorf("put %s: %w", key, err)
	}

	// For CAS, get the new etag.
	if opts.IfMatch != "" {
		newAttrs, err := d.obj(key).Attrs(ctx)
		if err != nil {
			return "", fmt.Errorf("put %s: get new etag: %w", key, err)
		}
		return newAttrs.Etag, nil
	}
	return "", nil
}

func isPreconditionFailed(err error) bool {
	return strings.Contains(err.Error(), "conditionNotMet") ||
		strings.Contains(err.Error(), "412") ||
		strings.Contains(err.Error(), "PreconditionFailed")
}

func (d *GCSDriver) Delete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		err := d.obj(key).Delete(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				continue
			}
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}

func (d *GCSDriver) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var result []ObjectInfo
	it := d.client.Bucket(d.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				break
			}
			if err.Error() == "no more items in iterator" {
				break
			}
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		result = append(result, ObjectInfo{Key: attrs.Name, Size: attrs.Size})
	}
	return result, nil
}

func (d *GCSDriver) Head(ctx context.Context, key string) (map[string]string, error) {
	attrs, err := d.obj(key).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("head %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("head %s: %w", key, err)
	}
	return attrs.Metadata, nil
}

func (d *GCSDriver) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	_, err := d.obj(key).Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: meta,
	})
	if err != nil {
		return fmt.Errorf("set meta %s: %w", key, err)
	}
	return nil
}
