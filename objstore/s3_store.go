package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/metrics"
)

// S3Store implements ObjectStore backed by an S3-compatible service.
type S3Store struct {
	client           *s3.Client
	bucket           string
	prefix           string // includes trailing slash if non-empty
	readRetries      int
	readBaseDelay    time.Duration
	sleepWithContext func(context.Context, time.Duration) error
}

const (
	s3ReadRetries   = 2
	s3ReadBaseDelay = 100 * time.Millisecond
)

// optOrEnv returns val if non-empty, otherwise falls back to the
// environment variable, then to fallback.
func optOrEnv(val, envKey, fallback string) string {
	if val != "" {
		return val
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}

// NewS3Store creates an S3Store from a resolved store URL.
func NewS3Store(ctx context.Context, inst env.ResolvedStore) (*S3Store, error) {
	endpoint := optOrEnv(inst.Endpoint, "S3_ENDPOINT", "")
	region := os.Getenv("AWS_REGION")

	var cfgOpts []func(*config.LoadOptions) error
	if region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(region))
	}

	cfgOpts = append(cfgOpts, config.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), 10)
	}))
	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			// Non-AWS S3 providers (Tigris, MinIO, GCS, etc.) typically don't
			// support the newer checksum algorithms that became default in
			// aws-sdk-go-v2 service/s3 v1.73.0+.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
		o.UsePathStyle = true
	})

	// Ensure bucket exists: check first, create only if missing.
	// If HeadBucket returns AccessDenied, assume the bucket exists but the
	// token lacks admin permissions (e.g. R2 read/write-only tokens).
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(inst.Bucket),
	})
	if err != nil && !strings.Contains(err.Error(), "AccessDenied") && !strings.Contains(err.Error(), "Forbidden") {
		// Bucket doesn't exist or we can't tell — try to create it.
		_, createErr := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(inst.Bucket),
		})
		if createErr != nil {
			var owned *types.BucketAlreadyOwnedByYou
			var exists *types.BucketAlreadyExists
			if !errors.As(createErr, &owned) && !errors.As(createErr, &exists) &&
				!strings.Contains(createErr.Error(), "BucketAlreadyOwnedByYou") &&
				!strings.Contains(createErr.Error(), "BucketAlreadyExists") &&
				!strings.Contains(createErr.Error(), "AccessDenied") {
				return nil, fmt.Errorf("create bucket %q: %w", inst.Bucket, createErr)
			}
		}
	}

	prefix := ""
	if inst.Prefix != "" {
		prefix = inst.Prefix + "/"
	}
	return &S3Store{
		client:           client,
		bucket:           inst.Bucket,
		prefix:           prefix,
		readRetries:      s3ReadRetries,
		readBaseDelay:    s3ReadBaseDelay,
		sleepWithContext: sleepWithContext,
	}, nil
}

func (s *S3Store) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + key
}

func (s *S3Store) At(path string) ObjectStore {
	p := path + "/"
	if s.prefix != "" {
		p = s.prefix + p
	}
	return &S3Store{
		client:           s.client,
		bucket:           s.bucket,
		prefix:           p,
		readRetries:      s.readRetries,
		readBaseDelay:    s.readBaseDelay,
		sleepWithContext: s.sleepWithContext,
	}
}

func (s *S3Store) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "Get", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.S3Op("get")
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.fullKey(key)),
		})
		done(err)
		if err != nil {
			var nsk *types.NoSuchKey
			if errors.As(err, &nsk) {
				return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), ErrNotFound)
			}
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		if out.ContentLength != nil {
			metrics.S3Transfer("get", "rx", *out.ContentLength)
		}
		etag := ""
		if out.ETag != nil {
			etag = *out.ETag
		}
		return out.Body, etag, nil
	})
}

func (s *S3Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	return s.getWithRetry(ctx, "GetRange", key, func(ctx context.Context) (io.ReadCloser, string, error) {
		done := metrics.S3Op("get")
		out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.fullKey(key)),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
		})
		done(err)
		if err != nil {
			var nsk *types.NoSuchKey
			if errors.As(err, &nsk) {
				return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), ErrNotFound)
			}
			return nil, "", fmt.Errorf("get %s: %w", s.fullKey(key), err)
		}
		if out.ContentLength != nil {
			metrics.S3Transfer("get", "rx", *out.ContentLength)
		}
		etag := ""
		if out.ETag != nil {
			etag = *out.ETag
		}
		return out.Body, etag, nil
	})
}

func (s *S3Store) getWithRetry(ctx context.Context, op, key string, get func(context.Context) (io.ReadCloser, string, error)) (io.ReadCloser, string, error) {
	var lastErr error
	for attempt := range s.readRetries + 1 {
		body, etag, err := get(ctx)
		if err == nil {
			return body, etag, nil
		}
		if !s.retriableRead(err) {
			return nil, "", err
		}
		lastErr = err
		if attempt == s.readRetries {
			return nil, "", lastErr
		}
		slog.Warn("retrying S3 read", "op", op, "key", s.fullKey(key), "attempt", attempt+1, "error", err)
		if err := s.sleepWithContext(ctx, s.readBaseDelay<<attempt); err != nil {
			return nil, "", lastErr
		}
	}
	panic("unreachable")
}

func (s *S3Store) retriableRead(err error) bool {
	return !errors.Is(err, ErrNotFound)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *S3Store) PutBytes(ctx context.Context, key string, data []byte) error {
	done := metrics.S3Op("put")
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.fullKey(key)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	done(err)
	if err != nil {
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	metrics.S3Transfer("put", "tx", int64(len(data)))
	return nil
}

func (s *S3Store) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	// Use S3 conditional writes (If-Match header) for atomic CAS.
	// Supported by AWS S3 since August 2024.
	done := metrics.S3Op("put_cas")
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.fullKey(key)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		IfMatch:       aws.String(etag),
	})
	done(err)
	if err != nil {
		return "", fmt.Errorf("CAS put %s: %w", s.fullKey(key), err)
	}
	metrics.S3Transfer("put_cas", "tx", int64(len(data)))
	newEtag := ""
	if out.ETag != nil {
		newEtag = *out.ETag
	}
	return newEtag, nil
}

func (s *S3Store) PutReader(ctx context.Context, key string, r io.Reader) error {
	done := metrics.S3Op("put")
	// Wrap to count bytes transferred. Use seekableCountingReader when
	// the underlying reader supports Seek (preserves Content-Length detection).
	var body io.Reader
	cr := &countingReader{r: r}
	if _, ok := r.(io.Seeker); ok {
		body = &seekableCountingReader{countingReader: cr}
	} else {
		body = cr
	}
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   body,
	})
	done(err)
	metrics.S3Transfer("put", "tx", cr.n)
	if err != nil {
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	return nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type seekableCountingReader struct {
	*countingReader
}

func (s *seekableCountingReader) Seek(offset int64, whence int) (int64, error) {
	return s.r.(io.Seeker).Seek(offset, whence)
}

func (s *S3Store) PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error {
	// Use S3 conditional writes (If-None-Match: *) for atomic create-if-not-exists.
	// Supported by AWS S3 since August 2024.
	done := metrics.S3Op("put_if_not_exists")
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.fullKey(key)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		IfNoneMatch:   aws.String("*"),
	}
	if len(meta) > 0 && meta[0] != nil {
		input.Metadata = meta[0]
	}
	_, err := s.client.PutObject(ctx, input)
	done(err)
	if err != nil {
		// S3 returns 412 Precondition Failed if the object already exists.
		if strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412") {
			return ErrExists
		}
		return fmt.Errorf("put-if-not-exists %s: %w", s.fullKey(key), err)
	}
	metrics.S3Transfer("put_if_not_exists", "tx", int64(len(data)))
	return nil
}

const deleteObjectsBatchSize = 500

func (s *S3Store) DeleteObjects(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		batch := keys
		if len(batch) > deleteObjectsBatchSize {
			batch = batch[:deleteObjectsBatchSize]
		}
		keys = keys[len(batch):]

		objects := make([]types.ObjectIdentifier, len(batch))
		for i, key := range batch {
			objects[i] = types.ObjectIdentifier{Key: aws.String(s.fullKey(key))}
		}
		done := metrics.S3Op("delete")
		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		done(err)
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
	}
	return nil
}

func (s *S3Store) HeadMeta(ctx context.Context, key string) (map[string]string, error) {
	done := metrics.S3Op("head")
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	done(err)
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) || strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("head %s: %w", s.fullKey(key), ErrNotFound)
		}
		return nil, fmt.Errorf("head %s: %w", s.fullKey(key), err)
	}
	return out.Metadata, nil
}

func (s *S3Store) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	fk := s.fullKey(key)
	src := s.bucket + "/" + fk
	done := metrics.S3Op("copy")
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(fk),
		CopySource:        aws.String(src),
		Metadata:          meta,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	done(err)
	if err != nil {
		return fmt.Errorf("set meta %s: %w", fk, err)
	}
	return nil
}

func (s *S3Store) ListKeys(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	done := metrics.S3Op("list")
	fullPrefix := s.fullKey(prefix)
	var result []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			done(err)
			return nil, fmt.Errorf("list %s: %w", fullPrefix, err)
		}
		for _, obj := range page.Contents {
			rel := strings.TrimPrefix(*obj.Key, s.prefix)
			if prefix != "" {
				rel = strings.TrimPrefix(rel, prefix)
			}
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			result = append(result, ObjectInfo{Key: rel, Size: size})
		}
	}
	done(nil)
	return result, nil
}
