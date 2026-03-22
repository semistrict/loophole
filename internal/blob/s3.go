package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/semistrict/loophole/internal/env"
)

// S3Driver implements Driver backed by an S3-compatible service.
type S3Driver struct {
	client *s3.Client
	bucket string
}

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

// NewS3Driver creates an S3Driver from a resolved store URL.
func NewS3Driver(ctx context.Context, inst env.ResolvedStore) (*S3Driver, error) {
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

	return &S3Driver{client: client, bucket: inst.Bucket}, nil
}

func (d *S3Driver) Get(ctx context.Context, key string, opts GetOpts) (io.ReadCloser, int64, string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	}
	if opts.Length > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", opts.Offset, opts.Offset+opts.Length-1))
	}
	out, err := d.client.GetObject(ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, 0, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, 0, "", fmt.Errorf("get %s: %w", key, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return out.Body, size, etag, nil
}

func (d *S3Driver) Put(ctx context.Context, key string, body io.Reader, opts PutOpts) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if opts.Size >= 0 {
		input.ContentLength = aws.Int64(opts.Size)
	}
	if opts.IfMatch != "" {
		input.IfMatch = aws.String(opts.IfMatch)
	}
	if opts.IfNotExists {
		input.IfNoneMatch = aws.String("*")
	}
	if opts.Metadata != nil {
		input.Metadata = opts.Metadata
	}
	out, err := d.client.PutObject(ctx, input)
	if err != nil {
		if opts.IfNotExists && (strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412")) {
			return "", ErrExists
		}
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return etag, nil
}

const deleteObjectsBatchSize = 500

func (d *S3Driver) Delete(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		batch := keys
		if len(batch) > deleteObjectsBatchSize {
			batch = batch[:deleteObjectsBatchSize]
		}
		keys = keys[len(batch):]

		objects := make([]types.ObjectIdentifier, len(batch))
		for i, key := range batch {
			objects[i] = types.ObjectIdentifier{Key: aws.String(key)}
		}
		_, err := d.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(d.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
	}
	return nil
}

func (d *S3Driver) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var result []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			result = append(result, ObjectInfo{Key: *obj.Key, Size: size})
		}
	}
	return result, nil
}

func (d *S3Driver) Head(ctx context.Context, key string) (map[string]string, error) {
	out, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) || strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("head %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("head %s: %w", key, err)
	}
	return out.Metadata, nil
}

func (d *S3Driver) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	src := d.bucket + "/" + key
	_, err := d.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            aws.String(d.bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(src),
		Metadata:          meta,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	if err != nil {
		return fmt.Errorf("set meta %s: %w", key, err)
	}
	return nil
}
