package loophole

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/semistrict/loophole/metrics"
)

// S3Store implements ObjectStore backed by an S3-compatible service.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string // includes trailing slash if non-empty
}

// S3Options configures the S3 client. Zero values fall back to
// the corresponding environment variable, then to a built-in default.
type S3Options struct {
	AccessKey string // AWS_ACCESS_KEY_ID
	SecretKey string // AWS_SECRET_ACCESS_KEY
	Region    string // AWS_REGION (default "us-east-1")
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

// NewS3Store creates an S3Store from an Instance.
// If opts is nil, credentials come from environment variables.
// The endpoint from the Instance is used; opts.Endpoint is ignored.
func NewS3Store(ctx context.Context, inst Instance, opts *S3Options) (*S3Store, error) {
	if opts == nil {
		opts = &S3Options{}
	}
	endpoint := inst.Endpoint
	if endpoint == "" {
		endpoint = optOrEnv("", "S3_ENDPOINT", "")
	}
	accessKey := optOrEnv(opts.AccessKey, "AWS_ACCESS_KEY_ID", "")
	secretKey := optOrEnv(opts.SecretKey, "AWS_SECRET_ACCESS_KEY", "")
	region := optOrEnv(opts.Region, "AWS_REGION", "us-east-1")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = true
	})

	// Ensure bucket exists.
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(inst.Bucket),
	})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) &&
			!strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil, fmt.Errorf("create bucket %q: %w", inst.Bucket, err)
		}
	}

	prefix := ""
	if inst.Prefix != "" {
		prefix = inst.Prefix + "/"
	}
	return &S3Store{client: client, bucket: inst.Bucket, prefix: prefix}, nil
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
	return &S3Store{client: s.client, bucket: s.bucket, prefix: p}
}

func (s *S3Store) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, string, error) {
	done := metrics.S3Op("get")
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	}
	if offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}
	out, err := s.client.GetObject(ctx, input)
	done(err)
	if err != nil {
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
	// The AWS SDK determines Content-Length automatically for readers
	// that implement io.Seeker (e.g. *os.File, *bytes.Reader).
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   r,
	})
	done(err)
	if err != nil {
		return fmt.Errorf("put %s: %w", s.fullKey(key), err)
	}
	return nil
}

func (s *S3Store) PutIfNotExists(ctx context.Context, key string, data []byte) (bool, error) {
	// Use S3 conditional writes (If-None-Match: *) for atomic create-if-not-exists.
	// Supported by AWS S3 since August 2024.
	done := metrics.S3Op("put_if_not_exists")
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.fullKey(key)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		IfNoneMatch:   aws.String("*"),
	})
	done(err)
	if err != nil {
		// S3 returns 412 Precondition Failed if the object already exists.
		if strings.Contains(err.Error(), "PreconditionFailed") || strings.Contains(err.Error(), "412") {
			return false, nil
		}
		return false, fmt.Errorf("put-if-not-exists %s: %w", s.fullKey(key), err)
	}
	metrics.S3Transfer("put_if_not_exists", "tx", int64(len(data)))
	return true, nil
}

func (s *S3Store) DeleteObject(ctx context.Context, key string) error {
	done := metrics.S3Op("delete")
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	done(err)
	if err != nil {
		return fmt.Errorf("delete %s: %w", s.fullKey(key), err)
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
