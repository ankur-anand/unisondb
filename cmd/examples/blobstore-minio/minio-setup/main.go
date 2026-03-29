package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	defaultMinioAccessKey = "minioadmin"
	defaultMinioSecretKey = "minioadmin"
	defaultMinioRegion    = "us-east-1"

	minioReadyTimeout  = 20 * time.Second
	minioRetryInterval = 250 * time.Millisecond
)

func main() {
	var (
		bucketURL = flag.String("bucket-url", defaultBucketURL("unisondb-blob-demo"), "S3/MinIO bucket URL")
		prefix    = flag.String("prefix", "unisondb/examples/blobstore-minio", "Prefix to clear when reset is enabled")
		reset     = flag.Bool("reset", false, "delete all objects under the given prefix before running")
	)
	flag.Parse()

	ensureAWSDefaults()
	ctx := context.Background()

	client, bucket, err := newS3Client(ctx, *bucketURL)
	if err != nil {
		log.Fatalf("create s3 client: %v", err)
	}

	if err := retryMinioTransient(ctx, "ensure bucket", func(opCtx context.Context) error {
		return ensureBucket(opCtx, client, bucket)
	}); err != nil {
		log.Fatalf("ensure bucket %q: %v", bucket, err)
	}

	if *reset {
		if err := retryMinioTransient(ctx, "reset prefix", func(opCtx context.Context) error {
			return deletePrefix(opCtx, client, bucket, *prefix)
		}); err != nil {
			log.Fatalf("reset prefix %q in bucket %q: %v", *prefix, bucket, err)
		}
	}

	log.Printf("minio bucket ready: %s prefix=%s", bucket, *prefix)
}

func ensureAWSDefaults() {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		_ = os.Setenv("AWS_ACCESS_KEY_ID", defaultMinioAccessKey)
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		_ = os.Setenv("AWS_SECRET_ACCESS_KEY", defaultMinioSecretKey)
	}
	if os.Getenv("AWS_REGION") == "" {
		_ = os.Setenv("AWS_REGION", defaultMinioRegion)
	}
}

func defaultBucketURL(bucket string) string {
	return fmt.Sprintf("s3://%s?endpoint=http://127.0.0.1:9000&region=%s&use_path_style=true&response_checksum_validation=when_required&request_checksum_calculation=when_required",
		bucket, defaultMinioRegion)
}

func newS3Client(ctx context.Context, bucketURL string) (*s3.Client, string, error) {
	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, "", fmt.Errorf("parse bucket url: %w", err)
	}
	if u.Host == "" {
		return nil, "", fmt.Errorf("bucket name missing in url %q", bucketURL)
	}

	endpoint := u.Query().Get("endpoint")
	region := u.Query().Get("region")
	if region == "" {
		region = defaultMinioRegion
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		)),
	)
	if err != nil {
		return nil, "", fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = true
	})

	return client, u.Host, nil
}

func ensureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists" {
			return nil
		}
	}

	return err
}

func deletePrefix(ctx context.Context, client *s3.Client, bucket, prefix string) error {
	var continuation *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(strings.TrimPrefix(prefix, "/")),
			ContinuationToken: continuation,
		})
		if err != nil {
			return err
		}

		if len(out.Contents) > 0 {
			objects := make([]types.ObjectIdentifier, 0, len(out.Contents))
			for _, obj := range out.Contents {
				if obj.Key == nil {
					continue
				}
				objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
			}
			if len(objects) > 0 {
				_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
				})
				if err != nil {
					return err
				}
			}
		}

		if !aws.ToBool(out.IsTruncated) {
			return nil
		}
		continuation = out.NextContinuationToken
	}
}

func retryMinioTransient(ctx context.Context, opName string, fn func(context.Context) error) error {
	opCtx, cancel := context.WithTimeout(ctx, minioReadyTimeout)
	defer cancel()

	var lastErr error
	for {
		err := fn(opCtx)
		if err == nil {
			return nil
		}
		if !isTransientMinioError(err) {
			return err
		}

		lastErr = err
		select {
		case <-time.After(minioRetryInterval):
		case <-opCtx.Done():
			return fmt.Errorf("%s: minio not ready after %s: %w", opName, minioReadyTimeout, lastErr)
		}
	}
}

func isTransientMinioError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "eof")
}
