// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// LogFetcher reads a single object from a results bucket. MatrixRunner uses it
// to pull each sweep point's full Connect log (potentially ~200KB of rolling
// stats lines) from S3 instead of through SSM's ~24KB-capped stdout window.
type LogFetcher interface {
	Fetch(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

type s3LogFetcher struct {
	client *s3.Client
}

// NewS3LogFetcher builds a LogFetcher backed by the AWS SDK in the given region.
func NewS3LogFetcher(ctx context.Context, region string) (LogFetcher, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &s3LogFetcher{client: s3.NewFromConfig(cfg)}, nil
}

func (f *s3LogFetcher) Fetch(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s/%s: %w", bucket, key, err)
	}
	return out.Body, nil
}

// FakeLogFetcher returns canned content keyed by S3 object key — for tests.
type FakeLogFetcher struct {
	Contents map[string]string
	Errs     map[string]error
}

func (f *FakeLogFetcher) Fetch(_ context.Context, _, key string) (io.ReadCloser, error) {
	if err := f.Errs[key]; err != nil {
		return nil, err
	}
	body, ok := f.Contents[key]
	if !ok {
		return nil, fmt.Errorf("FakeLogFetcher: no canned content for key %q", key)
	}
	return io.NopCloser(bytes.NewReader([]byte(body))), nil
}
