// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package plan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3API is the subset of *s3.Client the plan store and lock use.
type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

var _ S3API = (*s3.Client)(nil)

type s3Objects struct {
	api    S3API
	bucket string
}

// NewS3Objects returns Objects backed by an S3 bucket, using S3 conditional
// writes (If-None-Match / If-Match) for Create, Replace and Delete.
func NewS3Objects(api S3API, bucket string) Objects { return &s3Objects{api: api, bucket: bucket} }

// mapErr turns S3's 412 PreconditionFailed and 409 ConditionalRequestConflict
// into ErrConflict, and a missing key into ErrNotFound.
func mapErr(err error) error {
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return ErrNotFound
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrConflict
		case "NoSuchKey", "NotFound":
			return ErrNotFound
		}
	}
	return err
}

// put writes conditionally. S3 answers 404 NoSuchKey, not 412, to a put with
// If-Match on a missing key; for such a put that is a lost race (ErrConflict).
func (o *s3Objects) put(ctx context.Context, key string, body []byte, in *s3.PutObjectInput) error {
	in.Bucket, in.Key = aws.String(o.bucket), aws.String(key)
	in.Body, in.ContentType = bytes.NewReader(body), aws.String("application/json")
	if _, err := o.api.PutObject(ctx, in); err != nil {
		m := mapErr(err)
		if errors.Is(m, ErrConflict) || (in.IfMatch != nil && errors.Is(m, ErrNotFound)) {
			return ErrConflict
		}
		return fmt.Errorf("putting s3://%s/%s: %w", o.bucket, key, err)
	}
	return nil
}

func (o *s3Objects) Create(ctx context.Context, key string, body []byte) error {
	return o.put(ctx, key, body, &s3.PutObjectInput{IfNoneMatch: aws.String("*")})
}

func (o *s3Objects) Replace(ctx context.Context, key string, body []byte, etag string) error {
	return o.put(ctx, key, body, &s3.PutObjectInput{IfMatch: aws.String(etag)})
}

func (o *s3Objects) Get(ctx context.Context, key string) ([]byte, string, error) {
	res, err := o.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key)})
	if err != nil {
		if m := mapErr(err); errors.Is(m, ErrNotFound) {
			return nil, "", m
		}
		return nil, "", fmt.Errorf("getting s3://%s/%s: %w", o.bucket, key, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading s3://%s/%s: %w", o.bucket, key, err)
	}
	return body, aws.ToString(res.ETag), nil
}

func (o *s3Objects) Delete(ctx context.Context, key, etag string) error {
	_, err := o.api.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key), IfMatch: aws.String(etag)})
	if err != nil {
		switch m := mapErr(err); {
		case errors.Is(m, ErrNotFound):
			return nil
		case errors.Is(m, ErrConflict):
			return m
		}
		return fmt.Errorf("deleting s3://%s/%s: %w", o.bucket, key, err)
	}
	return nil
}

func (o *s3Objects) List(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(o.bucket), Prefix: aws.String(prefix)}
	for {
		page, err := o.api.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("listing s3://%s/%s: %w", o.bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		in.ContinuationToken = page.NextContinuationToken
	}
	sort.Strings(keys)
	return keys, nil
}
