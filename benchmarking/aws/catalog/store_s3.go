// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of *s3.Client the S3 store uses.
type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

var _ S3API = (*s3.Client)(nil)

type s3Store struct {
	api    S3API
	bucket string
}

// NewS3Store returns a Store backed by an S3 bucket.
func NewS3Store(api S3API, bucket string) Store {
	return &s3Store{api: api, bucket: bucket}
}

func (s *s3Store) Put(ctx context.Context, e Entry) error {
	body, err := checkPut(e)
	if err != nil {
		return err
	}
	_, err = s.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(Key(e)),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("putting s3://%s/%s: %w", s.bucket, Key(e), err)
	}
	return nil
}

func (s *s3Store) List(ctx context.Context, input string) ([]Entry, error) {
	if err := checkListInput(input); err != nil {
		return nil, err
	}
	var out []Entry
	var token *string
	for {
		page, err := s.api.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(listPrefix(input)),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("listing s3://%s: %w", s.bucket, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".json") {
				continue
			}
			e, ok, err := s.get(ctx, key)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, e)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return out, nil
		}
		if page.NextContinuationToken == nil {
			// Returning here would silently drop the remaining entries.
			return nil, fmt.Errorf("listing s3://%s: truncated page without a continuation token", s.bucket)
		}
		token = page.NextContinuationToken
	}
}

func (s *s3Store) get(ctx context.Context, key string) (Entry, bool, error) {
	res, err := s.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return Entry{}, false, fmt.Errorf("getting s3://%s/%s: %w", s.bucket, key, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return Entry{}, false, fmt.Errorf("reading s3://%s/%s: %w", s.bucket, key, err)
	}
	return decodeEntry(key, body)
}
