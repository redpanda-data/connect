/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/oauth2"
	gcsopt "google.golang.org/api/option"
)

type uploader interface {
	upload(ctx context.Context, path string, encrypted, md5Hash []byte) error
}

func newUploader(ctx context.Context, fileLocationInfo fileLocationInfo) (uploader, error) {
	switch fileLocationInfo.LocationType {
	case "S3":
		creds := fileLocationInfo.Creds
		awsKeyID := creds["AWS_KEY_ID"]
		awsSecretKey := creds["AWS_SECRET_KEY"]
		awsToken := creds["AWS_TOKEN"]
		// TODO: Handle regional URLs
		if fileLocationInfo.UseS3RegionalURL {
			return nil, fmt.Errorf("S3 Regional URLs are not supported")
		}
		// TODO: Handle EndPoint, the Java SDK says this is only for Azure, but
		// that doesn't seem to be the case from reading the Java JDBC driver,
		// the Golang driver says this is used for FIPS in GovCloud.
		if fileLocationInfo.EndPoint != "" {
			return nil, fmt.Errorf("custom S3 endpoint is not supported")
		}
		client := s3.New(s3.Options{
			Region: fileLocationInfo.Region,
			Credentials: credentials.NewStaticCredentialsProvider(
				awsKeyID,
				awsSecretKey,
				awsToken,
			),
		})
		bucket, pathPrefix, err := splitBucketAndPath(fileLocationInfo.Location)
		if err != nil {
			return nil, err
		}
		uploader := manager.NewUploader(client)
		return &s3Uploader{
			client:     uploader,
			bucket:     bucket,
			pathPrefix: pathPrefix,
		}, nil
	case "GCS":
		accessToken := fileLocationInfo.Creds["GCS_ACCESS_TOKEN"]
		client, err := gcs.NewClient(ctx, gcsopt.WithTokenSource(
			oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: accessToken,
				TokenType:   "Bearer",
			}),
		))
		bucket, prefix, err := splitBucketAndPath(fileLocationInfo.Location)
		if err != nil {
			return nil, err
		}
		return &gcsUploader{
			bucket:     client.Bucket(bucket),
			pathPrefix: prefix,
		}, err
	}
	return nil, fmt.Errorf("unsupported location type: %s", fileLocationInfo.LocationType)
}

type s3Uploader struct {
	client             *manager.Uploader
	bucket, pathPrefix string
}

func (u *s3Uploader) upload(ctx context.Context, path string, encrypted, md5Hash []byte) error {
	input := &s3.PutObjectInput{
		Bucket:     &u.bucket,
		Key:        aws.String(filepath.Join(u.pathPrefix, path)),
		Body:       bytes.NewReader(encrypted),
		ContentMD5: aws.String(base64.StdEncoding.EncodeToString(md5Hash)),
	}
	_, err := u.client.Upload(ctx, input)
	return err
}

type gcsUploader struct {
	bucket     *gcs.BucketHandle
	pathPrefix string
}

func (u *gcsUploader) upload(ctx context.Context, path string, encrypted, md5Hash []byte) error {
	object := u.bucket.Object(filepath.Join(u.pathPrefix, path))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ow := object.NewWriter(ctx)
	ow.MD5 = md5Hash
	for len(encrypted) > 0 {
		n, err := ow.Write(encrypted)
		if err != nil {
			_ = ow.Close()
			return err
		}
		encrypted = encrypted[n:]
	}
	return ow.Close()
}

func splitBucketAndPath(stageLocation string) (string, string, error) {
	bucketAndPath := strings.SplitN(stageLocation, "/", 2)
	if len(bucketAndPath) != 2 {
		return "", "", fmt.Errorf("unexpected stage location: %s", stageLocation)
	}
	return bucketAndPath[0], bucketAndPath[1], nil
}
