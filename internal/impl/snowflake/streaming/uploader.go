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
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/oauth2"
	gcsopt "google.golang.org/api/option"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

type uploader interface {
	upload(ctx context.Context, path string, encrypted, md5Hash []byte, metadata map[string]string) error
}

func newUploader(fileLocationInfo fileLocationInfo) (uploader, error) {
	switch fileLocationInfo.LocationType {
	case "S3":
		creds := fileLocationInfo.Creds
		awsKeyID := creds["AWS_KEY_ID"]
		awsSecretKey := creds["AWS_SECRET_KEY"]
		awsToken := creds["AWS_TOKEN"]
		endpoint := buildS3Endpoint(fileLocationInfo)

		client := s3.New(s3.Options{
			Region:       fileLocationInfo.Region,
			BaseEndpoint: endpoint,
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
		return &s3Uploader{
			client:     client,
			bucket:     bucket,
			pathPrefix: pathPrefix,
		}, nil
	case "GCS":
		accessToken := fileLocationInfo.Creds["GCS_ACCESS_TOKEN"]
		// Even though the GCS uploader takes a context, it's not used because we configure
		// static access token credentials. The context is only used for service account
		// auth via the instance metadata server.
		client, err := gcs.NewClient(context.Background(), gcsopt.WithTokenSource(
			oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: accessToken,
				TokenType:   "Bearer",
			}),
		))
		if err != nil {
			return nil, err
		}
		bucket, prefix, err := splitBucketAndPath(fileLocationInfo.Location)
		if err != nil {
			return nil, err
		}
		return &gcsUploader{
			bucket:     client.Bucket(bucket),
			pathPrefix: prefix,
		}, err
	case "AZURE":
		sasToken := fileLocationInfo.Creds["AZURE_SAS_TOKEN"]
		urlString := fmt.Sprintf("https://%s.%s/%s", fileLocationInfo.StorageAccount, fileLocationInfo.EndPoint, sasToken)
		u, err := url.Parse(urlString)
		if err != nil {
			return nil, fmt.Errorf("invalid azure blob storage url: %w", err)
		}
		client, err := azblob.NewClientWithNoCredential(u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("unable to create azure blob storage client: %w", err)
		}
		container, prefix, err := splitBucketAndPath(fileLocationInfo.Location)
		if err != nil {
			return nil, err
		}
		return &azureUploader{
			client:     client,
			container:  container,
			pathPrefix: prefix,
		}, nil
	}
	return nil, fmt.Errorf("unsupported location type: %s", fileLocationInfo.LocationType)
}

type azureUploader struct {
	client                *azblob.Client
	container, pathPrefix string
}

func (u *azureUploader) upload(ctx context.Context, path string, encrypted, md5Hash []byte, metadata map[string]string) error {
	// We upload in multiple parts, so we have to validate ourselves post upload 😒
	md := map[string]*string{}
	for k, v := range metadata {
		val := v
		md[k] = &val
	}
	o := blockblob.UploadBufferOptions{Metadata: md}
	resp, err := u.client.UploadBuffer(ctx, u.container, filepath.Join(u.pathPrefix, path), encrypted, &o)
	if err != nil {
		return err
	}
	if !bytes.Equal(resp.ContentMD5, md5Hash) {
		return fmt.Errorf("invalid md5 hash got: %s want: %s", hex.EncodeToString(resp.ContentMD5), md5Hash)
	}
	return nil
}

type s3Uploader struct {
	client             *s3.Client
	bucket, pathPrefix string
}

func (u *s3Uploader) upload(ctx context.Context, path string, encrypted, md5Hash []byte, metadata map[string]string) error {
	input := &s3.PutObjectInput{
		Bucket:        &u.bucket,
		Key:           aws.String(filepath.Join(u.pathPrefix, path)),
		ContentLength: aws.Int64(int64(len(encrypted))),
		Body:          bytes.NewReader(encrypted),
		Metadata:      metadata,
		ContentMD5:    aws.String(base64.StdEncoding.EncodeToString(md5Hash)),
	}
	_, err := u.client.PutObject(ctx, input)
	return err
}

type gcsUploader struct {
	bucket     *gcs.BucketHandle
	pathPrefix string
}

func (u *gcsUploader) upload(ctx context.Context, path string, encrypted, md5Hash []byte, metadata map[string]string) error {
	object := u.bucket.Object(filepath.Join(u.pathPrefix, path))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ow := object.NewWriter(ctx)
	ow.Metadata = metadata
	ow.MD5 = md5Hash
	// Prevent resumable uploads and staging files in the bucket by removing the chunk size.
	// https://cloud.google.com/storage/docs/uploading-objects-from-memory#storage-upload-object-from-memory-go
	ow.ChunkSize = 0
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

func buildS3Endpoint(info fileLocationInfo) *string {
	var endpoint *string
	if info.EndPoint != "" {
		endpoint = aws.String("https://" + info.EndPoint)
	} else if info.UseS3RegionalURL && info.Region != "" {
		domainSuffixForRegionalURL := "amazonaws.com"
		if strings.HasPrefix(strings.ToLower(info.Region), "cn-") {
			domainSuffixForRegionalURL = "amazonaws.com.cn"
		}
		endpoint = aws.String(fmt.Sprintf("https://s3.%s.%s", info.Region, domainSuffixForRegionalURL))
	}
	return endpoint
}

type (
	uploaderLoadResult struct {
		uploader uploader
		// Time of when the uploader was created
		timestamp time.Time
		// If there was an error creating the uploader
		err error
	}

	uploaderManager struct {
		state    *uploaderLoadResult
		client   *SnowflakeRestClient
		role     string
		stateMu  sync.RWMutex
		uploadMu sync.Mutex
		periodic asyncroutine.Periodic
	}
)

func newUploaderManager(client *SnowflakeRestClient, role string) *uploaderManager {
	m := &uploaderManager{state: nil, client: client, role: role}
	// According to the Java SDK tokens are refreshed every hour on GCP
	// and 2 hours on AWS. It seems in practice some customers only have
	// tokens that live for 30 minutes, so we need to support ealier
	// refreshes (those are opt in however).
	const refreshTime = time.Hour - time.Minute*5
	m.periodic = *asyncroutine.NewPeriodicWithContext(refreshTime, m.RefreshUploader)
	return m
}

func (m *uploaderManager) Start(ctx context.Context) error {
	m.RefreshUploader(ctx)
	s := m.GetUploader()
	if s.err != nil {
		return s.err
	}
	m.periodic.Start()
	return nil
}

func (m *uploaderManager) GetUploader() *uploaderLoadResult {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state
}

func (m *uploaderManager) RefreshUploader(ctx context.Context) {
	m.uploadMu.Lock()
	defer m.uploadMu.Unlock()
	r := m.GetUploader()
	// Don't refresh sooner than every minute.
	if r != nil && time.Now().Before(r.timestamp.Add(time.Minute)) {
		return
	}
	u, err := backoff.RetryWithData(func() (uploader, error) {
		resp, err := m.client.configureClient(ctx, clientConfigureRequest{Role: m.role})
		if err == nil && resp.StatusCode != responseSuccess {
			msg := "(no message)"
			if resp.Message != "" {
				msg = resp.Message
			}
			err = fmt.Errorf("unable to reconfigure client - status: %d, message: %s", resp.StatusCode, msg)
		}
		if err != nil {
			return nil, err
		}
		// TODO: Do the other checks here that the Java SDK does (deploymentID, etc)
		return newUploader(resp.StageLocation)
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
	if r != nil {
		// Only log when this is running as a background task (so it's a refresh not initial setup).
		if err != nil {
			m.client.logger.Warnf("refreshing snowflake storage credentials failure: %v", err)
		} else {
			m.client.logger.Debug("refreshing snowflake storage credentials success")
		}
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.state = &uploaderLoadResult{uploader: u, timestamp: time.Now(), err: err}
}

func (m *uploaderManager) Stop() {
	m.periodic.Stop()
}
