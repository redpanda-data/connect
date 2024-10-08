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
	"crypto/aes"
	"crypto/rsa"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/cenkalti/backoff/v4"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/oauth2"
	gcsopt "google.golang.org/api/option"
)

// ClientOptions
type ClientOptions struct {
	// Account name
	Account string
	// username
	User string
	// Snowflake Role (i.e. ACCOUNTADMIN)
	Role string
	// Private key for the user
	PrivateKey *rsa.PrivateKey
}

// SnowflakeServiceClient is a port from Java :)
type SnowflakeServiceClient struct {
	client        *restClient
	clientPrefix  string
	deploymentID  int64
	stageLocation fileLocationInfo
	options       ClientOptions
}

func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := newRestClient(
		opts.Account,
		opts.User,
		opts.PrivateKey,
		nil,
	)
	if err != nil {
		return nil, err
	}
	resp, err := client.ConfigureClient(ctx, clientConfigureRequest{Role: opts.Role})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		return nil, fmt.Errorf("unable to initialize client - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	return &SnowflakeServiceClient{
		client:        client,
		clientPrefix:  fmt.Sprintf("%s_%d", resp.Prefix, resp.DeploymentID),
		deploymentID:  resp.DeploymentID,
		stageLocation: resp.StageLocation,
		options:       opts,
	}, nil
}

func (c *SnowflakeServiceClient) Close() error {
	c.client.Close()
	return nil
}

// ChannelOptions the parameters to opening a channel using SnowflakeServiceClient
type ChannelOptions struct {
	// Name is the name of the channel
	Name string
	// DatabaseName is the name of the database
	DatabaseName string
	// SchemaName is the name of the schema
	SchemaName string
	// TableName is the name of the table
	TableName string
	// ContinueOnError if true, continue loading rows and return all errors in the response instead of aborting on the the first error
	ContinueOnError bool
	// The default timezone for TIMESTAMP_LTZ and TIMESTAMP_TZ columns, defaults to UTC
	DefaultTimeZone *time.Location
}

type encryptionInfo struct {
	encryptionKeyID int64
	encryptionKey   string
}

func (c *SnowflakeServiceClient) OpenChannel(ctx context.Context, opts ChannelOptions) (*SnowflakeIngestionChannel, error) {
	if opts.DefaultTimeZone == nil {
		opts.DefaultTimeZone = time.UTC
	}
	resp, err := c.client.OpenChannel(ctx, openChannelRequest{
		RequestID: fmt.Sprintf("%s_%d", c.clientPrefix, 1),
		Role:      c.options.Role,
		Channel:   opts.Name,
		Database:  opts.DatabaseName,
		Schema:    opts.SchemaName,
		Table:     opts.TableName,
		WriteMode: "CLOUD_STORAGE",
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		return nil, fmt.Errorf("unable to open channel - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	schema, transformers, err := constructParquetSchema(resp.TableColumns)
	if err != nil {
		return nil, err
	}
	ch := &SnowflakeIngestionChannel{
		options:       opts,
		clientPrefix:  c.clientPrefix,
		schema:        schema,
		client:        c.client,
		stageLocation: c.stageLocation,
		role:          c.options.Role,
		encryptionInfo: &encryptionInfo{
			encryptionKeyID: resp.EncryptionKeyID,
			encryptionKey:   resp.EncryptionKey,
		},
		clientSequencer: resp.ClientSequencer,
		transformers:    transformers,
	}
	return ch, nil
}

// SnowflakeIngestionChannel is a write connection to a single table in Snowflake
type SnowflakeIngestionChannel struct {
	options         ChannelOptions
	role            string
	clientPrefix    string
	schema          *parquet.Schema
	client          *restClient
	stageLocation   fileLocationInfo
	encryptionInfo  *encryptionInfo
	clientSequencer int64
	rowSequencer    int64
	transformers    map[string]*dataTransformer
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, rows []map[string]any) error {
	startTime := time.Now()
	uploader, err := newGCSUploader(ctx, c.stageLocation)
	if err != nil {
		return err
	}
	for _, t := range c.transformers {
		t.stats.Reset()
	}
	for _, row := range rows {
		for k, t := range c.transformers {
			row[k], err = t.converter(t.stats, row[k])
			if err != nil {
				return err
			}
		}
	}
	blobPath := generateBlobPath(c.clientPrefix, 32, 0)
	unencrypted, err := writeParquetFile(c.schema, rows, getShortname(blobPath))
	unencryptedLen := len(unencrypted)
	if err != nil {
		return err
	}
	unencrypted = padBuffer(unencrypted, aes.BlockSize)
	encrypted, err := encrypt(unencrypted, c.encryptionInfo.encryptionKey, blobPath, 0)
	if err != nil {
		return err
	}
	uploadStartTime := time.Now()
	err = backoff.Retry(func() error {
		return uploader.upload(ctx, blobPath, encrypted)
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
	if err != nil {
		return err
	}
	uploadFinishTime := time.Now()
	columnEpInfo := computeColumnEpInfo(c.transformers)
	resp, err := c.client.RegisterBlob(ctx, registerBlobRequest{
		RequestID: fmt.Sprintf("%s_%d", c.clientPrefix, 2),
		Role:      c.role,
		Blobs: []blobMetadata{
			{
				Path:        blobPath,
				MD5:         md5Hash(encrypted),
				BDECVersion: 3,
				BlobStats: blobStats{
					FlushStartMs:     startTime.UnixMilli(),
					BuildDurationMs:  uploadStartTime.UnixMilli() - startTime.UnixMilli(),
					UploadDurationMs: uploadFinishTime.UnixMilli() - uploadStartTime.UnixMilli(),
				},
				Chunks: []chunkMetadata{
					{
						Database:         c.options.DatabaseName,
						Schema:           c.options.SchemaName,
						Table:            c.options.TableName,
						ChunkStartOffset: 0,
						ChunkLength:      int32(unencryptedLen),
						// This is an estimate in the Java SDK, so we're going to be a
						// bit loose here, not sure how important this is to get right.
						// I suppose we could use the parquet metadata to get this
						// information.
						ChunkLengthUncompressed: int32(unencryptedLen),
						ChunkMD5:                md5Hash(encrypted[:unencryptedLen]),
						EncryptionKeyID:         c.encryptionInfo.encryptionKeyID,
						FirstInsertTimeInMillis: startTime.UnixMilli(),
						LastInsertTimeInMillis:  startTime.UnixMilli(),
						EPS: &epInfo{
							Rows:    int64(len(rows)),
							Columns: columnEpInfo,
						},
						Channels: []channelMetadata{
							{
								Channel:          c.options.Name,
								ClientSequencer:  c.clientSequencer,
								RowSequencer:     c.rowSequencer + 1,
								StartOffsetToken: nil,
								EndOffsetToken:   nil,
								OffsetToken:      nil,
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if len(resp.Blobs) != 1 {
		return fmt.Errorf("unexpected number of response blobs: %d", len(resp.Blobs))
	}
	status := resp.Blobs[0]
	if len(status.Chunks) != 1 {
		return fmt.Errorf("unexpected number of response blob chunks: %d", len(status.Chunks))
	}
	chunk := status.Chunks[0]
	if len(chunk.Channels) != 1 {
		return fmt.Errorf("unexpected number of channels for blob chunk: %d", len(chunk.Channels))
	}
	channel := chunk.Channels[0]
	if channel.StatusCode != 0 {
		msg := channel.Message
		if msg == "" {
			msg = "(no message)"
		}
		return fmt.Errorf("error response injesting data (%d): %s", channel.StatusCode, msg)
	}
	c.clientSequencer = channel.ClientSequencer
	return nil
}

func splitBucketAndPath(stageLocation string) (string, string, error) {
	bucketAndPath := strings.SplitN(stageLocation, "/", 2)
	if len(bucketAndPath) != 2 {
		return "", "", fmt.Errorf("unexpected stage location: %s", stageLocation)
	}
	return bucketAndPath[0], bucketAndPath[1], nil
}

func newGCSUploader(ctx context.Context, fileLocationInfo fileLocationInfo) (*gcsUploader, error) {
	if fileLocationInfo.LocationType != "GCS" {
		return nil, fmt.Errorf("unsupported location type: %s", fileLocationInfo.LocationType)
	}
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

type gcsUploader struct {
	bucket     *gcs.BucketHandle
	pathPrefix string
}

func (u *gcsUploader) fullPath(path string) string {
	return filepath.Join(u.pathPrefix, path)
}

func (u *gcsUploader) upload(ctx context.Context, path string, encrypted []byte) error {
	object := u.bucket.Object(u.fullPath(path))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ow := object.NewWriter(ctx)
	_, err := ow.Write(encrypted)
	if err != nil {
		_ = ow.Close()
		return err
	}
	return ow.Close()
}

func writeParquetFile(schema *parquet.Schema, rows []map[string]any, primaryFileID string) ([]byte, error) {
	buf := &bytes.Buffer{}
	pw := parquet.NewGenericWriter[map[string]any](
		buf,
		schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
		// Recommended by the Snowflake team to enable data page stats
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Gzip),
		parquet.KeyValueMetadata("primaryFileId", primaryFileID),
	)
	err := writeWithoutPanic(pw, rows)
	if err != nil {
		return nil, err
	}
	err = closeWithoutPanic(pw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeWithoutPanic[T any](pWtr *parquet.GenericWriter[T], rows []T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	_, err = pWtr.Write(rows)
	return
}

func closeWithoutPanic[T any](pWtr *parquet.GenericWriter[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	err = pWtr.Close()
	return
}
