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
	"context"
	"crypto/aes"
	"crypto/md5"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/periodic"
	"github.com/redpanda-data/connect/v4/internal/typed"
)

// ClientOptions is the options to create a Snowflake Snowpipe API Client
type ClientOptions struct {
	// Account name
	Account string
	// username
	User string
	// Snowflake Role (i.e. ACCOUNTADMIN)
	Role string
	// Private key for the user
	PrivateKey *rsa.PrivateKey
	// Logger for... logging?
	Logger *service.Logger
}

type stageUploaderResult struct {
	uploader uploader
	err      error
}

// SnowflakeServiceClient is a port from Java :)
type SnowflakeServiceClient struct {
	client           *restClient
	clientPrefix     string
	deploymentID     int64
	options          ClientOptions
	requestIDCounter int

	uploader          *typed.AtomicValue[stageUploaderResult]
	uploadRefreshLoop *periodic.Periodic
}

// NewSnowflakeServiceClient creates a new API client for the Snowpipe Streaming API
func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := newRestClient(
		opts.Account,
		opts.User,
		opts.PrivateKey,
		opts.Logger,
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
	uploader, err := newUploader(resp.StageLocation)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize stage uploader: %w", err)
	}
	uploaderAtomic := typed.NewAtomicValue(stageUploaderResult{
		uploader: uploader,
	})
	ssc := &SnowflakeServiceClient{
		client:       client,
		clientPrefix: fmt.Sprintf("%s_%d", resp.Prefix, resp.DeploymentID),
		deploymentID: resp.DeploymentID,
		options:      opts,

		uploader: uploaderAtomic,
		// Tokens expire every hour, so refresh a bit before that
		uploadRefreshLoop: periodic.NewWithContext(time.Hour-2*time.Minute, func(ctx context.Context) {
			resp, err := client.ConfigureClient(ctx, clientConfigureRequest{Role: opts.Role})
			if err != nil {
				uploaderAtomic.Store(stageUploaderResult{err: err})
				return
			}
			// TODO: Do the other checks here that the Java SDK does (deploymentID, etc)
			uploader, err := newUploader(resp.StageLocation)
			uploaderAtomic.Store(stageUploaderResult{uploader: uploader, err: err})
		}),
	}
	ssc.uploadRefreshLoop.Start()
	return ssc, nil
}

// Close closes the client and future requests have undefined behavior.
func (c *SnowflakeServiceClient) Close() error {
	c.uploadRefreshLoop.Stop()
	c.client.Close()
	return nil
}

func (c *SnowflakeServiceClient) nextRequestID() string {
	rid := c.requestIDCounter
	c.requestIDCounter++
	return fmt.Sprintf("%s_%d", c.clientPrefix, rid)
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
	// The default timezone for TIMESTAMP_LTZ and TIMESTAMP_TZ columns, defaults to UTC
	DefaultTimeZone *time.Location
}

type encryptionInfo struct {
	encryptionKeyID int64
	encryptionKey   string
}

// OpenChannel creates a new or reuses a channel to load data into a Snowflake table.
func (c *SnowflakeServiceClient) OpenChannel(ctx context.Context, opts ChannelOptions) (*SnowflakeIngestionChannel, error) {
	if opts.DefaultTimeZone == nil {
		opts.DefaultTimeZone = time.UTC
	}
	resp, err := c.client.OpenChannel(ctx, openChannelRequest{
		RequestID: c.nextRequestID(),
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
	schema, transformers, typeMetadata, err := constructParquetSchema(resp.TableColumns)
	if err != nil {
		return nil, err
	}
	ch := &SnowflakeIngestionChannel{
		options:      opts,
		clientPrefix: c.clientPrefix,
		schema:       schema,
		client:       c.client,
		role:         c.options.Role,
		uploader:     c.uploader,
		encryptionInfo: &encryptionInfo{
			encryptionKeyID: resp.EncryptionKeyID,
			encryptionKey:   resp.EncryptionKey,
		},
		clientSequencer: resp.ClientSequencer,
		rowSequencer:    resp.RowSequencer,
		transformers:    transformers,
		fileMetadata:    typeMetadata,
	}
	return ch, nil
}

// SnowflakeIngestionChannel is a write connection to a single table in Snowflake
type SnowflakeIngestionChannel struct {
	options          ChannelOptions
	role             string
	clientPrefix     string
	schema           *parquet.Schema
	client           *restClient
	uploader         *typed.AtomicValue[stageUploaderResult]
	encryptionInfo   *encryptionInfo
	clientSequencer  int64
	rowSequencer     int64
	transformers     map[string]*dataTransformer
	fileMetadata     map[string]string
	requestIDCounter int
}

func (c *SnowflakeIngestionChannel) nextRequestID() string {
	rid := c.requestIDCounter
	c.requestIDCounter++
	return fmt.Sprintf("%s_%d", c.clientPrefix, rid)
}

func messageToRow(msg *service.Message) (map[string]any, error) {
	v, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("error extracting object from message: %w", err)
	}
	row, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object, got: %T", v)
	}
	return row, nil
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, batch service.MessageBatch) error {
	startTime := time.Now()
	for _, t := range c.transformers {
		t.stats.Reset()
	}
	var err error
	rows := make([]map[string]any, len(batch))
	for i, msg := range batch {
		transformed := make(map[string]any, len(c.transformers))
		row, err := messageToRow(msg)
		if err != nil {
			return err
		}
		for k, v := range row {
			name := normalizeColumnName(k)
			t, ok := c.transformers[name]
			if !ok {
				// Skip extra columns
				continue
			}
			transformed[name], err = t.converter(t.stats, v)
			if err != nil {
				return err
			}
		}
		rows[i] = transformed
	}
	blobPath := generateBlobPath(c.clientPrefix, 32, c.requestIDCounter)
	c.requestIDCounter++
	c.fileMetadata["primaryFileId"] = getShortname(blobPath)
	unencrypted, err := writeParquetFile(c.schema, rows, c.fileMetadata)
	if err != nil {
		return err
	}
	metadata, err := readParquetMetadata(unencrypted)
	if err != nil {
		return fmt.Errorf("unable to parse parquet metadata: %w", err)
	}
	// Uncomment out to debug parquet compat bugs...
	// os.WriteFile("latest_test.parquet", unencrypted, 0o644)
	unencryptedLen := len(unencrypted)
	unencrypted = padBuffer(unencrypted, aes.BlockSize)
	encrypted, err := encrypt(unencrypted, c.encryptionInfo.encryptionKey, blobPath, 0)
	if err != nil {
		return err
	}

	uploadStartTime := time.Now()
	fileMD5Hash := md5.Sum(encrypted)
	uploaderResult := c.uploader.Load()
	if uploaderResult.err != nil {
		return fmt.Errorf("failed to acquire stage uploader: %w", uploaderResult.err)
	}
	uploader := uploaderResult.uploader
	err = backoff.Retry(func() error {
		return uploader.upload(ctx, blobPath, encrypted, fileMD5Hash[:])
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
	if err != nil {
		return err
	}

	uploadFinishTime := time.Now()
	columnEpInfo := computeColumnEpInfo(c.transformers)
	resp, err := c.client.RegisterBlob(ctx, registerBlobRequest{
		RequestID: c.nextRequestID(),
		Role:      c.role,
		Blobs: []blobMetadata{
			{
				Path:        blobPath,
				MD5:         hex.EncodeToString(fileMD5Hash[:]),
				BDECVersion: 3,
				BlobStats: blobStats{
					FlushStartMs:     startTime.UnixMilli(),
					BuildDurationMs:  uploadStartTime.UnixMilli() - startTime.UnixMilli(),
					UploadDurationMs: uploadFinishTime.UnixMilli() - uploadStartTime.UnixMilli(),
				},
				Chunks: []chunkMetadata{
					{
						Database:                c.options.DatabaseName,
						Schema:                  c.options.SchemaName,
						Table:                   c.options.TableName,
						ChunkStartOffset:        0,
						ChunkLength:             int32(unencryptedLen),
						ChunkLengthUncompressed: totalUncompressedSize(metadata),
						ChunkMD5:                md5Hash(encrypted[:unencryptedLen]),
						EncryptionKeyID:         c.encryptionInfo.encryptionKeyID,
						FirstInsertTimeInMillis: startTime.UnixMilli(),
						LastInsertTimeInMillis:  startTime.UnixMilli(),
						EPS: &epInfo{
							Rows:    metadata.NumRows,
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
	c.rowSequencer++
	c.clientSequencer = channel.ClientSequencer
	// TODO: we need to validate the offset moved forward...
	return nil
}
