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
	"crypto/md5"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
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
	client           *SnowflakeRestClient
	clientPrefix     string
	deploymentID     int64
	options          ClientOptions
	requestIDCounter int

	uploader          *typed.AtomicValue[stageUploaderResult]
	uploadRefreshLoop *periodic.Periodic
}

// NewSnowflakeServiceClient creates a new API client for the Snowpipe Streaming API
func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := NewRestClient(
		opts.Account,
		opts.User,
		opts.PrivateKey,
		opts.Logger,
	)
	if err != nil {
		return nil, err
	}
	resp, err := client.configureClient(ctx, clientConfigureRequest{Role: opts.Role})
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
			resp, err := client.configureClient(ctx, clientConfigureRequest{Role: opts.Role})
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
}

type encryptionInfo struct {
	encryptionKeyID int64
	encryptionKey   string
}

// OpenChannel creates a new or reuses a channel to load data into a Snowflake table.
func (c *SnowflakeServiceClient) OpenChannel(ctx context.Context, opts ChannelOptions) (*SnowflakeIngestionChannel, error) {
	resp, err := c.client.openChannel(ctx, openChannelRequest{
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
		return nil, fmt.Errorf("unable to open channel %s - status: %d, message: %s", opts.Name, resp.StatusCode, resp.Message)
	}
	schema, transformers, typeMetadata, err := constructParquetSchema(resp.TableColumns)
	if err != nil {
		return nil, err
	}
	ch := &SnowflakeIngestionChannel{
		ChannelOptions: opts,
		clientPrefix:   c.clientPrefix,
		schema:         schema,
		client:         c.client,
		role:           c.options.Role,
		uploader:       c.uploader,
		encryptionInfo: &encryptionInfo{
			encryptionKeyID: resp.EncryptionKeyID,
			encryptionKey:   resp.EncryptionKey,
		},
		clientSequencer: resp.ClientSequencer,
		rowSequencer:    resp.RowSequencer,
		transformers:    transformers,
		fileMetadata:    typeMetadata,
		buffer:          bytes.NewBuffer(nil),
	}
	return ch, nil
}

// OffsetToken is the persisted client offset of a stream. This can be used to implement exactly-once
// processing.
type OffsetToken = string

// ChannelStatus returns the offset token for a channel or an error
func (c *SnowflakeServiceClient) ChannelStatus(ctx context.Context, opts ChannelOptions) (OffsetToken, error) {
	resp, err := c.client.channelStatus(ctx, batchChannelStatusRequest{
		Role: c.options.Role,
		Channels: []channelStatusRequest{
			{
				Name:     opts.Name,
				Table:    opts.TableName,
				Database: opts.DatabaseName,
				Schema:   opts.SchemaName,
			},
		},
	})
	if err != nil {
		return "", err
	}
	if resp.StatusCode != responseSuccess {
		return "", fmt.Errorf("unable to status channel %s - status: %d, message: %s", opts.Name, resp.StatusCode, resp.Message)
	}
	if len(resp.Channels) != 1 {
		return "", fmt.Errorf("failed to fetch channel %s, got %d channels in response", opts.Name, len(resp.Channels))
	}
	channel := resp.Channels[0]
	if channel.StatusCode != responseSuccess {
		return "", fmt.Errorf("unable to status channel %s - status: %d", opts.Name, resp.StatusCode)
	}
	return channel.PersistedOffsetToken, nil
}

// DropChannel drops it like it's hot 🔥
func (c *SnowflakeServiceClient) DropChannel(ctx context.Context, opts ChannelOptions) error {
	resp, err := c.client.dropChannel(ctx, dropChannelRequest{
		RequestID: c.nextRequestID(),
		Role:      c.options.Role,
		Channel:   opts.Name,
		Table:     opts.TableName,
		Database:  opts.DatabaseName,
		Schema:    opts.SchemaName,
	})
	if err != nil {
		return err
	}
	if resp.StatusCode != responseSuccess {
		return fmt.Errorf("unable to drop channel %s - status: %d, message: %s", opts.Name, resp.StatusCode, resp.Message)
	}
	return nil
}

// SnowflakeIngestionChannel is a write connection to a single table in Snowflake
type SnowflakeIngestionChannel struct {
	ChannelOptions
	role             string
	clientPrefix     string
	schema           *parquet.Schema
	client           *SnowflakeRestClient
	uploader         *typed.AtomicValue[stageUploaderResult]
	encryptionInfo   *encryptionInfo
	clientSequencer  int64
	rowSequencer     int64
	transformers     map[string]*dataTransformer
	fileMetadata     map[string]string
	requestIDCounter int
	buffer           *bytes.Buffer
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

// InsertStats holds some basic statistics about the InsertRows operation
type InsertStats struct {
	BuildTime            time.Duration
	UploadTime           time.Duration
	CompressedOutputSize int
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, batch service.MessageBatch) (InsertStats, error) {
	stats := InsertStats{}
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
			return stats, err
		}
		for k, v := range row {
			name := normalizeColumnName(k)
			t, ok := c.transformers[name]
			if !ok {
				// Skip extra columns
				continue
			}
			transformed[name], err = t.converter.ValidateAndConvert(t.stats, v)
			if err != nil {
				return stats, fmt.Errorf("invalid data for column %s: %w", k, err)
			}
		}
		rows[i] = transformed
	}
	fakeThreadID := rand.N(1000)
	blobPath := generateBlobPath(c.clientPrefix, fakeThreadID, c.requestIDCounter)
	c.requestIDCounter++
	c.fileMetadata["primaryFileId"] = getShortname(blobPath)
	c.buffer.Reset()
	err = writeParquetFile(c.buffer, c.schema, rows, c.fileMetadata)
	if err != nil {
		return stats, err
	}
	unencrypted := c.buffer.Bytes()
	metadata, err := readParquetMetadata(unencrypted)
	if err != nil {
		return stats, fmt.Errorf("unable to parse parquet metadata: %w", err)
	}
	// Uncomment out to debug parquet compat bugs...
	os.WriteFile("latest_test.parquet", unencrypted, 0o644)
	unencryptedLen := len(unencrypted)
	unencrypted = padBuffer(unencrypted, aes.BlockSize)
	encrypted, err := encrypt(unencrypted, c.encryptionInfo.encryptionKey, blobPath, 0)
	if err != nil {
		return stats, err
	}
	uploadStartTime := time.Now()
	fileMD5Hash := md5.Sum(encrypted)
	uploaderResult := c.uploader.Load()
	if uploaderResult.err != nil {
		return stats, fmt.Errorf("failed to acquire stage uploader: %w", uploaderResult.err)
	}
	uploader := uploaderResult.uploader
	err = backoff.Retry(func() error {
		return uploader.upload(ctx, blobPath, encrypted, fileMD5Hash[:])
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
	if err != nil {
		return stats, err
	}

	uploadFinishTime := time.Now()
	columnEpInfo := computeColumnEpInfo(c.transformers)
	resp, err := c.client.registerBlob(ctx, registerBlobRequest{
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
						Database:                c.DatabaseName,
						Schema:                  c.SchemaName,
						Table:                   c.TableName,
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
								Channel:          c.Name,
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
		return stats, err
	}
	if len(resp.Blobs) != 1 {
		return stats, fmt.Errorf("unexpected number of response blobs: %d", len(resp.Blobs))
	}
	status := resp.Blobs[0]
	if len(status.Chunks) != 1 {
		return stats, fmt.Errorf("unexpected number of response blob chunks: %d", len(status.Chunks))
	}
	chunk := status.Chunks[0]
	if len(chunk.Channels) != 1 {
		return stats, fmt.Errorf("unexpected number of channels for blob chunk: %d", len(chunk.Channels))
	}
	channel := chunk.Channels[0]
	if channel.StatusCode != 0 {
		msg := channel.Message
		if msg == "" {
			msg = "(no message)"
		}
		return stats, fmt.Errorf("error response injesting data (%d): %s", channel.StatusCode, msg)
	}
	c.rowSequencer++
	c.clientSequencer = channel.ClientSequencer
	stats.CompressedOutputSize = unencryptedLen
	stats.BuildTime = uploadStartTime.Sub(startTime)
	stats.UploadTime = uploadFinishTime.Sub(uploadStartTime)
	// TODO: we need to validate the offset moved forward...
	return stats, nil
}
