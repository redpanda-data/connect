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
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/typed"
)

const debug = false

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
	Logger         *service.Logger
	ConnectVersion string
	Application    string
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
	requestIDCounter *atomic.Int64

	uploader          *typed.AtomicValue[stageUploaderResult]
	uploadRefreshLoop *asyncroutine.Periodic

	flusher *asyncroutine.Batcher[blobMetadata, blobRegisterStatus]
}

// NewSnowflakeServiceClient creates a new API client for the Snowpipe Streaming API
func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := NewRestClient(
		opts.Account,
		opts.User,
		opts.ConnectVersion,
		opts.Application,
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
		uploadRefreshLoop: asyncroutine.NewPeriodicWithContext(time.Hour-(2*time.Minute), func(ctx context.Context) {
			resp, err := client.configureClient(ctx, clientConfigureRequest{Role: opts.Role})
			if err != nil {
				uploaderAtomic.Store(stageUploaderResult{err: err})
				return
			}
			// TODO: Do the other checks here that the Java SDK does (deploymentID, etc)
			uploader, err := newUploader(resp.StageLocation)
			uploaderAtomic.Store(stageUploaderResult{uploader: uploader, err: err})
		}),
		requestIDCounter: &atomic.Int64{},
	}
	// Flush up to 100 blobs at once, that seems like a fairly high upper bound
	ssc.flusher, err = asyncroutine.NewBatcher(100, ssc.registerBlobs)
	if err != nil {
		return nil, err
	}
	ssc.uploadRefreshLoop.Start()
	return ssc, nil
}

// Close closes the client and future requests have undefined behavior.
func (c *SnowflakeServiceClient) Close() error {
	c.uploadRefreshLoop.Stop()
	c.client.Close()
	c.flusher.Close()
	return nil
}

func (c *SnowflakeServiceClient) nextRequestID() string {
	rid := c.requestIDCounter.Add(1)
	return fmt.Sprintf("%s_%d", c.clientPrefix, rid)
}

func (c *SnowflakeServiceClient) registerBlobs(ctx context.Context, metadata []blobMetadata) ([]blobRegisterStatus, error) {
	req := registerBlobRequest{
		RequestID: c.nextRequestID(),
		Role:      c.options.Role,
		Blobs:     metadata,
	}
	resp, err := c.client.registerBlob(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		return nil, fmt.Errorf("unable to register blobs - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	return resp.Blobs, nil
}

// BuildOptions is the options for building a parquet file
type BuildOptions struct {
	// The maximum parallelism
	Parallelism int
	// The number of rows to chunk for parallelism
	ChunkSize int
}

// ChannelOptions the parameters to opening a channel using SnowflakeServiceClient
type ChannelOptions struct {
	// ID of this channel, should be unique per channel
	ID int16
	// Name is the name of the channel
	Name string
	// DatabaseName is the name of the database
	DatabaseName string
	// SchemaName is the name of the schema
	SchemaName string
	// TableName is the name of the table
	TableName string
	// The max parallelism used to build parquet files and convert message batches into rows.
	BuildOptions BuildOptions
	// If set to true, don't ignore extra columns in user data, but raise an error.
	StrictSchemaEnforcement bool
}

type encryptionInfo struct {
	encryptionKeyID int64
	encryptionKey   string
}

// OpenChannel creates a new or reuses a channel to load data into a Snowflake table.
func (c *SnowflakeServiceClient) OpenChannel(ctx context.Context, opts ChannelOptions) (*SnowflakeIngestionChannel, error) {
	if opts.BuildOptions.Parallelism <= 0 {
		return nil, fmt.Errorf("invalid build parallelism: %d", opts.BuildOptions.Parallelism)
	}
	if opts.BuildOptions.ChunkSize <= 0 {
		return nil, fmt.Errorf("invalid build chunk size: %d", opts.BuildOptions.ChunkSize)
	}
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
		parquetWriter:  newParquetWriter(c.options.ConnectVersion, schema),
		client:         c.client,
		role:           c.options.Role,
		uploader:       c.uploader,
		encryptionInfo: &encryptionInfo{
			encryptionKeyID: resp.EncryptionKeyID,
			encryptionKey:   resp.EncryptionKey,
		},
		flusher:          c.flusher,
		clientSequencer:  resp.ClientSequencer,
		rowSequencer:     resp.RowSequencer,
		offsetToken:      resp.OffsetToken,
		transformers:     transformers,
		fileMetadata:     typeMetadata,
		requestIDCounter: c.requestIDCounter,
	}
	return ch, nil
}

// OffsetToken is the persisted client offset of a stream. This can be used to implement exactly-once
// processing.
type OffsetToken string

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
	return OffsetToken(channel.PersistedOffsetToken), nil
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
	role            string
	clientPrefix    string
	parquetWriter   *parquetWriter
	schema          *parquet.Schema
	client          *SnowflakeRestClient
	uploader        *typed.AtomicValue[stageUploaderResult]
	flusher         *asyncroutine.Batcher[blobMetadata, blobRegisterStatus]
	encryptionInfo  *encryptionInfo
	clientSequencer int64
	rowSequencer    int64
	offsetToken     *OffsetToken
	transformers    []*dataTransformer
	fileMetadata    map[string]string
	// This is shared among the various open channels to get some uniqueness
	// when naming bdec files
	requestIDCounter *atomic.Int64
}

// InsertStats holds some basic statistics about the InsertRows operation
type InsertStats struct {
	BuildTime            time.Duration
	ConvertTime          time.Duration
	SerializeTime        time.Duration
	UploadTime           time.Duration
	RegisterTime         time.Duration
	CompressedOutputSize int
}

type bdecPart struct {
	unencryptedLen  int
	parquetFile     []byte
	parquetMetadata format.FileMetaData
	stats           []*statsBuffer
	convertTime     time.Duration
	serializeTime   time.Duration
}

func (c *SnowflakeIngestionChannel) constructBdecPart(batch service.MessageBatch, metadata map[string]string) (bdecPart, error) {
	wg := &errgroup.Group{}
	wg.SetLimit(c.BuildOptions.Parallelism)
	type rowGroup struct {
		rows  []parquet.Row
		stats []*statsBuffer
	}
	rowGroups := []rowGroup{}
	maxChunkSize := c.BuildOptions.ChunkSize
	convertStart := time.Now()
	for i := 0; i < len(batch); i += maxChunkSize {
		end := min(maxChunkSize, len(batch[i:]))
		j := len(rowGroups)
		rowGroups = append(rowGroups, rowGroup{})
		chunk := batch[i : i+end]
		wg.Go(func() error {
			rows, stats, err := constructRowGroup(chunk, c.schema, c.transformers, !c.StrictSchemaEnforcement)
			rowGroups[j] = rowGroup{rows, stats}
			return err
		})
	}
	if err := wg.Wait(); err != nil {
		return bdecPart{}, err
	}
	convertDone := time.Now()
	allRows := make([]parquet.Row, 0, len(batch))
	combinedStats := make([]*statsBuffer, len(c.schema.Fields()))
	for i := range combinedStats {
		combinedStats[i] = &statsBuffer{}
	}
	for _, rg := range rowGroups {
		allRows = append(allRows, rg.rows...)
		for i, s := range combinedStats {
			combinedStats[i] = mergeStats(s, rg.stats[i])
		}
	}
	// TODO(perf): It would be really nice to be able to compress in parallel,
	// that actually ends up taking quite of bit of CPU.
	buf, err := c.parquetWriter.WriteFile(allRows, metadata)
	if err != nil {
		return bdecPart{}, err
	}
	fileMetadata, err := readParquetMetadata(buf)
	if err != nil {
		return bdecPart{}, fmt.Errorf("unable to parse parquet metadata: %w", err)
	}
	done := time.Now()
	return bdecPart{
		unencryptedLen:  len(buf),
		parquetFile:     buf,
		parquetMetadata: fileMetadata,
		stats:           combinedStats,
		convertTime:     convertDone.Sub(convertStart),
		serializeTime:   done.Sub(convertDone),
	}, err
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, batch service.MessageBatch, offsetToken *OffsetToken) (InsertStats, error) {
	insertStats := InsertStats{}
	if len(batch) == 0 {
		return insertStats, nil
	}

	startTime := time.Now()
	// Prevent multiple channels from having the same bdec file (it must be globally unique)
	// so add the ID of the channel in the upper 16 bits and then get 48 bits of randomness outside that.
	fakeThreadID := (int64(c.ID) << 48) | rand.Int64N(1<<48)
	blobPath := generateBlobPath(c.clientPrefix, fakeThreadID, c.requestIDCounter.Add(1))
	// This is extra metadata that is required for functionality in snowflake.
	c.fileMetadata["primaryFileId"] = path.Base(blobPath)
	part, err := c.constructBdecPart(batch, c.fileMetadata)
	if err != nil {
		return insertStats, fmt.Errorf("unable to construct output: %w", err)
	}
	if debug {
		_ = os.WriteFile("latest_test.parquet", part.parquetFile, 0o644)
	}

	unencrypted := padBuffer(part.parquetFile, aes.BlockSize)
	part.parquetFile, err = encrypt(unencrypted, c.encryptionInfo.encryptionKey, blobPath, 0)
	if err != nil {
		return insertStats, fmt.Errorf("unable to encrypt output: %w", err)
	}

	uploadStartTime := time.Now()
	uploaderResult := c.uploader.Load()
	if uploaderResult.err != nil {
		return insertStats, fmt.Errorf("failed to acquire stage uploader: %w", uploaderResult.err)
	}
	uploader := uploaderResult.uploader
	fullMD5Hash := md5.Sum(part.parquetFile)
	err = backoff.Retry(func() error {
		return uploader.upload(ctx, blobPath, part.parquetFile, fullMD5Hash[:])
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
	if err != nil {
		return insertStats, fmt.Errorf("unable to upload to storage: %w", err)
	}

	uploadFinishTime := time.Now()

	resp, err := c.flusher.Submit(ctx, blobMetadata{
		Path:        blobPath,
		MD5:         hex.EncodeToString(fullMD5Hash[:]),
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
				ChunkLength:             int32(part.unencryptedLen),
				ChunkLengthUncompressed: totalUncompressedSize(part.parquetMetadata),
				ChunkMD5:                md5Hash(part.parquetFile[:part.unencryptedLen]),
				EncryptionKeyID:         c.encryptionInfo.encryptionKeyID,
				FirstInsertTimeInMillis: startTime.UnixMilli(),
				LastInsertTimeInMillis:  startTime.UnixMilli(),
				EPS: &epInfo{
					Rows:    part.parquetMetadata.NumRows,
					Columns: computeColumnEpInfo(c.transformers, part.stats),
				},
				Channels: []channelMetadata{
					{
						Channel:          c.Name,
						ClientSequencer:  c.clientSequencer,
						RowSequencer:     c.rowSequencer + 1,
						StartOffsetToken: nil,
						EndOffsetToken:   nil,
						OffsetToken:      offsetToken,
					},
				},
			},
		},
	})
	if err != nil {
		return insertStats, fmt.Errorf("registering output failed: %w", err)
	}
	if len(resp.Chunks) != 1 {
		return insertStats, fmt.Errorf("unexpected number of response blob chunks: %d", len(resp.Chunks))
	}
	chunk := resp.Chunks[0]
	if len(chunk.Channels) != 1 {
		return insertStats, fmt.Errorf("unexpected number of channels for blob chunk: %d", len(chunk.Channels))
	}
	channel := chunk.Channels[0]
	if channel.StatusCode != responseSuccess {
		msg := channel.Message
		if msg == "" {
			msg = "(no message)"
		}
		return insertStats, fmt.Errorf("error response ingesting data (%d): %s", channel.StatusCode, msg)
	}
	c.rowSequencer++
	c.clientSequencer = channel.ClientSequencer
	c.offsetToken = offsetToken
	insertStats.CompressedOutputSize = part.unencryptedLen
	insertStats.BuildTime = uploadStartTime.Sub(startTime)
	insertStats.UploadTime = uploadFinishTime.Sub(uploadStartTime)
	insertStats.RegisterTime = time.Since(uploadFinishTime)
	insertStats.ConvertTime = part.convertTime
	insertStats.SerializeTime = part.serializeTime
	return insertStats, err
}

// WaitUntilCommitted waits until all the data in the channel has been committed
// along with how many polls it took to get that.
func (c *SnowflakeIngestionChannel) WaitUntilCommitted(ctx context.Context) (int, error) {
	var polls int
	err := backoff.Retry(func() error {
		polls++
		resp, err := c.client.channelStatus(ctx, batchChannelStatusRequest{
			Role: c.role,
			Channels: []channelStatusRequest{
				{
					Table:           c.TableName,
					Database:        c.DatabaseName,
					Schema:          c.SchemaName,
					Name:            c.Name,
					ClientSequencer: &c.clientSequencer,
				},
			},
		})
		if err != nil {
			return err
		}
		if resp.StatusCode != responseSuccess {
			msg := resp.Message
			if msg == "" {
				msg = "(no message)"
			}
			return fmt.Errorf("error fetching channel status (%d): %s", resp.StatusCode, msg)
		}
		if len(resp.Channels) != 1 {
			return fmt.Errorf("unexpected number of channels for status request: %d", len(resp.Channels))
		}
		status := resp.Channels[0]
		if status.PersistedClientSequencer != c.clientSequencer {
			return backoff.Permanent(errors.New("channel client seqno has advanced - another process has reopened this channel"))
		}
		if status.PersistedRowSequencer < c.rowSequencer {
			return fmt.Errorf("row sequencer not yet committed: %d < %d", status.PersistedRowSequencer, c.rowSequencer)
		}
		return nil
	}, backoff.WithContext(
		// 1, 10, 100, 1000, 1000, ...
		backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(time.Millisecond),
			backoff.WithMultiplier(10),
			backoff.WithMaxInterval(time.Second),
			backoff.WithMaxElapsedTime(10*time.Minute),
		),
		ctx,
	))
	return polls, err
}

// LatestOffsetToken is the latest offset token written to the channel (not required to be persisted yet).
func (c *SnowflakeIngestionChannel) LatestOffsetToken() *OffsetToken {
	return c.offsetToken
}
