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
	"slices"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

const debug = false

// ClientOptions is the options to create a Snowflake Snowpipe API Client
type ClientOptions struct {
	// Account name
	Account string
	// Account url
	URL string
	// username
	User string
	// Snowflake Role (i.e. ACCOUNTADMIN)
	Role string
	// Private key for the user
	PrivateKey *rsa.PrivateKey
	// Logger for... logging?
	Logger *service.Logger
	// Connect version for the User-Agent in Snowflake
	ConnectVersion string
}

// SnowflakeServiceClient is a port from Java :)
type SnowflakeServiceClient struct {
	client           *SnowflakeRestClient
	clientPrefix     string
	deploymentID     int64
	options          ClientOptions
	requestIDCounter *atomic.Int64

	uploaderManager *uploaderManager

	flusher *asyncroutine.Batcher[blobMetadata, blobRegisterStatus]
}

// NewSnowflakeServiceClient creates a new API client for the Snowpipe Streaming API
func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := NewRestClient(RestOptions{
		Account:    opts.Account,
		URL:        opts.URL,
		User:       opts.User,
		Version:    opts.ConnectVersion,
		PrivateKey: opts.PrivateKey,
		Logger:     opts.Logger,
	})
	if err != nil {
		return nil, err
	}
	resp, err := client.configureClient(ctx, clientConfigureRequest{Role: opts.Role})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		if resp.Message == "" {
			resp.Message = "(no message)"
		}
		return nil, fmt.Errorf("unable to initialize client - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	um := newUploaderManager(client, opts.Role)
	if err := um.Start(ctx); err != nil {
		return nil, err
	}
	ssc := &SnowflakeServiceClient{
		client:       client,
		clientPrefix: fmt.Sprintf("%s_%d", resp.Prefix, resp.DeploymentID),
		deploymentID: resp.DeploymentID,
		options:      opts,

		uploaderManager:  um,
		requestIDCounter: &atomic.Int64{},
	}
	// Flush up to 100 blobs at once, that seems like a fairly high upper bound
	ssc.flusher, err = asyncroutine.NewBatcher(100, ssc.registerBlobs)
	if err != nil {
		um.Stop() // Don't leak the goroutine on failure
		return nil, err
	}
	return ssc, nil
}

// Close closes the client and future requests have undefined behavior.
func (c *SnowflakeServiceClient) Close() {
	c.options.Logger.Debug("closing snowflake streaming output")
	c.uploaderManager.Stop()
	c.client.Close()
	c.flusher.Close()
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
	// How to handle schema differences
	SchemaMode SchemaMode
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
		ChannelOptions:  opts,
		clientPrefix:    c.clientPrefix,
		schema:          schema,
		client:          c.client,
		role:            c.options.Role,
		uploaderManager: c.uploaderManager,
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
		connectVersion:   c.options.ConnectVersion,
	}
	c.options.Logger.Debugf(
		"successfully opened channel %s for table `%s.%s.%s` with client sequencer %v",
		opts.Name,
		opts.DatabaseName,
		opts.SchemaName,
		opts.TableName,
		resp.ClientSequencer,
	)
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
	schema          *parquet.Schema
	client          *SnowflakeRestClient
	uploaderManager *uploaderManager
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
	connectVersion   string
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
	type rowGroup struct {
		rows  []parquet.Row
		stats []*statsBuffer
	}
	rowGroups := []rowGroup{}
	maxChunkSize := c.BuildOptions.ChunkSize
	convertStart := time.Now()
	work := []func() error{}
	for chunk := range slices.Chunk(batch, maxChunkSize) {
		j := len(rowGroups)
		rowGroups = append(rowGroups, rowGroup{})
		work = append(work, func() error {
			rows, stats, err := constructRowGroup(chunk, c.schema, c.transformers, c.SchemaMode)
			rowGroups[j] = rowGroup{rows, stats}
			return err
		})
	}
	var wg errgroup.Group
	wg.SetLimit(c.BuildOptions.Parallelism)
	for _, w := range work {
		wg.Go(w)
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
	w := newParquetWriter(c.connectVersion, c.schema)
	buf, err := w.WriteFile(allRows, metadata)
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

// OffsetTokenRange is the range of offsets for the data being written.
type OffsetTokenRange struct {
	Start, End OffsetToken
}

func (r *OffsetTokenRange) start() *OffsetToken {
	if r == nil {
		return nil
	}
	return &r.Start
}

func (r *OffsetTokenRange) end() *OffsetToken {
	if r == nil {
		return nil
	}
	return &r.End
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, batch service.MessageBatch, offsets *OffsetTokenRange) (InsertStats, error) {
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
	fullMD5Hash := md5.Sum(part.parquetFile)

	uploadStartTime := time.Now()
	for i := range 3 {
		ur := c.uploaderManager.GetUploader()
		if ur.err != nil {
			return insertStats, fmt.Errorf("failed to acquire stage uploader (last fetch time=%v): %w", ur.timestamp, ur.err)
		}
		err = ur.uploader.upload(ctx, blobPath, part.parquetFile, fullMD5Hash[:], map[string]string{
			"ingestclientname": partnerID + "_" + c.Name,
			"ingestclientkey":  c.clientPrefix,
		})
		if err == nil {
			break
		}
		err = fmt.Errorf("unable to upload to storage (last cred refresh time=%v): %w", ur.timestamp, err)
		// Similar to the Java SDK, the first failure we retry immediately after attempting to refresh
		// our uploader. It seems there are some cases where the 1 hour refresh interval is too slow
		// and tokens are only valid for ~30min. This is a poor man's workaround for dynamic token
		// refreshing.
		if i == 0 {
			c.uploaderManager.RefreshUploader(ctx)
			continue
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return insertStats, ctx.Err()
		}
	}
	if err != nil {
		return insertStats, err
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
						StartOffsetToken: offsets.start(),
						EndOffsetToken:   offsets.end(),
						OffsetToken:      nil,
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
			if channel.ClientSequencer != c.clientSequencer {
				msg = fmt.Sprintf(
					"(client sequencer has changed (%v vs %v) - has another process opened this channel?)",
					channel.ClientSequencer,
					c.clientSequencer,
				)
			}
		}
		err = &IngestionFailedError{
			DatabaseName:            c.DatabaseName,
			SchemaName:              c.SchemaName,
			TableName:               c.TableName,
			ChannelName:             c.Name,
			StatusCode:              channel.StatusCode,
			Message:                 msg,
			ExpectedClientSequencer: c.clientSequencer,
			ActualClientSequencer:   channel.ClientSequencer,
		}
		return insertStats, err
	}
	c.rowSequencer++
	c.clientSequencer = channel.ClientSequencer
	c.offsetToken = offsets.end()
	insertStats.CompressedOutputSize = part.unencryptedLen
	insertStats.BuildTime = uploadStartTime.Sub(startTime)
	insertStats.UploadTime = uploadFinishTime.Sub(uploadStartTime)
	insertStats.RegisterTime = time.Since(uploadFinishTime)
	insertStats.ConvertTime = part.convertTime
	insertStats.SerializeTime = part.serializeTime
	return insertStats, err
}

// IngestionFailedError is an error that occurs when registing a BDEC file with Snowflake.
type IngestionFailedError struct {
	DatabaseName, SchemaName, TableName string
	ChannelName                         string
	StatusCode                          int64
	Message                             string
	ExpectedClientSequencer             int64
	ActualClientSequencer               int64
}

// LostOwnership returns true when another channel was opened and this one is invalidated now
func (e *IngestionFailedError) LostOwnership() bool {
	return e.ExpectedClientSequencer != e.ActualClientSequencer || e.StatusCode == responseErrInvalidClientSequencer
}

// CanRetry returns true when it's expected a retry can fix the issue
func (e *IngestionFailedError) CanRetry() bool {
	switch e.StatusCode {
	case responseErrRetryRequest,
		responseErrTransientError,
		responseErrMissingColumnStats:
		return true
	default:
		return false
	}
}

func (e *IngestionFailedError) Error() string {
	return fmt.Sprintf(
		"error response ingesting data to table `%s.%s.%s` on channel `%s` (statusCode=%d): %s",
		e.DatabaseName,
		e.SchemaName,
		e.TableName,
		e.ChannelName,
		e.StatusCode,
		e.Message,
	)
}

// NotCommittedError is when the table is not committed the data asynchronously to Snowflake.
type NotCommittedError struct {
	DatabaseName, SchemaName, TableName string
	ChannelName                         string
	ActualRowSequencer                  int64
	ExpectedRowSequencer                int64
}

func (e *NotCommittedError) Error() string {
	return fmt.Sprintf(
		"row sequencer not yet committed to table `%s.%s.%s` for channel %s: %d < %d",
		e.DatabaseName,
		e.SchemaName,
		e.TableName,
		e.ChannelName,
		e.ActualRowSequencer,
		e.ExpectedRowSequencer)
}

// WaitUntilCommitted waits until all the data in the channel has been committed
// along with how many polls it took to get that.
func (c *SnowflakeIngestionChannel) WaitUntilCommitted(ctx context.Context, timeout time.Duration) (int, error) {
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
			return &NotCommittedError{
				DatabaseName:         c.DatabaseName,
				SchemaName:           c.SchemaName,
				TableName:            c.TableName,
				ChannelName:          c.Name,
				ActualRowSequencer:   status.PersistedRowSequencer,
				ExpectedRowSequencer: c.rowSequencer,
			}
		}
		return nil
	}, backoff.WithContext(
		// 1, 10, 100, 1000, 1000, ...
		backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(time.Millisecond),
			backoff.WithMultiplier(10),
			backoff.WithMaxInterval(time.Second),
			backoff.WithMaxElapsedTime(timeout),
		),
		ctx,
	))
	return polls, err
}

// LatestOffsetToken is the latest offset token written to the channel (not required to be persisted yet).
func (c *SnowflakeIngestionChannel) LatestOffsetToken() *OffsetToken {
	return c.offsetToken
}
