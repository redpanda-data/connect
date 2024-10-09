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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/encoding/thrift"
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
	// Logger for... logging?
	Logger *service.Logger
}

// SnowflakeServiceClient is a port from Java :)
type SnowflakeServiceClient struct {
	client           *restClient
	clientPrefix     string
	deploymentID     int64
	stageLocation    fileLocationInfo
	options          ClientOptions
	requestIDCounter int
}

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
	// TODO: Refresh periodically
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
	stageLocation    fileLocationInfo
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

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, rows []map[string]any) error {
	startTime := time.Now()
	// TODO: we need to fetch new staging creds every once in a while...
	uploader, err := newUploader(ctx, c.stageLocation)
	if err != nil {
		return err
	}
	for _, t := range c.transformers {
		t.stats.Reset()
	}
	for i, row := range rows {
		transformed := make(map[string]any, len(c.transformers))
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
	c.rowSequencer++
	c.clientSequencer = channel.ClientSequencer
	return nil
}

func writeParquetFile(schema *parquet.Schema, rows []map[string]any, metadata map[string]string) ([]byte, error) {
	buf := &bytes.Buffer{}
	pw := parquet.NewGenericWriter[map[string]any](
		buf,
		schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
		// Recommended by the Snowflake team to enable data page stats
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Uncompressed),
	)
	for k, v := range metadata {
		pw.SetKeyValueMetadata(k, v)
	}
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

func readParquetMetadata(parquetFile []byte) (metadata format.FileMetaData, err error) {
	if len(parquetFile) < 8 {
		return format.FileMetaData{}, fmt.Errorf("too small of parquet file: %d", len(parquetFile))
	}
	trailingBytes := parquetFile[len(parquetFile)-8:]
	if string(trailingBytes[4:]) != "PAR1" {
		return metadata, fmt.Errorf("missing magic bytes, got: %q", trailingBytes[4:])
	}
	footerSize := int(binary.LittleEndian.Uint32(trailingBytes))
	if len(parquetFile) < footerSize+8 {
		return metadata, fmt.Errorf("too small of parquet file: %d, footer size: %d", len(parquetFile), footerSize)
	}
	footerBytes := parquetFile[len(parquetFile)-(footerSize+8) : len(parquetFile)-8]
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), footerBytes, &metadata); err != nil {
		return metadata, fmt.Errorf("unable to extract parquet metadata: %w", err)
	}
	return
}

func totalUncompressedSize(metadata format.FileMetaData) int32 {
	var size int64
	for _, rowGroup := range metadata.RowGroups {
		size += rowGroup.TotalByteSize
	}
	return int32(size)
}
