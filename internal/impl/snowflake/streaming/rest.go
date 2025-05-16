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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
	"github.com/redpanda-data/connect/v4/internal/typed"
)

const (
	responseSuccess                   = 0
	responseTableNotExist             = 4
	responseErrQueueFull              = 7
	responseErrRetryRequest           = 10
	responseErrInvalidClientSequencer = 20
	responseErrTransientError         = 35 // Can be due to schema changes
	responseErrMissingColumnStats     = 40 // Can be due to schema changes

	partnerID = "RedpandaConnect_SnowpipeStreamingSDK"
)

type (
	clientConfigureRequest struct {
		Role     string `json:"role"`
		FileName string `json:"file_name,omitempty"`
	}
	fileLocationInfo struct {
		// The stage type
		LocationType string
		// The container or bucket
		Location string
		// The path of the target file
		Path string
		// The credentials required for the stage
		Creds map[string]string
		// AWS/S3/GCS Region (s3/GCS only)
		Region string
		// The Azure Storage endpoint (Azure only)
		EndPoint string
		// The Azure Storage Account (Azure only)
		StorageAccount string
		// GCS gives us back a presigned URL instead of a cred (obsolete)
		PresignedURL string
		// Whether to encrypt/decrypt files on the stage
		IsClientSideEncrypted bool
		// Whether to use s3 regional URL (AWS only)
		UseS3RegionalURL bool
		// A unique ID for volume assigned by server
		VolumeHash string
	}
	clientConfigureResponse struct {
		Prefix        string           `json:"prefix"`
		StatusCode    int64            `json:"status_code"`
		Message       string           `json:"message"`
		StageLocation fileLocationInfo `json:"stage_location"`
		DeploymentID  int64            `json:"deployment_id"`
	}
	channelStatusRequest struct {
		Table           string `json:"table"`
		Database        string `json:"database"`
		Schema          string `json:"schema"`
		Name            string `json:"channel_name"`
		ClientSequencer *int64 `json:"client_sequencer,omitempty"`
	}
	batchChannelStatusRequest struct {
		Role     string                 `json:"role"`
		Channels []channelStatusRequest `json:"channels"`
	}
	channelStatusResponse struct {
		StatusCode               int64  `json:"status_code"`
		PersistedOffsetToken     string `json:"persisted_offset_token"`
		PersistedClientSequencer int64  `json:"persisted_client_sequencer"`
		PersistedRowSequencer    int64  `json:"persisted_row_sequencer"`
	}
	batchChannelStatusResponse struct {
		StatusCode int64                   `json:"status_code"`
		Message    string                  `json:"message"`
		Channels   []channelStatusResponse `json:"channels"`
	}
	openChannelRequest struct {
		RequestID   string `json:"request_id"`
		Role        string `json:"role"`
		Channel     string `json:"channel"`
		Table       string `json:"table"`
		Database    string `json:"database"`
		Schema      string `json:"schema"`
		WriteMode   string `json:"write_mode"`
		IsIceberg   bool   `json:"is_iceberg,omitempty"`
		OffsetToken string `json:"offset_token,omitempty"`
	}
	columnMetadata struct {
		Name         string  `json:"name"`
		Type         string  `json:"type"`
		LogicalType  string  `json:"logical_type"`
		PhysicalType string  `json:"physical_type"`
		Precision    *int32  `json:"precision"`
		Scale        *int32  `json:"scale"`
		ByteLength   *int32  `json:"byte_length"`
		Length       *int32  `json:"length"`
		Nullable     bool    `json:"nullable"`
		Collation    *string `json:"collation"`
		// The JSON serialization of Iceberg data type of the column,
		// see https://iceberg.apache.org/spec/#appendix-c-json-serialization for more details.
		SourceIcebergDataType *string `json:"source_iceberg_data_type"`
		// The column ordinal is an internal id of the column used by server scanner for the column identification.
		Ordinal int32 `json:"ordinal"`
	}
	openChannelResponse struct {
		StatusCode          int64            `json:"status_code"`
		Message             string           `json:"message"`
		Database            string           `json:"database"`
		Schema              string           `json:"schema"`
		Table               string           `json:"table"`
		Channel             string           `json:"channel"`
		ClientSequencer     int64            `json:"client_sequencer"`
		RowSequencer        int64            `json:"row_sequencer"`
		OffsetToken         *OffsetToken     `json:"offset_token"`
		TableColumns        []columnMetadata `json:"table_columns"`
		EncryptionKey       string           `json:"encryption_key"`
		EncryptionKeyID     int64            `json:"encryption_key_id"`
		IcebergLocationInfo fileLocationInfo `json:"iceberg_location"`
	}
	dropChannelRequest struct {
		RequestID string `json:"request_id"`
		Role      string `json:"role"`
		Channel   string `json:"channel"`
		Table     string `json:"table"`
		Database  string `json:"database"`
		Schema    string `json:"schema"`
		IsIceberg bool   `json:"is_iceberg"`
		// Optionally specify at a specific version
		ClientSequencer *int64 `json:"client_sequencer,omitempty"`
	}
	dropChannelResponse struct {
		StatusCode int64  `json:"status_code"`
		Message    string `json:"message"`
		Database   string `json:"database"`
		Schema     string `json:"schema"`
		Table      string `json:"table"`
		Channel    string `json:"channel"`
	}
	fileColumnProperties struct {
		ColumnOrdinal int32  `json:"columnId"`
		FieldID       *int32 `json:"field_id,omitempty"`
		// current hex-encoded max value, truncated down to 32 bytes
		MinStrValue *string `json:"minStrValue"`
		// current hex-encoded max value, truncated up to 32 bytes
		MaxStrValue  *string         `json:"maxStrValue"`
		MinIntValue  int128.Num      `json:"minIntValue"`
		MaxIntValue  int128.Num      `json:"maxIntValue"`
		MinRealValue json.RawMessage `json:"minRealValue"`
		MaxRealValue json.RawMessage `json:"maxRealValue"`
		NullCount    int64           `json:"nullCount"`
		// Currently not tracked
		DistinctValues int64 `json:"distinctValues"`
		MaxLength      int64 `json:"maxLength"`
		// collated columns do not support ingestion
		// they are always null
		Collation         *string `json:"collation"`
		MinStrNonCollated *string `json:"minStrNonCollated"`
		MaxStrNonCollated *string `json:"maxStrNonCollated"`
	}
	epInfo struct {
		Rows    int64                           `json:"rows"`
		Columns map[string]fileColumnProperties `json:"columns"`
	}
	channelMetadata struct {
		Channel          string       `json:"channel_name"`
		ClientSequencer  int64        `json:"client_sequencer"`
		RowSequencer     int64        `json:"row_sequencer"`
		StartOffsetToken *OffsetToken `json:"start_offset_token"`
		EndOffsetToken   *OffsetToken `json:"end_offset_token"`
		// In the JavaSDK this is always just the end offset version
		OffsetToken *OffsetToken `json:"offset_token"`
	}
	chunkMetadata struct {
		Database                string            `json:"database"`
		Schema                  string            `json:"schema"`
		Table                   string            `json:"table"`
		ChunkStartOffset        int64             `json:"chunk_start_offset"`
		ChunkLength             int32             `json:"chunk_length"`
		ChunkLengthUncompressed int32             `json:"chunk_length_uncompressed"`
		Channels                []channelMetadata `json:"channels"`
		ChunkMD5                string            `json:"chunk_md5"`
		EPS                     *epInfo           `json:"eps,omitempty"`
		EncryptionKeyID         int64             `json:"encryption_key_id,omitempty"`
		FirstInsertTimeInMillis int64             `json:"first_insert_time_in_ms"`
		LastInsertTimeInMillis  int64             `json:"last_insert_time_in_ms"`
	}
	blobStats struct {
		FlushStartMs     int64 `json:"flush_start_ms"`
		BuildDurationMs  int64 `json:"build_duration_ms"`
		UploadDurationMs int64 `json:"upload_duration_ms"`
	}
	blobMetadata struct {
		Path   string          `json:"path"`
		MD5    string          `json:"md5"`
		Chunks []chunkMetadata `json:"chunks"`
		// Currently always 3
		BDECVersion      int8      `json:"bdec_version"`
		SpansMixedTables bool      `json:"spans_mixed_tables"`
		BlobStats        blobStats `json:"blob_stats"`
	}
	registerBlobRequest struct {
		RequestID string         `json:"request_id"`
		Role      string         `json:"role"`
		Blobs     []blobMetadata `json:"blobs"`
		IsIceberg bool           `json:"is_iceberg"`
	}
	channelRegisterStatus struct {
		StatusCode      int64  `json:"status_code"`
		Message         string `json:"message"`
		Channel         string `json:"channel"`
		ClientSequencer int64  `json:"client_sequencer"`
	}
	chunkRegisterStatus struct {
		Channels []channelRegisterStatus `json:"channels"`
		Database string                  `json:"database"`
		Schema   string                  `json:"schema"`
		Table    string                  `json:"table"`
	}
	blobRegisterStatus struct {
		Chunks []chunkRegisterStatus `json:"chunks"`
	}
	registerBlobResponse struct {
		StatusCode int64                `json:"status_code"`
		Message    string               `json:"message"`
		Blobs      []blobRegisterStatus `json:"blobs"`
	}
	// BindingValue is a value available as a binding variable in a SQL statement.
	BindingValue struct {
		// The binding data type, generally TEXT is what you want
		// see: https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement
		Type  string `json:"type"`
		Value string `json:"value"`
	}
	// RunSQLRequest is the way to run a SQL statement
	RunSQLRequest struct {
		Statement string                  `json:"statement"`
		Timeout   int64                   `json:"timeout"`
		Database  string                  `json:"database,omitempty"`
		Schema    string                  `json:"schema,omitempty"`
		Warehouse string                  `json:"warehouse,omitempty"`
		Role      string                  `json:"role,omitempty"`
		Bindings  map[string]BindingValue `json:"bindings,omitempty"`
		// https://docs.snowflake.com/en/sql-reference/parameters
		Parameters map[string]string `json:"parameters,omitempty"`
	}
	// RowType holds metadata for a row
	RowType struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Length    int64  `json:"length"`
		Precision int64  `json:"precision"`
		Scale     int64  `json:"scale"`
		Nullable  bool   `json:"nullable"`
	}
	// ResultSetMetadata holds metadata for the result set
	ResultSetMetadata struct {
		NumRows int64     `json:"numRows"`
		Format  string    `json:"format"`
		RowType []RowType `json:"rowType"`
	}
	// RunSQLResponse is the completed SQL query response
	RunSQLResponse struct {
		ResultSetMetadata  ResultSetMetadata `json:"resultSetMetaData"`
		Data               [][]string        `json:"data"`
		Code               string            `json:"code"`
		StatementStatusURL string            `json:"statementStatusURL"`
		SQLState           string            `json:"sqlState"`
		StatementHandle    string            `json:"statementHandle"`
		Message            string            `json:"message"`
		CreatedOn          int64             `json:"createdOn"`
	}
)

// SnowflakeRestClient allows you to make REST API calls against Snowflake APIs.
type SnowflakeRestClient struct {
	account    string
	url        string
	user       string
	privateKey *rsa.PrivateKey
	client     *http.Client
	version    string
	logger     *service.Logger

	authRefreshLoop *asyncroutine.Periodic
	cachedJWT       *typed.AtomicValue[string]
}

// RestOptions is the options to create a REST client.
type RestOptions struct {
	Account    string
	User       string
	URL        string
	Version    string
	PrivateKey *rsa.PrivateKey
	Logger     *service.Logger
}

// NewRestClient creates a new REST client for the given parameters.
func NewRestClient(opts RestOptions) (c *SnowflakeRestClient, err error) {
	version := strings.TrimLeft(opts.Version, "v")
	// Drop any -rc suffix, Snowflake doesn't like it
	splits := strings.SplitN(version, "-", 2)
	if len(splits) > 1 {
		version = splits[0]
	}
	if version == "" {
		// We can't use a major version <2 so just use 99 as the unknown version
		// this should only show up in development, not released binaries
		version = "99.0.0"
	}
	c = &SnowflakeRestClient{
		account:    opts.Account,
		url:        opts.URL,
		user:       opts.User,
		client:     http.DefaultClient,
		privateKey: opts.PrivateKey,
		logger:     opts.Logger,
		version:    version,
		cachedJWT:  typed.NewAtomicValue(""),
		authRefreshLoop: asyncroutine.NewPeriodic(
			time.Hour-(2*time.Minute),
			func() {
				jwt, err := c.computeJWT()
				// We've already done this once, and there is no external component here
				// so this should never fail, but log just in case...
				if err != nil {
					c.logger.Errorf("unable to mint JWT for snowflake output: %s", err)
					return
				}
				c.cachedJWT.Store(jwt)
			},
		),
	}
	jwt, err := c.computeJWT()
	if err != nil {
		return nil, err
	}
	c.cachedJWT.Store(jwt)
	c.authRefreshLoop.Start()
	return c, nil
}

// Close stops the auth refresh loop for a REST client.
func (c *SnowflakeRestClient) Close() {
	c.authRefreshLoop.Stop()
}

func (c *SnowflakeRestClient) computeJWT() (string, error) {
	pubBytes, err := x509.MarshalPKIXPublicKey(c.privateKey.Public())
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(pubBytes)
	accountName := strings.ToUpper(c.account)
	userName := strings.ToUpper(c.user)
	issueAtTime := time.Now().UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": fmt.Sprintf("%s.%s.%s", accountName, userName, "SHA256:"+base64.StdEncoding.EncodeToString(hash[:])),
		"sub": fmt.Sprintf("%s.%s", accountName, userName),
		"iat": issueAtTime.Unix(),
		"exp": issueAtTime.Add(time.Hour).Unix(),
	})
	return token.SignedString(c.privateKey)
}

// RunSQL executes a series of SQL statements. It's expected that these statements execute in less than 45 seconds so
// we don't have to handle async requests.
func (c *SnowflakeRestClient) RunSQL(ctx context.Context, req RunSQLRequest) (resp RunSQLResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/api/v2/statements?requestId=%s", c.url, requestID), req, &resp)
	return
}

// configureClient configures a client for Snowpipe Streaming.
func (c *SnowflakeRestClient) configureClient(ctx context.Context, req clientConfigureRequest) (resp clientConfigureResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/v1/streaming/client/configure?requestId=%s", c.url, requestID), req, &resp)
	return
}

// channelStatus returns the status of a given channel
func (c *SnowflakeRestClient) channelStatus(ctx context.Context, req batchChannelStatusRequest) (resp batchChannelStatusResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/v1/streaming/channels/status?requestId=%s", c.url, requestID), req, &resp)
	return
}

// openChannel opens a channel for writing
func (c *SnowflakeRestClient) openChannel(ctx context.Context, req openChannelRequest) (resp openChannelResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/v1/streaming/channels/open?requestId=%s", c.url, requestID), req, &resp)
	return
}

// dropChannel drops a channel when it's no longer in use.
func (c *SnowflakeRestClient) dropChannel(ctx context.Context, req dropChannelRequest) (resp dropChannelResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/v1/streaming/channels/drop?requestId=%s", c.url, requestID), req, &resp)
	return
}

// registerBlob registers a blob in object storage to be ingested into Snowflake.
func (c *SnowflakeRestClient) registerBlob(ctx context.Context, req registerBlobRequest) (resp registerBlobResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("%s/v1/streaming/channels/write/blobs?requestId=%s", c.url, requestID), req, &resp)
	return
}

func debugf(l *service.Logger, msg string, args ...any) {
	if debug {
		fmt.Printf("%s\n", fmt.Sprintf(msg, args...))
	}
	l.Tracef(msg, args...)
}

func (c *SnowflakeRestClient) doPost(ctx context.Context, url string, req, resp any) error {
	marshaller := json.Marshal
	if debug {
		marshaller = func(v any) ([]byte, error) {
			return json.MarshalIndent(v, "", "  ")
		}
	}
	reqBody, err := marshaller(req)
	if err != nil {
		return err
	}
	respBody, err := backoff.RetryNotifyWithData(func() ([]byte, error) {
		debugf(c.logger, "making request to %s with body %s", url, reqBody)
		httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
		if errors.Is(err, context.Canceled) {
			return nil, backoff.Permanent(err)
		} else if err != nil {
			return nil, fmt.Errorf("unable to make http request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "application/json")
		httpReq.Header.Set("User-Agent", fmt.Sprintf(partnerID+"/%v", c.version))
		httpReq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
		httpReq.Header.Set("Authorization", "Bearer "+c.cachedJWT.Load())
		r, err := c.client.Do(httpReq)
		if errors.Is(err, context.Canceled) {
			return nil, backoff.Permanent(err)
		} else if err != nil {
			return nil, fmt.Errorf("unable to perform http request: %w", err)
		}
		respBody, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if errors.Is(err, context.Canceled) {
			return nil, backoff.Permanent(err)
		} else if err != nil {
			return nil, fmt.Errorf("unable to read http response: %w", err)
		}
		if r.StatusCode != 200 {
			var restErr APIError
			if unmarshalErr := json.Unmarshal(respBody, &restErr); unmarshalErr == nil && restErr.StatusCode != responseSuccess {
				return nil, &restErr
			}
			return nil, fmt.Errorf("non successful status code (%d): %s", r.StatusCode, respBody)
		}
		debugf(c.logger, "got response to %s with body %s", url, respBody)
		return respBody, nil
	},
		backoff.WithContext(
			backoff.WithMaxRetries(
				backoff.NewConstantBackOff(100*time.Millisecond),
				3,
			),
			ctx,
		),
		func(err error, _ time.Duration) {
			debugf(c.logger, "failed request at %s: %s", url, err)
		},
	)
	if err != nil {
		return err
	}
	err = json.Unmarshal(respBody, resp)
	if err != nil {
		return fmt.Errorf("invalid response: %w, full response: %s", err, respBody[:min(128, len(respBody))])
	}
	return err
}
