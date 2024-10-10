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
	"runtime"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/periodic"
	"github.com/redpanda-data/connect/v4/internal/typed"
)

const (
	responseSuccess         = 0
	responseErrRetryRequest = 10
	responseErrQueueFull    = 7

	// TODO: figure out how to plumb this down
	version = "4.38.0"
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
		ClientSequencer int64  `json:"client_sequencer"`
	}
	batchChannelStatusRequest struct {
		Role     string                 `json:"role"`
		Channels []channelStatusRequest `json:"channels"`
	}
	channelStatusResponse struct {
		StatusCode               int64  `json:"status_code"`
		PersistedOffsetToken     string `json:"persisted_offset_token"`
		PersistedClientSequencer string `json:"persisted_client_sequencer"`
		PersistedRowSequencer    string `json:"persisted_row_sequencer"`
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
		TableColumns        []columnMetadata `json:"table_columns"`
		EncryptionKey       string           `json:"encryption_key"`
		EncryptionKeyID     int64            `json:"encryption_key_id"`
		IcebergLocationInfo fileLocationInfo `json:"iceberg_location"`
	}
	dropChannelRequest struct {
		RequestID string `json:"request_id"`
		Role      string `json:"role"`
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
		MaxStrValue  *string `json:"maxStrValue"`
		MinIntValue  int64   `json:"minIntValue"`
		MaxIntValue  int64   `json:"maxIntValue"`
		MinRealValue float64 `json:"minRealValue"`
		MaxRealValue float64 `json:"maxRealValue"`
		NullCount    int64   `json:"nullCount"`
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
		Channel          string  `json:"channel_name"`
		ClientSequencer  int64   `json:"client_sequencer"`
		RowSequencer     int64   `json:"row_sequencer"`
		StartOffsetToken *string `json:"start_offset_token"`
		EndOffsetToken   *string `json:"end_offset_token"`
		// In the JavaSDK this is always just the end offset version
		OffsetToken *string `json:"offset_token"`
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
)

type restClient struct {
	account    string
	user       string
	privateKey *rsa.PrivateKey
	client     *http.Client
	userAgent  string
	logger     *service.Logger

	authRefreshLoop *periodic.Periodic
	cachedJWT       *typed.AtomicValue[string]
}

func newRestClient(account, user string, privateKey *rsa.PrivateKey, logger *service.Logger) (c *restClient, err error) {
	userAgent := fmt.Sprintf("RedpandaConnect/%v (%v-%v) %v/%v",
		version,
		runtime.GOOS,
		runtime.GOARCH,
		runtime.Compiler,
		runtime.Version())
	c = &restClient{
		account:    account,
		user:       user,
		client:     http.DefaultClient,
		privateKey: privateKey,
		userAgent:  userAgent,
		logger:     logger,
		cachedJWT:  typed.NewAtomicValue(""),
		authRefreshLoop: periodic.New(
			time.Hour-(2*time.Minute),
			func() {
				jwt, err := c.computeJWT()
				// We've already done this once, and there is no external component here
				// so this should never fail, but log just in case...
				if err != nil {
					logger.Errorf("unable to mint JWT for snowflake output: %s", err)
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

func (c *restClient) Close() {
	c.authRefreshLoop.Stop()
}

func (c *restClient) computeJWT() (string, error) {
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

func (c *restClient) ConfigureClient(ctx context.Context, req clientConfigureRequest) (resp clientConfigureResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("https://%s.snowflakecomputing.com/v1/streaming/client/configure?requestId=%s", c.account, requestID), req, &resp)
	return
}

func (c *restClient) ChannelStatus(ctx context.Context, req batchChannelStatusRequest) (resp batchChannelStatusResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("https://%s.snowflakecomputing.com/v1/streaming/channels/status?requestId=%s", c.account, requestID), req, &resp)
	return
}

func (c *restClient) OpenChannel(ctx context.Context, req openChannelRequest) (resp openChannelResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("https://%s.snowflakecomputing.com/v1/streaming/channels/open?requestId=%s", c.account, requestID), req, &resp)
	return
}

func (c *restClient) DropChannel(ctx context.Context, req dropChannelRequest) (resp dropChannelResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("https://%s.snowflakecomputing.com/v1/streaming/channels/drop?requestId=%s", c.account, requestID), req, &resp)
	return
}

func (c *restClient) RegisterBlob(ctx context.Context, req registerBlobRequest) (resp registerBlobResponse, err error) {
	requestID := uuid.NewString()
	err = c.doPost(ctx, fmt.Sprintf("https://%s.snowflakecomputing.com/v1/streaming/channels/write/blobs?requestId=%s", c.account, requestID), req, &resp)
	return
}

const debugAPICalls = true

func (c *restClient) doPost(ctx context.Context, url string, req any, resp any) error {
	marshaller := json.Marshal
	if debugAPICalls {
		marshaller = func(v any) ([]byte, error) {
			return json.MarshalIndent(v, "", "  ")
		}
	}
	reqBody, err := marshaller(req)
	if err != nil {
		return err
	}
	respBody, err := backoff.RetryNotifyWithData(func() ([]byte, error) {
		if debugAPICalls {
			fmt.Printf("making request to %s with body %s\n", url, reqBody)
		}
		httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
		if errors.Is(err, context.Canceled) {
			return nil, backoff.Permanent(err)
		} else if err != nil {
			return nil, fmt.Errorf("unable to make http request: %w", err)
		}
		httpReq.Header.Add("Content-Type", "application/json")
		httpReq.Header.Add("Accept", "application/json")
		httpReq.Header.Add("User-Agent", c.userAgent)
		jwt := c.cachedJWT.Load()
		httpReq.Header.Add("Authorization", "Bearer "+jwt)
		httpReq.Header.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
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
			return nil, fmt.Errorf("non successful status code (%d): %s", r.StatusCode, reqBody)
		}
		if debugAPICalls {
			fmt.Printf("got response to %s with body %s\n", url, respBody)
		}
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
			c.logger.Debugf("failed request at %s: %s", url, err)
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
