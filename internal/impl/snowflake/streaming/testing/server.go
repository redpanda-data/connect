/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package testing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
)

const (
	responseSuccess                   = 0
	responseTableNotExist             = 4
	responseErrQueueFull              = 7
	responseErrRetryRequest           = 10
	responseErrInvalidClientSequencer = 20
	responseErrTransientError         = 35
	responseErrMissingColumnStats     = 40
)

// MockSnowflakeServer is a mock HTTP server that implements the Snowflake Streaming API
type MockSnowflakeServer struct {
	Server  *httptest.Server
	State   *ServerState
	fakeGCS *FakeGCSContainer
}

// NewMockSnowflakeServer creates a new mock Snowflake server with fake-gcs-server
func NewMockSnowflakeServer(fakeGCS *FakeGCSContainer) *MockSnowflakeServer {
	state := NewServerState()
	state.SetGCSConfig(
		fakeGCS.Bucket(),
		fakeGCS.PathPrefix(),
	)

	mock := &MockSnowflakeServer{
		State:   state,
		fakeGCS: fakeGCS,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streaming/client/configure", mock.handleConfigureClient)
	mux.HandleFunc("/v1/streaming/channels/status", mock.handleChannelStatus)
	mux.HandleFunc("/v1/streaming/channels/open", mock.handleOpenChannel)
	mux.HandleFunc("/v1/streaming/channels/drop", mock.handleDropChannel)
	mux.HandleFunc("/v1/streaming/channels/write/blobs", mock.handleRegisterBlob)
	mux.HandleFunc("/api/v2/statements", mock.handleRunSQL)

	mock.Server = httptest.NewServer(mux)
	return mock
}

// Close closes the mock server
func (m *MockSnowflakeServer) Close() {
	m.Server.Close()
}

// URL returns the server URL
func (m *MockSnowflakeServer) URL() string {
	return m.Server.URL
}

type clientConfigureRequest struct {
	Role     string `json:"role"`
	FileName string `json:"file_name,omitempty"`
}

type fileLocationInfo struct {
	LocationType          string            `json:"locationType"`
	Location              string            `json:"location"`
	Path                  string            `json:"path"`
	Creds                 map[string]string `json:"creds"`
	Region                string            `json:"region,omitempty"`
	EndPoint              string            `json:"endPoint,omitempty"`
	StorageAccount        string            `json:"storageAccount,omitempty"`
	PresignedURL          string            `json:"presignedUrl,omitempty"`
	IsClientSideEncrypted bool              `json:"isClientSideEncrypted"`
	UseS3RegionalURL      bool              `json:"useS3RegionalURL"`
	VolumeHash            string            `json:"volumeHash,omitempty"`
}

type clientConfigureResponse struct {
	Prefix        string           `json:"prefix"`
	StatusCode    int64            `json:"status_code"`
	Message       string           `json:"message"`
	StageLocation fileLocationInfo `json:"stage_location"`
	DeploymentID  int64            `json:"deployment_id"`
}

func (m *MockSnowflakeServer) handleConfigureClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req clientConfigureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	bucket, pathPrefix := m.State.GetGCSConfig()

	// For GCS, we provide a dummy access token since STORAGE_EMULATOR_HOST is set
	// The GCS SDK will automatically use the emulator
	resp := clientConfigureResponse{
		Prefix:     m.State.GetClientPrefix(),
		StatusCode: responseSuccess,
		Message:    "",
		StageLocation: fileLocationInfo{
			LocationType:          "GCS",
			Location:              bucket + "/" + pathPrefix,
			Path:                  pathPrefix,
			IsClientSideEncrypted: true,
			Creds: map[string]string{
				"GCS_ACCESS_TOKEN": "fake-token-for-testing",
			},
		},
		DeploymentID: m.State.GetDeploymentID(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type channelStatusRequest struct {
	Table           string `json:"table"`
	Database        string `json:"database"`
	Schema          string `json:"schema"`
	Name            string `json:"channel_name"`
	ClientSequencer *int64 `json:"client_sequencer,omitempty"`
}

type batchChannelStatusRequest struct {
	Role     string                 `json:"role"`
	Channels []channelStatusRequest `json:"channels"`
}

type channelStatusResponse struct {
	StatusCode               int64  `json:"status_code"`
	PersistedOffsetToken     string `json:"persisted_offset_token"`
	PersistedClientSequencer int64  `json:"persisted_client_sequencer"`
	PersistedRowSequencer    int64  `json:"persisted_row_sequencer"`
}

type batchChannelStatusResponse struct {
	StatusCode int64                   `json:"status_code"`
	Message    string                  `json:"message"`
	Channels   []channelStatusResponse `json:"channels"`
}

func (m *MockSnowflakeServer) handleChannelStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req batchChannelStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	channels := make([]channelStatusResponse, 0, len(req.Channels))
	for _, chReq := range req.Channels {
		ch, exists := m.State.GetChannel(chReq.Database, chReq.Schema, chReq.Table, chReq.Name)
		if !exists {
			channels = append(channels, channelStatusResponse{
				StatusCode:               responseTableNotExist,
				PersistedOffsetToken:     "",
				PersistedClientSequencer: 0,
				PersistedRowSequencer:    0,
			})
		} else {
			channels = append(channels, channelStatusResponse{
				StatusCode:               responseSuccess,
				PersistedOffsetToken:     ch.PersistedOffsetToken,
				PersistedClientSequencer: ch.ClientSequencer,
				PersistedRowSequencer:    ch.RowSequencer,
			})
		}
	}

	resp := batchChannelStatusResponse{
		StatusCode: responseSuccess,
		Message:    "",
		Channels:   channels,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type openChannelRequest struct {
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

type columnMetadata struct {
	Name                  string  `json:"name"`
	Type                  string  `json:"type"`
	LogicalType           string  `json:"logical_type"`
	PhysicalType          string  `json:"physical_type"`
	Precision             *int32  `json:"precision"`
	Scale                 *int32  `json:"scale"`
	ByteLength            *int32  `json:"byte_length"`
	Length                *int32  `json:"length"`
	Nullable              bool    `json:"nullable"`
	Collation             *string `json:"collation"`
	SourceIcebergDataType *string `json:"source_iceberg_data_type"`
	Ordinal               int32   `json:"ordinal"`
}

type offsetToken struct {
	Token string `json:"token"`
}

type openChannelResponse struct {
	StatusCode          int64            `json:"status_code"`
	Message             string           `json:"message"`
	Database            string           `json:"database"`
	Schema              string           `json:"schema"`
	Table               string           `json:"table"`
	Channel             string           `json:"channel"`
	ClientSequencer     int64            `json:"client_sequencer"`
	RowSequencer        int64            `json:"row_sequencer"`
	OffsetToken         *offsetToken     `json:"offset_token,omitempty"`
	TableColumns        []columnMetadata `json:"table_columns"`
	EncryptionKey       string           `json:"encryption_key"`
	EncryptionKeyID     int64            `json:"encryption_key_id"`
	IcebergLocationInfo fileLocationInfo `json:"iceberg_location"`
}

func (m *MockSnowflakeServer) handleOpenChannel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req openChannelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ch := m.State.OpenChannel(req.Database, req.Schema, req.Table, req.Channel)

	// Generate mock table columns - these are generic columns that work for most tests
	tableColumns := []columnMetadata{
		{
			Name:         "A",
			Type:         "TEXT",
			LogicalType:  "TEXT",
			PhysicalType: "SB16",
			Nullable:     true,
			Ordinal:      0,
		},
		{
			Name:         "B",
			Type:         "BOOLEAN",
			LogicalType:  "BOOLEAN",
			PhysicalType: "SB1",
			Nullable:     true,
			Ordinal:      1,
		},
		{
			Name:         "C",
			Type:         "VARIANT",
			LogicalType:  "VARIANT",
			PhysicalType: "LOB",
			Nullable:     true,
			Ordinal:      2,
		},
		{
			Name:         "D",
			Type:         "ARRAY",
			LogicalType:  "ARRAY",
			PhysicalType: "LOB",
			Nullable:     true,
			Ordinal:      3,
		},
		{
			Name:         "E",
			Type:         "OBJECT",
			LogicalType:  "OBJECT",
			PhysicalType: "LOB",
			Nullable:     true,
			Ordinal:      4,
		},
		{
			Name:         "F",
			Type:         "REAL",
			LogicalType:  "REAL",
			PhysicalType: "SB8",
			Nullable:     true,
			Ordinal:      5,
		},
		{
			Name:         "G",
			Type:         "FIXED",
			LogicalType:  "FIXED",
			PhysicalType: "SB16",
			Precision:    ptr[int32](38),
			Scale:        ptr[int32](0),
			Nullable:     true,
			Ordinal:      6,
		},
	}

	// Check if we need a custom table schema based on table name
	if strings.Contains(req.Table, "INT_TABLE") {
		tableColumns = []columnMetadata{
			{
				Name:         "A",
				Type:         "FIXED",
				LogicalType:  "FIXED",
				PhysicalType: "SB16",
				Precision:    ptr[int32](38),
				Scale:        ptr[int32](0),
				Nullable:     true,
				Ordinal:      0,
			},
			{
				Name:         "B",
				Type:         "FIXED",
				LogicalType:  "FIXED",
				PhysicalType: "SB16",
				Precision:    ptr[int32](38),
				Scale:        ptr[int32](8),
				Nullable:     true,
				Ordinal:      1,
			},
			{
				Name:         "C",
				Type:         "FIXED",
				LogicalType:  "FIXED",
				PhysicalType: "SB16",
				Precision:    ptr[int32](18),
				Scale:        ptr[int32](0),
				Nullable:     true,
				Ordinal:      2,
			},
			{
				Name:         "D",
				Type:         "FIXED",
				LogicalType:  "FIXED",
				PhysicalType: "SB16",
				Precision:    ptr[int32](28),
				Scale:        ptr[int32](8),
				Nullable:     true,
				Ordinal:      3,
			},
		}
	} else if strings.Contains(req.Table, "CHANNEL_TABLE") || strings.Contains(req.Table, "OFFSET_TOKEN_TABLE") {
		tableColumns = []columnMetadata{
			{
				Name:         "A",
				Type:         "FIXED",
				LogicalType:  "FIXED",
				PhysicalType: "SB16",
				Precision:    ptr[int32](38),
				Scale:        ptr[int32](0),
				Nullable:     true,
				Ordinal:      0,
			},
		}
	}

	var offsetTok *offsetToken
	if ch.PersistedOffsetToken != "" {
		offsetTok = &offsetToken{Token: ch.PersistedOffsetToken}
	}

	resp := openChannelResponse{
		StatusCode:      responseSuccess,
		Message:         "",
		Database:        req.Database,
		Schema:          req.Schema,
		Table:           req.Table,
		Channel:         req.Channel,
		ClientSequencer: ch.ClientSequencer,
		RowSequencer:    ch.RowSequencer,
		OffsetToken:     offsetTok,
		TableColumns:    tableColumns,
		EncryptionKey:   ch.EncryptionKey,
		EncryptionKeyID: ch.EncryptionKeyID,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type dropChannelRequest struct {
	RequestID       string `json:"request_id"`
	Role            string `json:"role"`
	Channel         string `json:"channel"`
	Table           string `json:"table"`
	Database        string `json:"database"`
	Schema          string `json:"schema"`
	IsIceberg       bool   `json:"is_iceberg"`
	ClientSequencer *int64 `json:"client_sequencer,omitempty"`
}

type dropChannelResponse struct {
	StatusCode int64  `json:"status_code"`
	Message    string `json:"message"`
	Database   string `json:"database"`
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Channel    string `json:"channel"`
}

func (m *MockSnowflakeServer) handleDropChannel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req dropChannelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m.State.DropChannel(req.Database, req.Schema, req.Table, req.Channel)

	resp := dropChannelResponse{
		StatusCode: responseSuccess,
		Message:    "",
		Database:   req.Database,
		Schema:     req.Schema,
		Table:      req.Table,
		Channel:    req.Channel,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type channelMetadata struct {
	Channel          string       `json:"channel_name"`
	ClientSequencer  int64        `json:"client_sequencer"`
	RowSequencer     int64        `json:"row_sequencer"`
	StartOffsetToken *offsetToken `json:"start_offset_token,omitempty"`
	EndOffsetToken   *offsetToken `json:"end_offset_token,omitempty"`
	OffsetToken      *offsetToken `json:"offset_token,omitempty"`
}

type chunkMetadata struct {
	Database                string            `json:"database"`
	Schema                  string            `json:"schema"`
	Table                   string            `json:"table"`
	ChunkStartOffset        int64             `json:"chunk_start_offset"`
	ChunkLength             int32             `json:"chunk_length"`
	ChunkLengthUncompressed int32             `json:"chunk_length_uncompressed"`
	Channels                []channelMetadata `json:"channels"`
	ChunkMD5                string            `json:"chunk_md5"`
	EncryptionKeyID         int64             `json:"encryption_key_id,omitempty"`
	FirstInsertTimeInMillis int64             `json:"first_insert_time_in_ms"`
	LastInsertTimeInMillis  int64             `json:"last_insert_time_in_ms"`
}

type blobMetadata struct {
	Path             string          `json:"path"`
	MD5              string          `json:"md5"`
	Chunks           []chunkMetadata `json:"chunks"`
	BDECVersion      int8            `json:"bdec_version"`
	SpansMixedTables bool            `json:"spans_mixed_tables"`
}

type registerBlobRequest struct {
	RequestID string         `json:"request_id"`
	Role      string         `json:"role"`
	Blobs     []blobMetadata `json:"blobs"`
	IsIceberg bool           `json:"is_iceberg"`
}

type channelRegisterStatus struct {
	StatusCode      int64  `json:"status_code"`
	Message         string `json:"message"`
	Channel         string `json:"channel"`
	ClientSequencer int64  `json:"client_sequencer"`
}

type chunkRegisterStatus struct {
	Channels []channelRegisterStatus `json:"channels"`
	Database string                  `json:"database"`
	Schema   string                  `json:"schema"`
	Table    string                  `json:"table"`
}

type blobRegisterStatus struct {
	Chunks []chunkRegisterStatus `json:"chunks"`
}

type registerBlobResponse struct {
	StatusCode int64                `json:"status_code"`
	Message    string               `json:"message"`
	Blobs      []blobRegisterStatus `json:"blobs"`
}

func (m *MockSnowflakeServer) handleRegisterBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req registerBlobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	blobs := make([]blobRegisterStatus, 0, len(req.Blobs))
	for _, blob := range req.Blobs {
		m.State.RegisterBlob(blob.Path)

		chunks := make([]chunkRegisterStatus, 0, len(blob.Chunks))
		for _, chunk := range blob.Chunks {
			channels := make([]channelRegisterStatus, 0, len(chunk.Channels))
			for _, channel := range chunk.Channels {
				// Update channel state with the new offset token
				offsetToken := ""
				if channel.EndOffsetToken != nil {
					offsetToken = channel.EndOffsetToken.Token
				} else if channel.OffsetToken != nil {
					offsetToken = channel.OffsetToken.Token
				}

				m.State.UpdateChannelOffset(
					chunk.Database,
					chunk.Schema,
					chunk.Table,
					channel.Channel,
					offsetToken,
					channel.ClientSequencer,
					channel.RowSequencer,
				)

				channels = append(channels, channelRegisterStatus{
					StatusCode:      responseSuccess,
					Message:         "",
					Channel:         channel.Channel,
					ClientSequencer: channel.ClientSequencer,
				})
			}

			chunks = append(chunks, chunkRegisterStatus{
				Channels: channels,
				Database: chunk.Database,
				Schema:   chunk.Schema,
				Table:    chunk.Table,
			})
		}

		blobs = append(blobs, blobRegisterStatus{
			Chunks: chunks,
		})
	}

	resp := registerBlobResponse{
		StatusCode: responseSuccess,
		Message:    "",
		Blobs:      blobs,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (*MockSnowflakeServer) handleRunSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Return 500 error as requested
	w.WriteHeader(http.StatusInternalServerError)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":    "500",
		"message": "SQL execution not supported in mock server",
	})
}

func ptr[T any](v T) *T {
	return &v
}
