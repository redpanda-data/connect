// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

// StreamMessageChanges represents the changes in a single message
// Single message can have multiple changes
type StreamMessageChanges struct {
	Operation             string   `json:"operation"`
	Schema                string   `json:"schema"`
	Table                 string   `json:"table"`
	TableSnapshotProgress *float64 `json:"table_snapshot_progress,omitempty"`
	// For deleted messages - there will be old changes if replica identity set to full or empty changes
	Data map[string]any `json:"data"`
}

// StreamMessageMetrics represents the metrics of a stream. Passed to each message
type StreamMessageMetrics struct {
	WALLagBytes *int64 `json:"wal_lag_bytes"`
	IsStreaming bool   `json:"is_streaming"`
}

// StreamMode represents the mode of the stream at the time of the message
type StreamMode string

const (
	// StreamModeStreaming indicates that the stream is in streaming mode
	StreamModeStreaming StreamMode = "streaming"
	// StreamModeSnapshot indicates that the stream is in snapshot mode
	StreamModeSnapshot StreamMode = "snapshot"
)

// StreamMessage represents a single message after it has been decoded by the plugin
type StreamMessage struct {
	Lsn         *string                `json:"lsn"`
	Changes     []StreamMessageChanges `json:"changes"`
	Mode        StreamMode             `json:"mode"`
	WALLagBytes *int64                 `json:"wal_lag_bytes,omitempty"`
}
