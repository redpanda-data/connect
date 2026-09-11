// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Config is the configuration for the pglogicalstream plugin
type Config struct {
	// DBConfig is the configuration to connect to the database with
	DBConfig  *pgconn.Config
	DBRawDSN  string
	TLSConfig *tls.Config
	DBSchema  string
	DBTables  []string
	// Refreshes short lived IAM auth token that is treated as a password
	RefreshAuthToken func(ctx context.Context) error
	// ReplicationSlotName is the name of the replication slot to use
	//
	// MUST BE SQL INJECTION FREE
	ReplicationSlotName string
	// TemporaryReplicationSlot is whether to use a temporary replication slot
	TemporaryReplicationSlot bool
	// StreamOldData is whether to stream all existing data
	StreamOldData bool
	// BatchSize is the number of rows per snapshot page (snapshot_batch_size).
	// It does not affect the streaming path; see StreamBatchMaxRows.
	BatchSize int
	// StreamBatchMaxRows caps the rows the streaming reader accumulates before
	// handing a batch to the consumer. Zero means the package default
	// (streamBatchMaxRows). The input sets it from checkpoint_limit so that at
	// least two streaming batches fit under the checkpoint cap.
	StreamBatchMaxRows int
	// If true, include BEGIN and COMMIT messages in the stream
	IncludeTxnMarkers bool
	// SignalTableName is the name of the signal table. Rows inserted into this
	// table are treated as control signals rather than data, and the table is
	// excluded from snapshot scans.
	SignalTableName string

	Logger *service.Logger

	PgStandbyTimeout   time.Duration
	WalMonitorInterval time.Duration
	MaxSnapshotWorkers int
	// The value to use for unchanged toast columns
	UnchangedToastValue any
	// The interval to send logical messages
	HeartbeatInterval time.Duration
}
