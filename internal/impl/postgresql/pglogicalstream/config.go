// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Config is the configuration for the pglogicalstream plugin
type Config struct {
	// DBConfig is the configuration to connect to the database with
	DBConfig *pgconn.Config
	DBRawDSN string
	// The DB schema to lookup tables in
	DBSchema string
	// DbTables is the tables to stream changes from
	DBTables []string
	// ReplicationSlotName is the name of the replication slot to use
	//
	// MUST BE SQL INJECTION FREE
	ReplicationSlotName string
	// TemporaryReplicationSlot is whether to use a temporary replication slot
	TemporaryReplicationSlot bool
	// StreamOldData is whether to stream all existing data
	StreamOldData bool
	// SnapshotMemorySafetyFactor is the memory safety factor for streaming snapshot
	SnapshotMemorySafetyFactor float64
	// BatchSize is the batch size for streaming
	BatchSize int
	// BatchTransactions is whether to buffer transactions as an entire single message or to send
	// each row in a transaction as a message.
	BatchTransactions bool

	Logger *service.Logger

	PgStandbyTimeout          time.Duration
	WalMonitorInterval        time.Duration
	MaxParallelSnapshotTables int
}
