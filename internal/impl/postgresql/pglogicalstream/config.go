// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"crypto/tls"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Config is the configuration for the pglogicalstream plugin
type Config struct {
	// DbHost is the host of the PostgreSQL instance
	DBHost string `yaml:"db_host"`
	// DbPassword is the password for the PostgreSQL instance
	DBPassword string `yaml:"db_password"`
	// DbUser is the user for the PostgreSQL instance
	DBUser string `yaml:"db_user"`
	// DbPort is the port of the PostgreSQL instance
	DBPort int `yaml:"db_port"`
	// DbName is the name of the database to connect to
	DBName string `yaml:"db_name"`
	// DbSchema is the schema to stream changes from
	DBSchema string `yaml:"db_schema"`
	// DbTables is the tables to stream changes from
	DBTables []string `yaml:"db_tables"`
	// TLSConfig is the TLS verification configuration
	TLSConfig *tls.Config `yaml:"tls"`
	// PgConnRuntimeParam is the runtime parameter for the PostgreSQL connection
	PgConnRuntimeParam string `yaml:"pg_conn_options"`

	// ReplicationSlotName is the name of the replication slot to use
	ReplicationSlotName string `yaml:"replication_slot_name"`
	// TemporaryReplicationSlot is whether to use a temporary replication slot
	TemporaryReplicationSlot bool `yaml:"temporary_replication_slot"`
	// StreamOldData is whether to stream all existing data
	StreamOldData bool `yaml:"stream_old_data"`
	// SnapshotMemorySafetyFactor is the memory safety factor for streaming snapshot
	SnapshotMemorySafetyFactor float64 `yaml:"snapshot_memory_safety_factor"`
	// DecodingPlugin is the decoding plugin to use
	DecodingPlugin string `yaml:"decoding_plugin"`
	// BatchSize is the batch size for streaming
	BatchSize int `yaml:"batch_size"`

	// StreamUncommitted is whether to stream uncommitted messages before receiving commit message
	StreamUncomited bool `yaml:"stream_uncommitted"`

	logger *service.Logger
}
