// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "github.com/redpanda-data/benthos/v4/public/service"

type Config struct {
	DbHost             string    `yaml:"db_host"`
	DbPassword         string    `yaml:"db_password"`
	DbUser             string    `yaml:"db_user"`
	DbPort             int       `yaml:"db_port"`
	DbName             string    `yaml:"db_name"`
	DbSchema           string    `yaml:"db_schema"`
	DbTables           []string  `yaml:"db_tables"`
	TlsVerify          TlsVerify `yaml:"tls_verify"`
	PgConnRuntimeParam string    `yaml:"pg_conn_options"`

	ReplicationSlotName        string  `yaml:"replication_slot_name"`
	StreamOldData              bool    `yaml:"stream_old_data"`
	SeparateChanges            bool    `yaml:"separate_changes"`
	SnapshotMemorySafetyFactor float64 `yaml:"snapshot_memory_safety_factor"`
	DecodingPlugin             string  `yaml:"decoding_plugin"`
	BatchSize                  int     `yaml:"batch_size"`

	logger *service.Logger
}
