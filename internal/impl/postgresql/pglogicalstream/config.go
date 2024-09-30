// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

type TlsVerify string

const TlsNoVerify TlsVerify = "none"
const TlsRequireVerify TlsVerify = "require"

type Config struct {
	DbHost                     string    `yaml:"db_host"`
	DbPassword                 string    `yaml:"db_password"`
	DbUser                     string    `yaml:"db_user"`
	DbPort                     int       `yaml:"db_port"`
	DbName                     string    `yaml:"db_name"`
	DbSchema                   string    `yaml:"db_schema"`
	DbTables                   []string  `yaml:"db_tables"`
	ReplicationSlotName        string    `yaml:"replication_slot_name"`
	TlsVerify                  TlsVerify `yaml:"tls_verify"`
	StreamOldData              bool      `yaml:"stream_old_data"`
	SeparateChanges            bool      `yaml:"separate_changes"`
	SnapshotMemorySafetyFactor float64   `yaml:"snapshot_memory_safety_factor"`
	BatchSize                  int       `yaml:"batch_size"`
}
