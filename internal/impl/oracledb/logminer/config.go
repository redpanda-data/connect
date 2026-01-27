// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import "time"

// Field constants for configuration
const (
	FieldMaxBatchSize    = "max_batch_size"
	FieldBackoffInterval = "backoff_interval"
	FieldMiningStrategy  = "strategy"
)

// Default values
const (
	DefaultMaxBatchSize    = 500
	DefaultBackoffInterval = 5 * time.Second
	DefaultMiningStrategy  = "online_catalog"
)

// Config holds configuration for LogMiner
type Config struct {
	MaxBatchSize          int
	MiningBackoffInterval time.Duration
	MiningStrategy        MiningStrategy
}

// MiningStrategy defines how LogMiner accesses dictionary information
type MiningStrategy string

const (
	// OnlineCatalogStrategy uses the online catalog for dictionary lookups (default, recommended)
	OnlineCatalogStrategy MiningStrategy = "online_catalog"
	// RedoLogsStrategy extracts dictionary from redo logs
	RedoLogsStrategy MiningStrategy = "redo_logs"
)

// NewDefaultConfig returns a Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		MaxBatchSize:          DefaultMaxBatchSize,
		MiningBackoffInterval: DefaultBackoffInterval,
		MiningStrategy:        MiningStrategy(DefaultMiningStrategy),
	}
}
