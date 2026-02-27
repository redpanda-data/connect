// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Field constants for configuration
const (
	OciFieldLogMiner = "logminer"

	FieldSCNWindowSize   = "scn_window_size"
	FieldBackoffInterval = "backoff_interval"
	FieldMiningStrategy  = "strategy"

	// Default values
	defaultSCNWindowSize   = 20000
	defaultBackoffInterval = 5 * time.Second
	defaultMiningStrategy  = "online_catalog"
)

// Config holds configuration for LogMiner
type Config struct {
	SCNWindowSize         int
	MiningBackoffInterval time.Duration
	MiningStrategy        MiningStrategy
}

// NewConfigFields provides the configurations specific to Logminer.
func NewConfigFields() *service.ConfigField {
	return service.NewObjectField(OciFieldLogMiner,
		service.NewIntField(FieldSCNWindowSize).
			Description("The SCN range to mine per cycle. Each cycle reads changes between the current SCN and current SCN + scn_window_size. Smaller values mean more frequent queries with lower memory usage but higher overhead; larger values reduce query frequency and improve throughput at the cost of higher memory usage per cycle.").
			Default(defaultSCNWindowSize),
		service.NewDurationField(FieldBackoffInterval).
			Description("The interval between attempts to check for new changes once all data is processed. For low traffic tables increasing this value can reduce network traffic to the server.").
			Default(defaultBackoffInterval.String()).
			Example("5s").Example("1m"),
		service.NewStringField(FieldMiningStrategy).
			Description("Controls how LogMiner retrieves data dictionary information. `online_catalog` (default) uses the current data dictionary for best performance but cannot capture DDL changes. `online_catalog` currently only supported.").
			Default(defaultMiningStrategy),
	).Description("LogMiner configuration settings.").Optional()
}

// MiningStrategy defines how LogMiner accesses dictionary information
type MiningStrategy string

const (
	// OnlineCatalogStrategy uses the online catalog for dictionary lookups (default, recommended)
	OnlineCatalogStrategy MiningStrategy = "online_catalog"
)

// NewDefaultConfig returns a Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		SCNWindowSize:         defaultSCNWindowSize,
		MiningBackoffInterval: defaultBackoffInterval,
		MiningStrategy:        MiningStrategy(defaultMiningStrategy),
	}
}
