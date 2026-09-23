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
)

var (
	// DefaultSCNWindowSize sets the window size used between SCNs in LogMiner.
	DefaultSCNWindowSize = 20000
	// DefaultMinSCNWindowSize is the minimum SCN gap required before starting a new LogMiner
	// session.
	DefaultMinSCNWindowSize = 1000
	// DefaultMaxSCNWindowSize is the maximum SCN range that can be mined in a single cycle.
	// The adaptive window grows toward this ceiling during backlog and shrinks during steady state.
	DefaultMaxSCNWindowSize = 100000
	// DefaultMiningBackoffInterval controls the mining cycle backoff interval.
	DefaultMiningBackoffInterval = 5 * time.Second
	// DefaultMiningInterval controls the interval between mining cycles during normal operation.
	DefaultMiningInterval = 300 * time.Millisecond
	// DefaultMiningStrategy determines LogMiner's default mining strategy.
	DefaultMiningStrategy = "online_catalog"
	// DefaultMaxTransactionEvents controls the maximu number of events that can be buffered
	// per transaction before they're discarded.
	// Used to prevent large events resulting in memory exhaustion.
	DefaultMaxTransactionEvents = 0
	// DefaultLOBEnabled controls whether LOB column processing is enabled.
	DefaultLOBEnabled = true
	// DefaultTransactionCacheKey is the default prefix used for the transaction buffer cache key.
	// Only relevant when configuring a (potentially shared) cache_resource transaction buffer.
	DefaultTransactionCacheKey = "oracledb_cdc"
	// DefaultMaxSessionAge controls how long a single LogMiner session may remain active
	// before being forcibly restarted, independent of redo log switches. 0 disables this,
	// restarting only on log switches (the previous, and still default, behaviour).
	DefaultMaxSessionAge = 0 * time.Second
	// DefaultLogCountMin is the minimum number of redo log files mined per cycle,
	// per redo thread, under the WindowStrategyLogCount window strategy. This is
	// applied internally as file-size-equivalent bytes (this many multiples of
	// the online redo log's configured size), not a literal file count - see
	// logFileSelector.
	DefaultLogCountMin = 2
	// DefaultLogCountGrowthMax is the ceiling the per-thread log file budget can
	// grow to under the WindowStrategyLogCount window strategy, once forward
	// progress stalls. Like DefaultLogCountMin, this is a file-size-equivalent
	// byte multiplier internally, not a literal file count.
	DefaultLogCountGrowthMax = 4
)

// WindowStrategy selects how the SCN range mined per LogMiner cycle is sized.
type WindowStrategy string

const (
	// WindowStrategySCNWindow sizes the mined range by growing/shrinking a fixed
	// SCN-count window each cycle.
	WindowStrategySCNWindow WindowStrategy = "scn_window"
	// WindowStrategyLogCount sizes the mined range by a bounded number of redo
	// log files per cycle, per redo thread.
	WindowStrategyLogCount WindowStrategy = "log_count"
)

// MiningStrategy defines how LogMiner accesses dictionary information
type MiningStrategy string

const (
	// OnlineCatalogStrategy uses the online catalog for dictionary lookups (default, recommended)
	OnlineCatalogStrategy MiningStrategy = "online_catalog"
)

// TransactionCacheConfig contains config specific to service.Cache implementations (ie cache_resources)
type TransactionCacheConfig struct {
	CacheName string
	CacheKey  string
	MaxEvents int
}

// Config holds configuration for LogMiner
type Config struct {
	SCNWindowSize          int
	MinSCNWindowSize       int
	MaxSCNWindowSize       int
	MiningBackoffInterval  time.Duration
	MiningInterval         time.Duration
	MiningStrategy         MiningStrategy
	MaxTransactionEvents   int
	LOBEnabled             bool
	PDBName                string
	TransactionCacheConfig TransactionCacheConfig
	MaxSessionAge          time.Duration
	WindowStrategy         WindowStrategy
	LogCountMin            int
	LogCountGrowthMax      int
}

// NewDefaultConfig returns a Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		SCNWindowSize:         DefaultSCNWindowSize,
		MinSCNWindowSize:      DefaultMinSCNWindowSize,
		MaxSCNWindowSize:      DefaultMaxSCNWindowSize,
		MiningBackoffInterval: DefaultMiningBackoffInterval,
		MiningInterval:        DefaultMiningInterval,
		MiningStrategy:        MiningStrategy(DefaultMiningStrategy),
		MaxTransactionEvents:  DefaultMaxTransactionEvents,
		LOBEnabled:            DefaultLOBEnabled,
		MaxSessionAge:         DefaultMaxSessionAge,
		WindowStrategy:        WindowStrategySCNWindow,
		LogCountMin:           DefaultLogCountMin,
		LogCountGrowthMax:     DefaultLogCountGrowthMax,
	}
}
