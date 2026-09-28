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
	// DefaultRedoVolumeMin is how much redo one mining cycle reads per redo
	// thread under WindowStrategyRedoVolume, when nothing else applies.
	//
	// The unit is the size of the largest online redo log group, read once at
	// start with MAX(BYTES) over V$LOG. Groups are normally all the same size,
	// so this is usually just the redo log size. For each thread, log files
	// are added in sequence order until their total size reaches this many
	// units. The file that crosses the limit is kept, so one very large file
	// is still selected.
	//
	// After a cycle that reads everything available, the budget goes back to
	// this value.
	DefaultRedoVolumeMin = 2
	// DefaultRedoVolumeGrowthMax is the ceiling the per-thread redo volume
	// budget can grow to under the WindowStrategyRedoVolume window strategy,
	// once forward progress stalls.
	DefaultRedoVolumeGrowthMax = 4
	// MinRedoVolumeGrowthCeiling is the smallest growth ceiling that avoids a
	// permanent stall: a budget of 1 file always reselects its own single
	// file forever, since that file's own boundary re-qualifies it next
	// cycle, so growth is its only way to make progress.
	MinRedoVolumeGrowthCeiling = 2
	// redoVolumeStallWarnThreshold is how many consecutive stalled cycles
	// (see logFileSelector.consecutiveStalls) trigger a warning log. This is
	// proven unreachable for every known failure mode, so reaching it is a
	// signal to investigate, not routine backoff - the threshold is only
	// above 1 to give a single incidental stall room without logging.
	redoVolumeStallWarnThreshold = 3
)

// WindowStrategy selects how the SCN range mined per LogMiner cycle is sized.
type WindowStrategy string

const (
	// WindowStrategySCNWindow sizes the mined range by growing/shrinking a fixed
	// SCN-count window each cycle.
	WindowStrategySCNWindow WindowStrategy = "scn_window"
	// WindowStrategyRedoVolume sizes the mined range by a bounded redo volume
	// budget per cycle, per redo thread.
	WindowStrategyRedoVolume WindowStrategy = "redo_volume"
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
	RedoVolumeMin          int
	RedoVolumeGrowthMax    int
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
		RedoVolumeMin:         DefaultRedoVolumeMin,
		RedoVolumeGrowthMax:   DefaultRedoVolumeGrowthMax,
	}
}
