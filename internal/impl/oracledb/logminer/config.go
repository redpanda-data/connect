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
)

// MiningStrategy defines how LogMiner accesses dictionary information
type MiningStrategy string

const (
	// OnlineCatalogStrategy uses the online catalog for dictionary lookups (default, recommended)
	OnlineCatalogStrategy MiningStrategy = "online_catalog"
)

// Config holds configuration for LogMiner
type Config struct {
	SCNWindowSize         int
	MiningBackoffInterval time.Duration
	MiningInterval        time.Duration
	MiningStrategy        MiningStrategy
	MaxTransactionEvents  int
}

// NewDefaultConfig returns a Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		SCNWindowSize:         DefaultSCNWindowSize,
		MiningBackoffInterval: DefaultMiningBackoffInterval,
		MiningInterval:        DefaultMiningInterval,
		MiningStrategy:        MiningStrategy(DefaultMiningStrategy),
		MaxTransactionEvents:  DefaultMaxTransactionEvents,
	}
}
