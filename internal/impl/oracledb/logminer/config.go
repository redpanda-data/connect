package logminer

import "time"

// Field constants for configuration
const (
	FieldMaxBatchSize     = "max_batch_size"
	FieldBackoffInterval  = "backoff_interval"
	FieldMiningStrategy   = "strategy"
)

// Default values
const (
	DefaultMaxBatchSize     = 500
	DefaultBackoffInterval  = 5 * time.Second
	DefaultMiningStrategy   = "online_catalog"
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
