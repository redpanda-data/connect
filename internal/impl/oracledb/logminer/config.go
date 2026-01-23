package logminer

import "time"

// Connect config primatives
const (
	FieldMaxBatchSize    = "max_batch_size"
	FieldBackoffInterval = "backoff_interval"
	FieldMiningStrategy  = "mining_strategy"

	DefaultMaxBatchSize    = 500
	DefaultBackoffInterval = 5 * time.Second
	DefaultMiningStrategy  = "online_catalog"

	// log mining strategies
	OnlineCatalogStrategy MiningStrategy = DefaultMiningStrategy
)

type MiningStrategy string

type Config struct {
	MaxBatchSize          int
	MiningBackoffInterval time.Duration
	MiningStrategy        MiningStrategy
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxBatchSize:          DefaultMaxBatchSize,
		MiningBackoffInterval: DefaultBackoffInterval,
		MiningStrategy:        OnlineCatalogStrategy,
	}
}
