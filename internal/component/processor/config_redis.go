package processor

import (
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisConfig contains configuration fields for the Redis processor.
type RedisConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Operator      string `json:"operator" yaml:"operator"`
	Key           string `json:"key" yaml:"key"`

	// TODO: V4 replace this
	Retries     int    `json:"retries" yaml:"retries"`
	RetryPeriod string `json:"retry_period" yaml:"retry_period"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		Config:      bredis.NewConfig(),
		Operator:    "",
		Key:         "",
		Retries:     3,
		RetryPeriod: "500ms",
	}
}
