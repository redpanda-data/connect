package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisListConfig contains configuration fields for the RedisList output type.
type RedisListConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Key           string        `json:"key" yaml:"key"`
	MaxInFlight   int           `json:"max_in_flight" yaml:"max_in_flight"`
	Batching      policy.Config `json:"batching" yaml:"batching"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		Config:      bredis.NewConfig(),
		Key:         "",
		MaxInFlight: 64,
		Batching:    policy.NewConfig(),
	}
}
