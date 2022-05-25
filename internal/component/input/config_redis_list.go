package input

import (
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisListConfig contains configuration fields for the RedisList input type.
type RedisListConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Key           string `json:"key" yaml:"key"`
	Timeout       string `json:"timeout" yaml:"timeout"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		Config:  bredis.NewConfig(),
		Key:     "",
		Timeout: "5s",
	}
}
