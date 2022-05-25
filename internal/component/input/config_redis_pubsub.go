package input

import (
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisPubSubConfig contains configuration fields for the RedisPubSub input
// type.
type RedisPubSubConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Channels      []string `json:"channels" yaml:"channels"`
	UsePatterns   bool     `json:"use_patterns" yaml:"use_patterns"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		Config:      bredis.NewConfig(),
		Channels:    []string{},
		UsePatterns: false,
	}
}
