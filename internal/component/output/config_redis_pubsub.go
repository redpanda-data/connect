package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisPubSubConfig contains configuration fields for the RedisPubSub output
// type.
type RedisPubSubConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Channel       string             `json:"channel" yaml:"channel"`
	MaxInFlight   int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching      batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		Config:      bredis.NewConfig(),
		Channel:     "",
		MaxInFlight: 64,
		Batching:    batchconfig.NewConfig(),
	}
}
