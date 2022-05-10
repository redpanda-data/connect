package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// RedisStreamsConfig contains configuration fields for the RedisStreams output type.
type RedisStreamsConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Stream        string                       `json:"stream" yaml:"stream"`
	BodyKey       string                       `json:"body_key" yaml:"body_key"`
	MaxLenApprox  int64                        `json:"max_length" yaml:"max_length"`
	MaxInFlight   int                          `json:"max_in_flight" yaml:"max_in_flight"`
	Metadata      metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	Batching      batchconfig.Config           `json:"batching" yaml:"batching"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	return RedisStreamsConfig{
		Config:       bredis.NewConfig(),
		Stream:       "",
		BodyKey:      "body",
		MaxLenApprox: 0,
		MaxInFlight:  64,
		Metadata:     metadata.NewExcludeFilterConfig(),
		Batching:     batchconfig.NewConfig(),
	}
}
