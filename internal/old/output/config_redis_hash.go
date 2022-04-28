package output

import (
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisHashConfig contains configuration fields for the RedisHash output type.
type RedisHashConfig struct {
	bredis.Config  `json:",inline" yaml:",inline"`
	Key            string            `json:"key" yaml:"key"`
	WalkMetadata   bool              `json:"walk_metadata" yaml:"walk_metadata"`
	WalkJSONObject bool              `json:"walk_json_object" yaml:"walk_json_object"`
	Fields         map[string]string `json:"fields" yaml:"fields"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewRedisHashConfig creates a new RedisHashConfig with default values.
func NewRedisHashConfig() RedisHashConfig {
	return RedisHashConfig{
		Config:         bredis.NewConfig(),
		Key:            "",
		WalkMetadata:   false,
		WalkJSONObject: false,
		Fields:         map[string]string{},
		MaxInFlight:    64,
	}
}
