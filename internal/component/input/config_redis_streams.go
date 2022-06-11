package input

import (
	bredis "github.com/benthosdev/benthos/v4/internal/impl/redis/old"
)

// RedisStreamsConfig contains configuration fields for the RedisStreams input
// type.
type RedisStreamsConfig struct {
	bredis.Config   `json:",inline" yaml:",inline"`
	BodyKey         string   `json:"body_key" yaml:"body_key"`
	Streams         []string `json:"streams" yaml:"streams"`
	CreateStreams   bool     `json:"create_streams" yaml:"create_streams"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	Limit           int64    `json:"limit" yaml:"limit"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
	CommitPeriod    string   `json:"commit_period" yaml:"commit_period"`
	Timeout         string   `json:"timeout" yaml:"timeout"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	return RedisStreamsConfig{
		Config:          bredis.NewConfig(),
		BodyKey:         "body",
		Streams:         []string{},
		CreateStreams:   true,
		ConsumerGroup:   "",
		ClientID:        "",
		Limit:           10,
		StartFromOldest: true,
		CommitPeriod:    "1s",
		Timeout:         "1s",
	}
}
