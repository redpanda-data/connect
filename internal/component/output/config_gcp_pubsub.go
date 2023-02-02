package output

import (
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// GCPPubSubConfig contains configuration fields for the output GCPPubSub type.
type GCPPubSubConfig struct {
	ProjectID      string                       `json:"project" yaml:"project"`
	TopicID        string                       `json:"topic" yaml:"topic"`
	MaxInFlight    int                          `json:"max_in_flight" yaml:"max_in_flight"`
	PublishTimeout string                       `json:"publish_timeout" yaml:"publish_timeout"`
	Metadata       metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	OrderingKey    string                       `json:"ordering_key" yaml:"ordering_key"`
	Endpoint       string                       `json:"endpoint" yaml:"endpoint"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:      "",
		TopicID:        "",
		MaxInFlight:    64,
		PublishTimeout: "60s",
		Metadata:       metadata.NewExcludeFilterConfig(),
		OrderingKey:    "",
		Endpoint:       "",
	}
}
