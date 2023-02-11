package output

import (
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// GCPPubSubFlowControlConfig configures a flow control policy of the PubSub
// client. This affects the internal buffering mechanism that the client manages
// for each topic when publishing messages.
type GCPPubSubFlowControlConfig struct {
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	LimitExceededBehavior  string `json:"limit_exceeded_behavior" yaml:"limit_exceeded_behavior"`
}

// NewGCPPubSubFlowControlConfig creates a new flow control policy with default
// values.
func NewGCPPubSubFlowControlConfig() GCPPubSubFlowControlConfig {
	return GCPPubSubFlowControlConfig{
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  "ignore",
	}
}

// GCPPubSubConfig contains configuration fields for the output GCPPubSub type.
type GCPPubSubConfig struct {
	ProjectID      string                       `json:"project" yaml:"project"`
	TopicID        string                       `json:"topic" yaml:"topic"`
	MaxInFlight    int                          `json:"max_in_flight" yaml:"max_in_flight"`
	PublishTimeout string                       `json:"publish_timeout" yaml:"publish_timeout"`
	Metadata       metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	OrderingKey    string                       `json:"ordering_key" yaml:"ordering_key"`
	Endpoint       string                       `json:"endpoint" yaml:"endpoint"`
	FlowControl    GCPPubSubFlowControlConfig   `json:"flow_control" yaml:"flow_control"`
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
		FlowControl:    NewGCPPubSubFlowControlConfig(),
	}
}
