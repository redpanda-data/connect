package output

import (
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// SNSConfig contains configuration fields for the output SNS type.
type SNSConfig struct {
	TopicArn               string                       `json:"topic_arn" yaml:"topic_arn"`
	MessageGroupID         string                       `json:"message_group_id" yaml:"message_group_id"`
	MessageDeduplicationID string                       `json:"message_deduplication_id" yaml:"message_deduplication_id"`
	Metadata               metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	SessionConfig          `json:",inline" yaml:",inline"`
	Timeout                string `json:"timeout" yaml:"timeout"`
	MaxInFlight            int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewSNSConfig creates a new Config with default values.
func NewSNSConfig() SNSConfig {
	return SNSConfig{
		SessionConfig: SessionConfig{
			Config: sess.NewConfig(),
		},
		TopicArn:               "",
		MessageGroupID:         "",
		MessageDeduplicationID: "",
		Metadata:               metadata.NewExcludeFilterConfig(),
		Timeout:                "5s",
		MaxInFlight:            64,
	}
}
