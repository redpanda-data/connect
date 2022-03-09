package input

import (
	"github.com/benthosdev/benthos/v4/internal/impl/pulsar/auth"
)

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL              string      `json:"url" yaml:"url"`
	Topics           []string    `json:"topics" yaml:"topics"`
	SubscriptionName string      `json:"subscription_name" yaml:"subscription_name"`
	SubscriptionType string      `json:"subscription_type" yaml:"subscription_type"`
	Auth             auth.Config `json:"auth" yaml:"auth"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:              "",
		Topics:           []string{},
		SubscriptionName: "",
		SubscriptionType: "",
		Auth:             auth.New(),
	}
}
