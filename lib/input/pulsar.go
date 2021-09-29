package input

import (
	"github.com/Jeffail/benthos/v3/internal/impl/pulsar/auth"
)

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL              string      `json:"url" yaml:"url"`
	Topics           []string    `json:"topics" yaml:"topics"`
	SubscriptionName string      `json:"subscription_name" yaml:"subscription_name"`
	Auth             auth.Config `json:"auth" yaml:"auth"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:              "",
		Topics:           []string{},
		SubscriptionName: "",
		Auth:             auth.New(),
	}
}
