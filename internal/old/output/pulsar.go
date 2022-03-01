package output

import "github.com/Jeffail/benthos/v3/internal/impl/pulsar/auth"

// PulsarConfig contains configuration for the Pulsar input type.
type PulsarConfig struct {
	URL         string      `json:"url" yaml:"url"`
	Topic       string      `json:"topic" yaml:"topic"`
	MaxInFlight int         `json:"max_in_flight" yaml:"max_in_flight"`
	Key         string      `json:"key" yaml:"key"`
	OrderingKey string      `json:"ordering_key" yaml:"ordering_key"`
	Auth        auth.Config `json:"auth" yaml:"auth"`
}

// NewPulsarConfig creates a new PulsarConfig with default values.
func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:         "",
		Topic:       "",
		MaxInFlight: 1,
		Key:         "",
		OrderingKey: "",
		Auth:        auth.New(),
	}
}
