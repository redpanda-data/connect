package output

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSJetStreamConfig contains configuration fields for the NATS Jetstream
// input type.
type NATSJetStreamConfig struct {
	URLs        []string    `json:"urls" yaml:"urls"`
	Subject     string      `json:"subject" yaml:"subject"`
	MaxInFlight int         `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         tls.Config  `json:"tls" yaml:"tls"`
	Auth        auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSJetStreamConfig creates a new NATSJetstreamConfig with default values.
func NewNATSJetStreamConfig() NATSJetStreamConfig {
	return NATSJetStreamConfig{
		URLs:        []string{},
		Subject:     "",
		MaxInFlight: 1024,
		TLS:         tls.NewConfig(),
		Auth:        auth.New(),
	}
}
