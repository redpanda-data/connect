package output

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSConfig contains configuration fields for the NATS output type.
type NATSConfig struct {
	URLs        []string          `json:"urls" yaml:"urls"`
	Subject     string            `json:"subject" yaml:"subject"`
	Headers     map[string]string `json:"headers" yaml:"headers"`
	MaxInFlight int               `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         btls.Config       `json:"tls" yaml:"tls"`
	Auth        auth.Config       `json:"auth" yaml:"auth"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:        []string{},
		Subject:     "",
		MaxInFlight: 64,
		TLS:         btls.NewConfig(),
		Auth:        auth.New(),
	}
}
