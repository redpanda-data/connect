package output

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSStreamConfig contains configuration fields for the NATSStream output
// type.
type NATSStreamConfig struct {
	URLs        []string    `json:"urls" yaml:"urls"`
	ClusterID   string      `json:"cluster_id" yaml:"cluster_id"`
	ClientID    string      `json:"client_id" yaml:"client_id"`
	Subject     string      `json:"subject" yaml:"subject"`
	MaxInFlight int         `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
	Auth        auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:        []string{},
		ClusterID:   "",
		ClientID:    "",
		Subject:     "",
		MaxInFlight: 64,
		TLS:         btls.NewConfig(),
		Auth:        auth.New(),
	}
}
