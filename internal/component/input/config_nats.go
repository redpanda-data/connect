package input

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSConfig contains configuration fields for the NATS input type.
type NATSConfig struct {
	URLs          []string    `json:"urls" yaml:"urls"`
	Subject       string      `json:"subject" yaml:"subject"`
	QueueID       string      `json:"queue" yaml:"queue"`
	PrefetchCount int         `json:"prefetch_count" yaml:"prefetch_count"`
	TLS           btls.Config `json:"tls" yaml:"tls"`
	Auth          auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:          []string{},
		Subject:       "",
		QueueID:       "",
		PrefetchCount: 32,
		TLS:           btls.NewConfig(),
		Auth:          auth.New(),
	}
}
