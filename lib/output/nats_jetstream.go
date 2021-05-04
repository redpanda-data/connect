package output

import (
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"
)

// NATSJetStreamConfig contains configuration fields for the NATS Jetstream
// input type.
type NATSJetStreamConfig struct {
	URLs        []string   `json:"urls" yaml:"urls"`
	Subject     string     `json:"subject" yaml:"subject"`
	MaxInFlight int        `json:"max_in_flight" yaml:"max_in_flight"`
	TLS         tls.Config `json:"tls" yaml:"tls"`
}

// NewNATSJetStreamConfig creates a new NATSJetstreamConfig with default values.
func NewNATSJetStreamConfig() NATSJetStreamConfig {
	return NATSJetStreamConfig{
		URLs:        []string{nats.DefaultURL},
		Subject:     "",
		MaxInFlight: 1024,
		TLS:         tls.NewConfig(),
	}
}
