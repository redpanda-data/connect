package input

import (
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"
)

// NATSJetStreamConfig contains configuration fields for the NATS Jetstream
// input type.
type NATSJetStreamConfig struct {
	URLs          []string   `json:"urls" yaml:"urls"`
	Subject       string     `json:"subject" yaml:"subject"`
	Queue         string     `json:"queue" yaml:"queue"`
	DurableName   string     `json:"durable_name" yaml:"durable_name"`
	MaxAckPending int        `json:"max_ack_pending" yaml:"max_ack_pending"`
	TLS           tls.Config `json:"tls" yaml:"tls"`
}

// NewNATSJetStreamConfig creates a new NATSJetstreamConfig with default values.
func NewNATSJetStreamConfig() NATSJetStreamConfig {
	return NATSJetStreamConfig{
		URLs:          []string{nats.DefaultURL},
		Subject:       "",
		MaxAckPending: 1024,
		TLS:           tls.NewConfig(),
	}
}
