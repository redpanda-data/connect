package input

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSJetStreamConfig contains configuration fields for the NATS Jetstream
// input type.
type NATSJetStreamConfig struct {
	URLs          []string    `json:"urls" yaml:"urls"`
	Subject       string      `json:"subject" yaml:"subject"`
	Queue         string      `json:"queue" yaml:"queue"`
	Durable       string      `json:"durable" yaml:"durable"`
	Stream        string      `json:"stream" yaml:"stream"`
	Bind          bool        `json:"bind" yaml:"bind"`
	Deliver       string      `json:"deliver" yaml:"deliver"`
	AckWait       string      `json:"ack_wait" yaml:"ack_wait"`
	MaxAckPending int         `json:"max_ack_pending" yaml:"max_ack_pending"`
	TLS           tls.Config  `json:"tls" yaml:"tls"`
	Auth          auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSJetStreamConfig creates a new NATSJetstreamConfig with default values.
func NewNATSJetStreamConfig() NATSJetStreamConfig {
	return NATSJetStreamConfig{
		URLs:          []string{},
		Subject:       "",
		AckWait:       "30s",
		MaxAckPending: 1024,
		Deliver:       "all",
		TLS:           tls.NewConfig(),
		Auth:          auth.New(),
	}
}
