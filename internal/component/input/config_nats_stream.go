package input

import (
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NATSStreamConfig contains configuration fields for the NATSStream input type.
type NATSStreamConfig struct {
	URLs            []string    `json:"urls" yaml:"urls"`
	ClusterID       string      `json:"cluster_id" yaml:"cluster_id"`
	ClientID        string      `json:"client_id" yaml:"client_id"`
	QueueID         string      `json:"queue" yaml:"queue"`
	DurableName     string      `json:"durable_name" yaml:"durable_name"`
	UnsubOnClose    bool        `json:"unsubscribe_on_close" yaml:"unsubscribe_on_close"`
	StartFromOldest bool        `json:"start_from_oldest" yaml:"start_from_oldest"`
	Subject         string      `json:"subject" yaml:"subject"`
	MaxInflight     int         `json:"max_inflight" yaml:"max_inflight"`
	AckWait         string      `json:"ack_wait" yaml:"ack_wait"`
	TLS             btls.Config `json:"tls" yaml:"tls"`
	Auth            auth.Config `json:"auth" yaml:"auth"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:            []string{},
		ClusterID:       "",
		ClientID:        "",
		QueueID:         "",
		DurableName:     "",
		UnsubOnClose:    false,
		StartFromOldest: true,
		Subject:         "",
		MaxInflight:     1024,
		AckWait:         "30s",
		TLS:             btls.NewConfig(),
		Auth:            auth.New(),
	}
}
