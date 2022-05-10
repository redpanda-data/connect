package output

import (
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NSQConfig contains configuration fields for the NSQ output type.
type NSQConfig struct {
	Address     string      `json:"nsqd_tcp_address" yaml:"nsqd_tcp_address"`
	Topic       string      `json:"topic" yaml:"topic"`
	UserAgent   string      `json:"user_agent" yaml:"user_agent"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
	MaxInFlight int         `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNSQConfig creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	return NSQConfig{
		Address:     "",
		Topic:       "",
		UserAgent:   "",
		TLS:         btls.NewConfig(),
		MaxInFlight: 64,
	}
}
