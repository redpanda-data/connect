package input

import (
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// NSQConfig contains configuration fields for the NSQ input type.
type NSQConfig struct {
	Addresses       []string    `json:"nsqd_tcp_addresses" yaml:"nsqd_tcp_addresses"`
	LookupAddresses []string    `json:"lookupd_http_addresses" yaml:"lookupd_http_addresses"`
	Topic           string      `json:"topic" yaml:"topic"`
	Channel         string      `json:"channel" yaml:"channel"`
	UserAgent       string      `json:"user_agent" yaml:"user_agent"`
	TLS             btls.Config `json:"tls" yaml:"tls"`
	MaxInFlight     int         `json:"max_in_flight" yaml:"max_in_flight"`
	MaxAttempts     uint16      `json:"max_attempts" yaml:"max_attempts"`
}

// NewNSQConfig creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	return NSQConfig{
		Addresses:       []string{},
		LookupAddresses: []string{},
		Topic:           "",
		Channel:         "",
		UserAgent:       "",
		TLS:             btls.NewConfig(),
		MaxInFlight:     100,
		MaxAttempts:     5,
	}
}
