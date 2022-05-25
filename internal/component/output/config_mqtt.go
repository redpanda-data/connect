package output

import (
	mqttconf "github.com/benthosdev/benthos/v4/internal/impl/mqtt/shared"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

// MQTTConfig contains configuration fields for the MQTT output type.
type MQTTConfig struct {
	URLs                  []string      `json:"urls" yaml:"urls"`
	QoS                   uint8         `json:"qos" yaml:"qos"`
	Retained              bool          `json:"retained" yaml:"retained"`
	RetainedInterpolated  string        `json:"retained_interpolated" yaml:"retained_interpolated"`
	Topic                 string        `json:"topic" yaml:"topic"`
	ClientID              string        `json:"client_id" yaml:"client_id"`
	DynamicClientIDSuffix string        `json:"dynamic_client_id_suffix" yaml:"dynamic_client_id_suffix"`
	Will                  mqttconf.Will `json:"will" yaml:"will"`
	User                  string        `json:"user" yaml:"user"`
	Password              string        `json:"password" yaml:"password"`
	ConnectTimeout        string        `json:"connect_timeout" yaml:"connect_timeout"`
	WriteTimeout          string        `json:"write_timeout" yaml:"write_timeout"`
	KeepAlive             int64         `json:"keepalive" yaml:"keepalive"`
	MaxInFlight           int           `json:"max_in_flight" yaml:"max_in_flight"`
	TLS                   tls.Config    `json:"tls" yaml:"tls"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:           []string{},
		QoS:            1,
		Topic:          "",
		ClientID:       "",
		Will:           mqttconf.EmptyWill(),
		User:           "",
		Password:       "",
		ConnectTimeout: "30s",
		WriteTimeout:   "3s",
		MaxInFlight:    64,
		KeepAlive:      30,
		TLS:            tls.NewConfig(),
	}
}
