package input

import (
	mqttconf "github.com/benthosdev/benthos/v4/internal/impl/mqtt/shared"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

// MQTTConfig contains configuration fields for the MQTT input type.
type MQTTConfig struct {
	URLs                  []string      `json:"urls" yaml:"urls"`
	QoS                   uint8         `json:"qos" yaml:"qos"`
	Topics                []string      `json:"topics" yaml:"topics"`
	ClientID              string        `json:"client_id" yaml:"client_id"`
	DynamicClientIDSuffix string        `json:"dynamic_client_id_suffix" yaml:"dynamic_client_id_suffix"`
	Will                  mqttconf.Will `json:"will" yaml:"will"`
	CleanSession          bool          `json:"clean_session" yaml:"clean_session"`
	User                  string        `json:"user" yaml:"user"`
	Password              string        `json:"password" yaml:"password"`
	ConnectTimeout        string        `json:"connect_timeout" yaml:"connect_timeout"`
	KeepAlive             int64         `json:"keepalive" yaml:"keepalive"`
	TLS                   tls.Config    `json:"tls" yaml:"tls"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:           []string{},
		QoS:            1,
		Topics:         []string{},
		ClientID:       "",
		Will:           mqttconf.EmptyWill(),
		CleanSession:   true,
		User:           "",
		Password:       "",
		ConnectTimeout: "30s",
		KeepAlive:      30,
		TLS:            tls.NewConfig(),
	}
}
