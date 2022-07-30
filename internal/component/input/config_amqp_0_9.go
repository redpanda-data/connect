package input

import (
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// AMQP09QueueDeclareConfig contains fields indicating whether the target AMQP09
// queue needs to be declared and bound to an exchange, as well as any fields
// specifying how to accomplish that.
type AMQP09QueueDeclareConfig struct {
	Enabled    bool `json:"enabled" yaml:"enabled"`
	Durable    bool `json:"durable" yaml:"durable"`
	AutoDelete bool `json:"auto_delete" yaml:"auto_delete"`
}

// AMQP09BindingConfig contains fields describing a queue binding to be
// declared.
type AMQP09BindingConfig struct {
	Exchange   string `json:"exchange" yaml:"exchange"`
	RoutingKey string `json:"key" yaml:"key"`
}

// AMQP09Config contains configuration for the AMQP09 input type.
type AMQP09Config struct {
	URLs               []string                 `json:"urls" yaml:"urls"`
	Queue              string                   `json:"queue" yaml:"queue"`
	QueueDeclare       AMQP09QueueDeclareConfig `json:"queue_declare" yaml:"queue_declare"`
	BindingsDeclare    []AMQP09BindingConfig    `json:"bindings_declare" yaml:"bindings_declare"`
	ConsumerTag        string                   `json:"consumer_tag" yaml:"consumer_tag"`
	AutoAck            bool                     `json:"auto_ack" yaml:"auto_ack"`
	NackRejectPatterns []string                 `json:"nack_reject_patterns" yaml:"nack_reject_patterns"`
	PrefetchCount      int                      `json:"prefetch_count" yaml:"prefetch_count"`
	PrefetchSize       int                      `json:"prefetch_size" yaml:"prefetch_size"`
	TLS                btls.Config              `json:"tls" yaml:"tls"`
}

// NewAMQP09Config creates a new AMQP09Config with default values.
func NewAMQP09Config() AMQP09Config {
	return AMQP09Config{
		URLs:  []string{},
		Queue: "",
		QueueDeclare: AMQP09QueueDeclareConfig{
			Enabled:    false,
			Durable:    true,
			AutoDelete: false,
		},
		ConsumerTag:        "",
		AutoAck:            false,
		NackRejectPatterns: []string{},
		PrefetchCount:      10,
		PrefetchSize:       0,
		TLS:                btls.NewConfig(),
		BindingsDeclare:    []AMQP09BindingConfig{},
	}
}
