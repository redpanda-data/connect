package output

import (
	"github.com/benthosdev/benthos/v4/internal/metadata"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// AMQPExchangeDeclareConfig contains fields indicating whether the target AMQP
// exchange needs to be declared, as well as any fields specifying how to
// accomplish that.
type AMQPExchangeDeclareConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Type    string `json:"type" yaml:"type"`
	Durable bool   `json:"durable" yaml:"durable"`
}

// AMQPConfig contains configuration fields for the AMQP output type.
type AMQPConfig struct {
	URLs            []string                     `json:"urls" yaml:"urls"`
	MaxInFlight     int                          `json:"max_in_flight" yaml:"max_in_flight"`
	Exchange        string                       `json:"exchange" yaml:"exchange"`
	ExchangeDeclare AMQPExchangeDeclareConfig    `json:"exchange_declare" yaml:"exchange_declare"`
	BindingKey      string                       `json:"key" yaml:"key"`
	Type            string                       `json:"type" yaml:"type"`
	ContentType     string                       `json:"content_type" yaml:"content_type"`
	ContentEncoding string                       `json:"content_encoding" yaml:"content_encoding"`
	Metadata        metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	Priority        string                       `json:"priority" yaml:"priority"`
	Persistent      bool                         `json:"persistent" yaml:"persistent"`
	Mandatory       bool                         `json:"mandatory" yaml:"mandatory"`
	Immediate       bool                         `json:"immediate" yaml:"immediate"`
	TLS             btls.Config                  `json:"tls" yaml:"tls"`
	Timeout         string                       `json:"timeout" yaml:"timeout"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URLs:        []string{},
		MaxInFlight: 64,
		Exchange:    "",
		ExchangeDeclare: AMQPExchangeDeclareConfig{
			Enabled: false,
			Type:    "direct",
			Durable: true,
		},
		BindingKey:      "",
		Type:            "",
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		Metadata:        metadata.NewExcludeFilterConfig(),
		Priority:        "",
		Persistent:      false,
		Mandatory:       false,
		Immediate:       false,
		TLS:             btls.NewConfig(),
		Timeout:         "",
	}
}
