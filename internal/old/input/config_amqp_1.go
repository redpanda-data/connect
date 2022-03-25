package input

import (
	"github.com/benthosdev/benthos/v4/internal/impl/amqp1/shared"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// AMQP1Config contains configuration for the AMQP1 input type.
type AMQP1Config struct {
	URL            string            `json:"url" yaml:"url"`
	SourceAddress  string            `json:"source_address" yaml:"source_address"`
	AzureRenewLock bool              `json:"azure_renew_lock" yaml:"azure_renew_lock"`
	TLS            btls.Config       `json:"tls" yaml:"tls"`
	SASL           shared.SASLConfig `json:"sasl" yaml:"sasl"`
}

// NewAMQP1Config creates a new AMQP1Config with default values.
func NewAMQP1Config() AMQP1Config {
	return AMQP1Config{
		URL:           "",
		SourceAddress: "",
		TLS:           btls.NewConfig(),
		SASL:          shared.NewSASLConfig(),
	}
}
