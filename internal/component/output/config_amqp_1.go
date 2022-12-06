package output

import (
	"github.com/benthosdev/benthos/v4/internal/impl/amqp1/shared"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// AMQP1Config contains configuration fields for the AMQP1 output type.
type AMQP1Config struct {
	URL                          string                       `json:"url" yaml:"url"`
	TargetAddress                string                       `json:"target_address" yaml:"target_address"`
	MaxInFlight                  int                          `json:"max_in_flight" yaml:"max_in_flight"`
	TLS                          btls.Config                  `json:"tls" yaml:"tls"`
	SASL                         shared.SASLConfig            `json:"sasl" yaml:"sasl"`
	Metadata                     metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	ApplicationPropertiesMapping string                       `json:"application_properties_map" yaml:"application_properties_map"`
}

// NewAMQP1Config creates a new AMQP1Config with default values.
func NewAMQP1Config() AMQP1Config {
	return AMQP1Config{
		URL:                          "",
		TargetAddress:                "",
		MaxInFlight:                  64,
		TLS:                          btls.NewConfig(),
		SASL:                         shared.NewSASLConfig(),
		Metadata:                     metadata.NewExcludeFilterConfig(),
		ApplicationPropertiesMapping: "",
	}
}
