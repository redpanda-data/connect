package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
)

// BrokerConfig contains configuration fields for the Broker output type.
type BrokerConfig struct {
	Copies   int           `json:"copies" yaml:"copies"`
	Pattern  string        `json:"pattern" yaml:"pattern"`
	Outputs  []Config      `json:"outputs" yaml:"outputs"`
	Batching policy.Config `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Copies:   1,
		Pattern:  "fan_out",
		Outputs:  []Config{},
		Batching: policy.NewConfig(),
	}
}
