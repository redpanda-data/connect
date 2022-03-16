package input

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
)

// BrokerConfig contains configuration fields for the Broker input type.
type BrokerConfig struct {
	Copies   int           `json:"copies" yaml:"copies"`
	Inputs   []Config      `json:"inputs" yaml:"inputs"`
	Batching policy.Config `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Copies:   1,
		Inputs:   []Config{},
		Batching: policy.NewConfig(),
	}
}
