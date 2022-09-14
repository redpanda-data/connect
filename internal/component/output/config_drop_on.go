package output

import (
	"encoding/json"
)

// DropOnConditions is a config struct representing the different circumstances
// under which messages should be dropped.
type DropOnConditions struct {
	Error        bool   `json:"error" yaml:"error"`
	BackPressure string `json:"back_pressure" yaml:"back_pressure"`
}

// DropOnConfig contains configuration values for the DropOn output type.
type DropOnConfig struct {
	DropOnConditions `json:",inline" yaml:",inline"`
	Output           *Config `json:"output" yaml:"output"`
}

// NewDropOnConfig creates a new DropOnConfig with default values.
func NewDropOnConfig() DropOnConfig {
	return DropOnConfig{
		DropOnConditions: DropOnConditions{
			Error:        false,
			BackPressure: "",
		},
		Output: nil,
	}
}

//------------------------------------------------------------------------------

type dummyDropOnConfig struct {
	DropOnConditions `json:",inline" yaml:",inline"`
	Output           any `json:"output" yaml:"output"`
}

// MarshalJSON prints an empty object instead of nil.
func (d DropOnConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyDropOnConfig{
		Output:           d.Output,
		DropOnConditions: d.DropOnConditions,
	}
	if d.Output == nil {
		dummy.Output = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (d DropOnConfig) MarshalYAML() (any, error) {
	dummy := dummyDropOnConfig{
		Output:           d.Output,
		DropOnConditions: d.DropOnConditions,
	}
	if d.Output == nil {
		dummy.Output = struct{}{}
	}
	return dummy, nil
}
