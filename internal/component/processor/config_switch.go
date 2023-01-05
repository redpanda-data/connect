package processor

import (
	"encoding/json"
)

// SwitchCaseConfig contains a condition, processors and other fields for an
// individual case in the Switch processor.
type SwitchCaseConfig struct {
	Check       string   `json:"check" yaml:"check"`
	Processors  []Config `json:"processors" yaml:"processors"`
	Fallthrough bool     `json:"fallthrough" yaml:"fallthrough"`
}

// NewSwitchCaseConfig returns a new SwitchCaseConfig with default values.
func NewSwitchCaseConfig() SwitchCaseConfig {
	return SwitchCaseConfig{
		Check:       "",
		Processors:  []Config{},
		Fallthrough: false,
	}
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (s *SwitchCaseConfig) UnmarshalYAML(unmarshal func(any) error) error {
	type confAlias SwitchCaseConfig
	aliased := confAlias(NewSwitchCaseConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*s = SwitchCaseConfig(aliased)
	return nil
}

// SwitchConfig is a config struct containing fields for the Switch processor.
type SwitchConfig []SwitchCaseConfig

// NewSwitchConfig returns a default SwitchConfig.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{}
}
