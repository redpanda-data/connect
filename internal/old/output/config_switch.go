package output

// SwitchConfig contains configuration fields for the switchOutput output type.
type SwitchConfig struct {
	RetryUntilSuccess bool               `json:"retry_until_success" yaml:"retry_until_success"`
	StrictMode        bool               `json:"strict_mode" yaml:"strict_mode"`
	Cases             []SwitchConfigCase `json:"cases" yaml:"cases"`
}

// NewSwitchConfig creates a new SwitchConfig with default values.
func NewSwitchConfig() SwitchConfig {
	return SwitchConfig{
		RetryUntilSuccess: false,
		StrictMode:        false,
		Cases:             []SwitchConfigCase{},
	}
}

// SwitchConfigCase contains configuration fields per output of a switch type.
type SwitchConfigCase struct {
	Check    string `json:"check" yaml:"check"`
	Continue bool   `json:"continue" yaml:"continue"`
	Output   Config `json:"output" yaml:"output"`
}

// NewSwitchConfigCase creates a new switch output config with default values.
func NewSwitchConfigCase() SwitchConfigCase {
	return SwitchConfigCase{
		Check:    "",
		Continue: false,
		Output:   NewConfig(),
	}
}
