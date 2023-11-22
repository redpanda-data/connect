package input

import (
	"encoding/json"
)

// ReadUntilConfig contains configuration values for the ReadUntil input type.
type ReadUntilConfig struct {
	Input       *Config `json:"input" yaml:"input"`
	Restart     bool    `json:"restart_input" yaml:"restart_input"`
	Check       string  `json:"check" yaml:"check"`
	IdleTimeout string  `json:"idle_timeout" yaml:"idle_timeout"`
}

// NewReadUntilConfig creates a new ReadUntilConfig with default values.
func NewReadUntilConfig() ReadUntilConfig {
	return ReadUntilConfig{
		Input:       nil,
		Restart:     false,
		Check:       "",
		IdleTimeout: "",
	}
}

type dummyReadUntilConfig struct {
	Input       any    `json:"input" yaml:"input"`
	Restart     bool   `json:"restart_input" yaml:"restart_input"`
	Check       string `json:"check" yaml:"check"`
	IdleTimeout string `json:"idle_timeout" yaml:"idle_timeout"`
}

// MarshalJSON prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyReadUntilConfig{
		Input:       r.Input,
		Restart:     r.Restart,
		Check:       r.Check,
		IdleTimeout: r.IdleTimeout,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (r ReadUntilConfig) MarshalYAML() (any, error) {
	dummy := dummyReadUntilConfig{
		Input:       r.Input,
		Restart:     r.Restart,
		Check:       r.Check,
		IdleTimeout: r.IdleTimeout,
	}
	if r.Input == nil {
		dummy.Input = struct{}{}
	}
	return dummy, nil
}
