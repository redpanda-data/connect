package input

import (
	"bufio"
)

// SubprocessConfig contains configuration for the Subprocess input type.
type SubprocessConfig struct {
	Name          string   `json:"name" yaml:"name"`
	Args          []string `json:"args" yaml:"args"`
	Codec         string   `json:"codec" yaml:"codec"`
	RestartOnExit bool     `json:"restart_on_exit" yaml:"restart_on_exit"`
	MaxBuffer     int      `json:"max_buffer" yaml:"max_buffer"`
}

// NewSubprocessConfig creates a new SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Name:          "",
		Args:          []string{},
		Codec:         "lines",
		RestartOnExit: false,
		MaxBuffer:     bufio.MaxScanTokenSize,
	}
}
