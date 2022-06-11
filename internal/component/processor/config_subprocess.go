package processor

import (
	"bufio"
)

// SubprocessConfig contains configuration fields for the Subprocess processor.
type SubprocessConfig struct {
	Name      string   `json:"name" yaml:"name"`
	Args      []string `json:"args" yaml:"args"`
	MaxBuffer int      `json:"max_buffer" yaml:"max_buffer"`
	CodecSend string   `json:"codec_send" yaml:"codec_send"`
	CodecRecv string   `json:"codec_recv" yaml:"codec_recv"`
}

// NewSubprocessConfig returns a SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Name:      "",
		Args:      []string{},
		MaxBuffer: bufio.MaxScanTokenSize,
		CodecSend: "lines",
		CodecRecv: "lines",
	}
}
