package output

// SubprocessConfig contains configuration for the Subprocess input type.
type SubprocessConfig struct {
	Name  string   `json:"name" yaml:"name"`
	Args  []string `json:"args" yaml:"args"`
	Codec string   `json:"codec" yaml:"codec"`
}

// NewSubprocessConfig creates a new SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Name:  "",
		Args:  []string{},
		Codec: "lines",
	}
}
