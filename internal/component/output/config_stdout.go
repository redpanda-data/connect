package output

// STDOUTConfig contains configuration fields for the stdout based output type.
type STDOUTConfig struct {
	Codec string `json:"codec" yaml:"codec"`
}

// NewSTDOUTConfig creates a new STDOUTConfig with default values.
func NewSTDOUTConfig() STDOUTConfig {
	return STDOUTConfig{
		Codec: "lines",
	}
}
