package processor

// CompressConfig contains configuration fields for the Compress processor.
type CompressConfig struct {
	Algorithm string `json:"algorithm" yaml:"algorithm"`
	Level     int    `json:"level" yaml:"level"`
}

// NewCompressConfig returns a CompressConfig with default values.
func NewCompressConfig() CompressConfig {
	return CompressConfig{
		Algorithm: "",
		Level:     -1,
	}
}
