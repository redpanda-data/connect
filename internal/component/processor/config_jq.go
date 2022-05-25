package processor

// JQConfig contains configuration fields for the JQ processor.
type JQConfig struct {
	Query     string `json:"query" yaml:"query"`
	Raw       bool   `json:"raw" yaml:"raw"`
	OutputRaw bool   `json:"output_raw" yaml:"output_raw"`
}

// NewJQConfig returns a JQConfig with default values.
func NewJQConfig() JQConfig {
	return JQConfig{
		Query: "",
	}
}
